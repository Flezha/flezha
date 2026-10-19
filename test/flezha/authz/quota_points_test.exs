defmodule Flezha.AuthZ.QuotaPointsTest do
  @moduledoc """
  finish-platform-authn-authz §6: the runner-connection quota point, the
  disk-footprint accounting (boot rebuild from the on-disk truth), and the
  breach alarm that never interrupts durable writes.
  """
  # async: false: writes Application env; a shared on-disk root; the live controller and its fleet.
  use FlezhaTest.Case, fixtures: [:env], async: false

  use Mimic

  import ExUnit.CaptureLog
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.AuthZ
  alias Flezha.AuthZ.{NamespaceStore, QuotaCounters, Quotas, TokenStore}
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.HubManager
  alias Flezha.Service.RelayManager.Admission
  alias Flezha.State.Paths
  alias Flezha.State.PersistenceResolver
  alias Flezha.State.Watchdog
  alias FlezhaTest.Helper.AuthTestHelper
  alias FlezhaTest.Helper.Telemetry

  setup :set_mimic_global_until_exit

  setup do
    %{tmp: tmp} =
      AuthTestHelper.setup_auth_stores(prefix: "flz_quota_points", claim: true)

    assert AuthZ.enabled?()

    state_root = Path.join(tmp, "state")
    File.mkdir_p!(state_root)
    Application.put_env(:flezha, :state_root, state_root)

    {:ok, state_root: state_root}
  end

  describe "max_runners (§6.1)" do
    test "a runner connect over quota is refused before any session is minted" do
      {:ok, _} = NamespaceStore.create("team-tight", quotas: %{max_runners: 1})
      :ok = QuotaCounters.set("team-tight", :runners, 1)

      # The quota point is reached only by a connect whose namespace is proven,
      # so the runner credential is part of the set-up now.
      {:ok, secret, _} =
        TokenStore.issue("runner@team-tight", "team-tight", :runner, :never)

      assert {:error,
              %Flezha.Error{
                code: "quota.exceeded",
                details: %{"kind" => "max_runners", "current" => 1, "limit" => 1}
              }} =
               FleetManager.connect_handler(
                 make_ref(),
                 "team-tight",
                 %{"namespace_secret" => secret},
                 :uds
               )
    end

    test "under the limit the quota check passes" do
      {:ok, _} = NamespaceStore.create("team-roomy", quotas: %{max_runners: 4})
      :ok = QuotaCounters.set("team-roomy", :runners, 3)

      assert :ok = Quotas.check_allocation("team-roomy", :max_runners)
    end

    test "a runner of a namespace never created is refused namespace.not_found, and admitted once it exists" do
      {:ok, secret, _} = TokenStore.issue("runner@team-later", "team-later", :runner, :never)

      assert {:error,
              %Flezha.Error{
                code: "namespace.not_found",
                message: "namespace 'team-later' does not exist",
                details: %{"namespace" => "team-later"}
              }} =
               FleetManager.connect_handler(
                 make_ref(),
                 "team-later",
                 %{"namespace_secret" => secret},
                 :uds
               )

      {:ok, _} = NamespaceStore.create("team-later", quotas: %{})

      assert :ok =
               Flezha.Runner.Admission.Quota.check(
                 %{namespace: "team-later", transport: :uds},
                 %{}
               )
    end
  end

  describe "max_relays" do
    # fails when: a namespace whose relays all completed stays locked out with
    # quota_exceeded max_relays, because each stop released its slot on the node
    # hosting the hub child rather than the node that counted the start.
    test "releases that landed on another node's counter do not lock the namespace out" do
      {:ok, _} = NamespaceStore.create("team-relays", quotas: %{max_relays: 100})
      :ok = QuotaCounters.incr("team-relays", :relays, 101)
      _ = :sys.get_state(QuotaCounters)

      assert :ok = Admission.check_relay_quota("team-relays")
    end

    test "live relays at the limit still refuse a start" do
      {:ok, _} = NamespaceStore.create("team-full", quotas: %{max_relays: 2})
      stub(HubManager, :get_all_relays, fn "team-full" -> [{"a", self()}, {"b", self()}] end)

      assert {:error, {:quota_exceeded, :max_relays, %{current: 2, limit: 2}}} =
               Admission.check_relay_quota("team-full")
    end
  end

  describe "max_state_bytes accounting (§6.2)" do
    test "the counter is rebuilt from the on-disk truth", %{state_root: state_root} do
      {:ok, _} = NamespaceStore.create("team-disk", quotas: %{})
      dir = Path.join([state_root, "team-disk", "op", "task"])
      File.mkdir_p!(dir)
      File.write!(Path.join(dir, "k1.wal"), :binary.copy(<<0>>, 2_048))

      assert Quotas.rebuild_disk_bytes("team-disk") == 2_048
      assert QuotaCounters.get("team-disk", :disk_bytes) == 2_048

      # Drift within the cache window is served from the counter; a rebuild
      # reads the disk again.
      File.write!(Path.join(dir, "k2.wal"), :binary.copy(<<0>>, 1_024))
      assert Quotas.refresh_disk_bytes("team-disk") == 2_048
      assert Quotas.rebuild_disk_bytes("team-disk") == 3_072
    end

    test "a burst of callers rebuilds the counter once between them", %{state_root: state_root} do
      {:ok, _} = NamespaceStore.create("team-burst", quotas: %{})
      dir = Path.join([state_root, "team-burst", "op", "task"])
      File.mkdir_p!(dir)
      File.write!(Path.join(dir, "k1.wal"), :binary.copy(<<0>>, 2_048))

      # One claim is granted and the other 199 are told the window is held, so
      # 200 concurrent starts walk the tree once rather than 200 times and none
      # of them waits behind 199 fsyncing writes to be refused on a timeout.
      claims =
        1..200
        |> Task.async_stream(
          fn _ -> QuotaCounters.claim_refresh("team-burst", :disk_bytes, 30) end,
          max_concurrency: 200,
          ordered: false
        )
        |> Enum.map(fn {:ok, claim} -> claim end)

      assert Enum.count(claims, &(&1 == :claimed)) == 1
      assert Enum.count(claims, &(&1 == :fresh)) == 199

      # The window holds the burst off, never the truth: the bypass still reads
      # the disk.
      assert Quotas.rebuild_disk_bytes("team-burst") == 2_048
    end

    test "a lapsed window is claimed again", %{state_root: state_root} do
      {:ok, _} = NamespaceStore.create("team-lapse", quotas: %{})
      File.mkdir_p!(Path.join([state_root, "team-lapse"]))

      # A zero-second window has always lapsed, which is the boot rebuild's
      # reading: nothing it finds stamped can be newer than the uptime it lost.
      assert QuotaCounters.claim_refresh("team-lapse", :disk_bytes, 0) == :claimed
      assert QuotaCounters.claim_refresh("team-lapse", :disk_bytes, 0) == :claimed

      assert QuotaCounters.claim_refresh("team-lapse", :disk_bytes, 30) == :fresh
    end

    test "a breached namespace refuses new allocation without touching writes", %{
      state_root: state_root
    } do
      {:ok, _} = NamespaceStore.create("team-over", quotas: %{max_state_bytes: 1_000})
      dir = Path.join([state_root, "team-over", "op", "task"])
      File.mkdir_p!(dir)
      File.write!(Path.join(dir, "k1.wal"), :binary.copy(<<0>>, 4_096))
      _ = Quotas.rebuild_disk_bytes("team-over")

      # New allocation refused (the start_relay gate asks with 0 more
      # bytes: over means over)...
      assert {:error, {:quota_exceeded, :max_state_bytes, _}} =
               Quotas.check_allocation("team-over", :max_state_bytes, 0)

      # ...while the existing durable file is untouched.
      assert File.stat!(Path.join(dir, "k1.wal")).size == 4_096
    end

    test "the Watchdog sweep raises a loud, latched alarm on breach", %{
      state_root: state_root
    } do
      {:ok, _} = NamespaceStore.create("team-alarm", quotas: %{max_state_bytes: 100})
      dir = Path.join([state_root, "team-alarm", "op", "task"])
      File.mkdir_p!(dir)
      File.write!(Path.join(dir, "k1.wal"), :binary.copy(<<0>>, 4_096))

      Telemetry.capture([:flezha, :state, :disk_quota_breach],
        as: :breach
      )

      # The alarm is deliberately loud: assert the caller-facing line is the
      # contract rather than letting it reach the console unasserted.
      log =
        capture_log(fn ->
          :ok = Watchdog.sweep_now()

          assert_receive {:breach, %{bytes: bytes, limit: 100}, %{namespace: "team-alarm"}},
                         2_000

          assert bytes > 100
        end)

      assert log =~ "namespace team-alarm exceeds max_state_bytes"

      # Latched: a second sweep with the breach still standing does not
      # re-alarm.
      :ok = Watchdog.sweep_now()
      refute_received {:breach, _, %{namespace: "team-alarm"}}
    end

    test "a quota store that exits mid-sweep leaves the Watchdog running", %{
      state_root: state_root
    } do
      File.mkdir_p!(Path.join([state_root, "team-blip", "op", "task"]))
      watchdog = Process.whereis(Watchdog)
      stub(Quotas, :refresh_disk_bytes, fn _namespace -> exit(:shutdown) end)

      :ok = Watchdog.sweep_now()
      assert Process.whereis(Watchdog) == watchdog
    end
  end

  describe "per-namespace mailbox watermark (§6.3)" do
    test "the credit window is capped by max_mailbox_bytes_total" do
      {:ok, _} = NamespaceStore.create("team-mail", quotas: %{max_mailbox_bytes_total: 4_096})
      :ok = Paths.ensure_claims_table()
      :ok = Paths.claim_namespace("mail-op", "team-mail")

      assert PersistenceResolver.credit_window_bytes("mail-op") == 4_096

      # A class in a namespace with the default (large) quota keeps the
      # configured window.
      {:ok, _} = NamespaceStore.create("team-wide", quotas: %{})
      :ok = Paths.claim_namespace("wide-op", "team-wide")

      assert PersistenceResolver.credit_window_bytes("wide-op") ==
               PersistenceResolver.wal_high_watermark_bytes()
    end
  end
end
