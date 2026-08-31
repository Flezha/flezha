defmodule Flezha.AuthZ.QuotaPointsTest do
  @moduledoc """
  finish-platform-authn-authz §6: the runner-connection quota point, the
  disk-footprint accounting (boot rebuild from the on-disk truth), and the
  breach alarm that never interrupts durable writes.
  """
  # async: false: writes Application env; a shared on-disk root; the live controller and its fleet.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Flezha.AuthZ
  alias Flezha.AuthZ.{NamespaceStore, QuotaCounters, Quotas}
  alias Flezha.Runner.FleetManager

  setup do
    %{tmp: tmp} =
      FlezhaTest.Helper.AuthTestHelper.setup_auth_stores(prefix: "flz_quota_points", claim: true)

    assert AuthZ.enabled?()

    state_root = Path.join(tmp, "state")
    File.mkdir_p!(state_root)
    Application.put_env(:flezha, :state_root, state_root)
    on_exit(fn -> Application.delete_env(:flezha, :state_root) end)

    {:ok, state_root: state_root}
  end

  describe "max_runners (§6.1)" do
    test "a runner connect over quota is refused before any session is minted" do
      {:ok, _} = NamespaceStore.create("team-tight", quotas: %{max_runners: 1})
      :ok = QuotaCounters.set("team-tight", :runners, 1)

      assert {:error, {:quota_exceeded, :max_runners, %{current: 1, limit: 1}}} =
               FleetManager.connect_handler(make_ref(), "team-tight", %{}, :uds)
    end

    test "under the limit the quota check passes" do
      {:ok, _} = NamespaceStore.create("team-roomy", quotas: %{max_runners: 4})
      :ok = QuotaCounters.set("team-roomy", :runners, 3)

      assert :ok = Quotas.check_allocation("team-roomy", :max_runners)
    end
  end

  describe "max_state_bytes accounting (§6.2)" do
    test "the counter is rebuilt from the on-disk truth", %{state_root: state_root} do
      {:ok, _} = NamespaceStore.create("team-disk", quotas: %{})
      dir = Path.join([state_root, "team-disk", "op", "proc"])
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

    test "a breached namespace refuses new allocation without touching writes", %{
      state_root: state_root
    } do
      {:ok, _} = NamespaceStore.create("team-over", quotas: %{max_state_bytes: 1_000})
      dir = Path.join([state_root, "team-over", "op", "proc"])
      File.mkdir_p!(dir)
      File.write!(Path.join(dir, "k1.wal"), :binary.copy(<<0>>, 4_096))
      _ = Quotas.rebuild_disk_bytes("team-over")

      # New allocation refused (the start_operative gate asks with 0 more
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
      dir = Path.join([state_root, "team-alarm", "op", "proc"])
      File.mkdir_p!(dir)
      File.write!(Path.join(dir, "k1.wal"), :binary.copy(<<0>>, 4_096))

      handler_id = {__MODULE__, make_ref()}
      test_pid = self()

      :ok =
        :telemetry.attach(
          handler_id,
          [:flezha, :state, :disk_quota_breach],
          fn _event, measurements, metadata, _cfg ->
            send(test_pid, {:breach, measurements, metadata})
          end,
          nil
        )

      try do
        # The alarm is deliberately loud: assert the operator-facing line is the
        # contract rather than letting it reach the console unasserted.
        log =
          capture_log(fn ->
            :ok = Flezha.State.Watchdog.sweep_now()

            assert_receive {:breach, %{bytes: bytes, limit: 100}, %{namespace: "team-alarm"}},
                           2_000

            assert bytes > 100
          end)

        assert log =~ "namespace team-alarm exceeds max_state_bytes"

        # Latched: a second sweep with the breach still standing does not
        # re-alarm.
        :ok = Flezha.State.Watchdog.sweep_now()
        refute_received {:breach, _, %{namespace: "team-alarm"}}
      after
        :telemetry.detach(handler_id)
      end
    end
  end

  describe "per-namespace mailbox watermark (§6.3)" do
    test "the credit window is capped by max_mailbox_bytes_total" do
      {:ok, _} = NamespaceStore.create("team-mail", quotas: %{max_mailbox_bytes_total: 4_096})
      :ok = Flezha.State.Paths.ensure_claims_table()
      :ok = Flezha.State.Paths.claim_namespace("mail-op", "team-mail")

      assert Flezha.State.PersistenceResolver.credit_window_bytes("mail-op") == 4_096

      # A class in a namespace with the default (large) quota keeps the
      # configured window.
      {:ok, _} = NamespaceStore.create("team-wide", quotas: %{})
      :ok = Flezha.State.Paths.claim_namespace("wide-op", "team-wide")

      assert Flezha.State.PersistenceResolver.credit_window_bytes("wide-op") ==
               Flezha.State.PersistenceResolver.wal_high_watermark_bytes()
    end
  end
end
