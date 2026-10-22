defmodule Flezha.Service.Cli.RewindTest do
  @moduledoc """
  `flz relay rewind <instance> --to-iteration N` operations surface: how a
  rewind that went through, and each verdict the Overwatch guard can return, is
  reported. The guard's own logic is covered in `overwatch_suspension_test`.
  """
  # async: false: Mimic global mode; Mimic stubs.
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO
  import Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Service.Cli
  alias FlezhaTest.Helper.CliCapture

  setup :set_mimic_global_until_exit

  defp instance_id(), do: "rw_#{System.unique_integer([:positive])}"

  # `relay_rewind` resolves a pid and calls `{:rewind, n}` on it, so a process
  # that answers with a fixed verdict exercises the whole CLI surface.
  defp stub_overwatch(inst, verdict) do
    pid = spawn_link(fn -> reply_loop(verdict) end)
    stub(Flezha.Service.RelayManager, :get_relay_pid, fn _ns, ^inst -> {:ok, pid} end)
  end

  defp reply_loop(verdict) do
    receive do
      {:"$gen_call", from, {:rewind, _to_iter}} ->
        GenServer.reply(from, verdict)
        reply_loop(verdict)
    end
  end

  # A rewind that refuses exits non-zero and renders on stderr; one that
  # succeeds returns normally and reports on stdout. Both streams are captured
  # so a test can assert on whichever this rewind produced.
  defp rewind(inst, to_iter) do
    {_outcome, out, err} =
      CliCapture.run(fn ->
        Cli.call("Steps.rewind", Jason.encode!(["default", inst, to_iter]), [])
      end)

    out <> err
  end

  test "a rewind that goes through reports where the loop resumes" do
    inst = instance_id()
    stub_overwatch(inst, :ok)

    out = rewind(inst, 3)

    assert out =~ "rewound to iteration 3"
    assert out =~ "resumes at 4"
  end

  test "an uncommitted target is refused, naming the committed range" do
    inst = instance_id()
    stub_overwatch(inst, {:error, {:uncommitted_iteration, 7, [0, 1, 2]}})

    out = rewind(inst, 7)

    assert out =~ "iteration 7 has not committed"
    assert out =~ "(relay.iteration_not_committed)"
  end

  test "a loop with nothing committed says so rather than printing an empty range" do
    inst = instance_id()
    stub_overwatch(inst, {:error, {:uncommitted_iteration, 0, []}})

    assert rewind(inst, 0) =~ "iteration 0 has not committed"
  end

  test "an uncompensable emit is refused, naming the channels" do
    inst = instance_id()
    stub_overwatch(inst, {:error, {:uncovered, 1, ["replies", "audit"]}})

    out = rewind(inst, 1)

    assert out =~ "would unwind output on replies, audit"
    assert out =~ "no compensate hook, so it is refused (relay.rewind_uncovered)"
  end

  test "an unknown relay is reported, not crashed on" do
    inst = instance_id()
    stub(Flezha.Service.RelayManager, :get_relay_pid, fn _ns, ^inst -> {:error, :nf} end)

    assert rewind(inst, 1) =~ "(relay.not_running)"
  end
end

defmodule Flezha.Service.Cli.DlqTest do
  @moduledoc """
  `flz dlq list | show | replay | drop` controller-side behaviour: replay
  re-enters the ordinary `:io_in` input path, a clean replay removes the
  entry, a failing replay re-parks it with `generation` incremented, and
  a drop is audited.
  """
  # async: false: writes Application env; a shared on-disk root; global telemetry handlers.
  use FlezhaTest.Case, fixtures: [{:isolated_dlq_dir, replay_grace_ms: 400}], async: false

  import ExUnit.CaptureIO

  alias Flezha.Dlq.Store
  alias Flezha.Observability.RingBufferSink
  alias Flezha.Observability.TestSinks
  alias Flezha.Relay.ClassRegistry
  alias Flezha.Service.Cli
  alias FlezhaTest.Helper.DlqFixtures

  setup do
    ClassRegistry.ensure_table()

    rc = "dlqcli_" <> Integer.to_string(System.unique_integer([:positive]))

    entry = %{
      relay_class: rc,
      tasklet_id: "task",
      cell_key: "k",
      channel: "input",
      origin_offset: nil,
      error: "kaboom",
      attempts: 3,
      payload: "poison-bytes"
    }

    %{rc: rc, entry: entry}
  end

  test "list and show render the parked entry", %{entry: entry} do
    {:ok, ref} = Store.append(entry)

    listing =
      capture_io(fn -> Cli.call("Dlq.list", Jason.encode!([entry.relay_class]), []) end)

    assert listing =~ ref
    assert listing =~ "input"
    assert listing =~ "kaboom"

    shown = capture_io(fn -> Cli.call("Dlq.get", Jason.encode!([ref]), []) end)
    assert shown =~ ~s("kaboom")
    assert shown =~ "poison-bytes"
    assert shown =~ ~s("parked")
  end

  test "replay re-enters the normal input path; success removes the entry", %{
    rc: rc,
    entry: entry
  } do
    {:ok, ref} = Store.append(entry)
    ClassRegistry.put_overwatch(rc, self(), nil)

    output = capture_io(fn -> Cli.call("Dlq.replay", Jason.encode!([ref]), []) end)

    assert_received {:"$gen_cast", {:io_in, "poison-bytes", %{"channel" => "input"}}}
    assert output =~ "replay succeeded"
    assert {:error, :not_found} = Store.get(ref)
  end

  test "failing replay re-parks with generation 2", %{rc: rc, entry: entry} do
    {:ok, ref} = Store.append(entry)

    overwatch = DlqFixtures.spawn_reparking_overwatch(entry, self())

    ClassRegistry.put_overwatch(rc, overwatch, nil)

    output = capture_io(fn -> Cli.call("Dlq.replay", Jason.encode!([ref]), []) end)

    assert_received :re_parked
    assert output =~ "re-parked with generation 2"
    assert {:ok, still_there, _payload} = Store.get(ref)
    assert still_there["generation"] == 2
    assert still_there["status"] == "parked"
  end

  test "drop removes the entry and audits it on the dlq service source", %{entry: entry} do
    {:ok, ref} = Store.append(entry)
    boundary = System.monotonic_time(:microsecond)

    output = capture_io(fn -> Cli.call("Dlq.drop", Jason.encode!([ref]), []) end)

    assert output =~ "dropped #{ref}"
    TestSinks.settle(RingBufferSink)

    assert Enum.any?(RingBufferSink.query(:dlq_service, boundary), fn signal ->
             signal.name == :drop and signal.metadata[:entry] == ref and
               signal.metadata[:caller_source] == :cli
           end)

    assert {:error, :not_found} = Store.get(ref)
  end
end

defmodule Flezha.Service.Cli.ClusterTest do
  @moduledoc """
  CLI surface for `flz relay list` and `flz cluster status`. The data
  sources (`HubManager` registry access) are stubbed so the rendering
  and shaping are asserted deterministically.

  After `convergence-self-healing` §4 deleted the
  `Flezha.Reconciler.Leader` GenServer, the cluster_status leader
  column degrades to the local node — the cluster-wide registry-
  mutating actions that needed leader election are gone (process_hub's
  coordinator handles election where it is still required).
  """
  # async: false: Mimic stubs.
  use ExUnit.Case, async: false
  use Mimic

  import ExUnit.CaptureIO

  alias Flezha.Convergence.Snapshot
  alias Flezha.Service.Cli
  alias Flezha.Service.HubManager
  alias FlezhaTest.Helper.ConvergenceFixtures

  describe "list/2 (health column)" do
    setup do
      prev = Snapshot.current()
      on_exit(fn -> Snapshot.publish(prev) end)
    end

    defp partition_group(health) do
      for index <- 0..2 do
        %{
          id: "prices#p#{index}",
          health: health,
          runner: "r2",
          factory: "prices",
          partition: %{group: "prices", index: index, count: 3, range: {index, index}},
          node: node()
        }
      end
    end

    test "a group rebinding off a dead runner reads recovering inside the grace" do
      # The unmet-intent grace is the configured default of 120 s.
      ConvergenceFixtures.stub_registry_rows(
        "trading",
        partition_group(:failed) ++ [%{id: "quotes", health: :unbound, runner: nil, node: nil}],
        2_000
      )

      text = capture_io(fn -> Cli.call("Relay.list", Jason.encode!(["trading"]), []) end)
      assert text =~ ~r/prices\s+recovering\s+3\/3/
      assert text =~ ~r/quotes\s+recovering\s/
      refute text =~ "failed"

      rows =
        Jason.decode!(
          capture_io(fn ->
            Cli.call("Relay.list", Jason.encode!(["trading"]), output: "json")
          end)
        )

      assert Enum.map(rows, &{&1["id"], &1["health"]}) ==
               [{"prices", "recovering"}, {"quotes", "recovering"}]
    end

    test "a group still failed past the grace reads failed" do
      ConvergenceFixtures.stub_registry_rows("trading", partition_group(:failed), 500_000)

      assert capture_io(fn -> Cli.call("Relay.list", Jason.encode!(["trading"]), []) end) =~
               ~r/prices\s+failed\s+3\/3/
    end

    test "json output carries per-relay health, runner, and node" do
      ConvergenceFixtures.stub_registry_rows(
        "shop",
        [
          %{id: "r1", health: :healthy, runner: "e1", node: :a@h},
          %{id: "r2", health: :failed, runner: nil, node: nil}
        ],
        500_000
      )

      out =
        capture_io(fn -> Cli.call("Relay.list", Jason.encode!(["shop"]), output: "json") end)

      assert {:ok, decoded} = Jason.decode(out)

      assert Enum.map(decoded, &Map.take(&1, ["id", "health", "runner", "node"])) == [
               %{"id" => "r1", "health" => "healthy", "runner" => "e1", "node" => "a@h"},
               %{"id" => "r2", "health" => "failed", "runner" => nil, "node" => nil}
             ]
    end

    test "text output shows a HEALTH column and surfaces a FAILED, unplaced relay" do
      ConvergenceFixtures.stub_registry_rows(
        "shop",
        [%{id: "r2", health: :failed, runner: nil, node: nil}],
        500_000
      )

      out = capture_io(fn -> Cli.call("Relay.list", Jason.encode!(["shop"]), []) end)

      assert out =~ "HEALTH"
      assert out =~ "r2"
      assert out =~ "failed"
      assert out =~ "(unplaced)"
    end

    test "empty registry renders a friendly message, not a crash" do
      stub(HubManager, :relay_rows, fn _ns -> [] end)
      out = capture_io(fn -> Cli.call("Relay.list", Jason.encode!(["shop"]), []) end)
      assert out =~ "No relays found"
    end
  end

  describe "cluster_status/2" do
    test "json reports the local node as leader, per-node relay counts, unplaced, and a health rollup" do
      stub(HubManager, :get_hub_nodes, fn _ns, true -> [node()] end)

      stub(HubManager, :relay_rows, fn _ns ->
        [
          %{id: "r1", health: :healthy, runner: "e1", node: node()},
          %{id: "r2", health: :degraded, runner: "e2", node: node()},
          %{id: "r4", health: :failed, runner: nil, node: nil}
        ]
      end)

      stub(HubManager, :recovery_state, fn hub_id ->
        if hub_id == Flezha.Identificator.hub("shop"),
          do: {:ok, :recovering},
          else: {:error, :unknown_hub}
      end)

      out =
        capture_io(fn ->
          Cli.call("Controller.cluster_status", Jason.encode!(["shop"]), output: "json")
        end)

      assert {:ok, d} = Jason.decode(out)
      # Each node reports the recovery state of the namespace's own hub.
      assert [%{"recovery_state" => "recovering"}] = d["nodes"]
      assert d["leader"] == to_string(node())
      assert d["is_leader"] == true
      assert d["unplaced_relays"] == 1
      assert d["relay_health"] == %{"healthy" => 1, "degraded" => 1, "failed" => 1}
    end

    test "text output names the local node as leader" do
      stub(HubManager, :get_hub_nodes, fn _ns, true -> [node()] end)

      stub(HubManager, :relay_rows, fn _ns ->
        [%{id: "r1", health: :healthy, runner: "e1", node: node()}]
      end)

      stub(HubManager, :recovery_state, fn _hub_id -> {:ok, :normal} end)

      out =
        capture_io(fn -> Cli.call("Controller.cluster_status", Jason.encode!(["shop"]), []) end)

      assert out =~ "Cluster status for namespace 'shop'"
      assert out =~ "Leader:"
      assert out =~ to_string(node())
      assert out =~ "1 relay(s)"
      assert out =~ "[leader]"
      assert out =~ "healthy=1"
    end
  end
end

defmodule Flezha.Service.Cli.AuthzRenderTest do
  @moduledoc """
  finish-platform-authn-authz §8.1: the `flz auth|token|role|namespace` verb
  families as the caller sees them — thin renderers over the authorized
  wrappers, driven end to end against live auth stores.
  """
  # async: false: a shared on-disk root.
  use FlezhaTest.Case, fixtures: [{:auth_stores, prefix: "flz_authz_render"}], async: false

  import ExUnit.CaptureIO

  alias Flezha.AuthZ.{AuthState, Bootstrap, Config, RoleStore, TokenStore}
  alias Flezha.Observability.{RingBufferSink, TestSinks}

  alias Flezha.Service.Cli

  test "the whole verb-family flow: bootstrap → namespace → role → token → whoami" do
    # `flz auth bootstrap` — claims and prints the minted admin token.
    :ok = Bootstrap.check_and_write()
    raw_bootstrap = File.read!(Config.bootstrap_token_path()) |> String.trim()

    bootstrap_out =
      capture_io(fn -> Cli.call("Namespace.bootstrap", Jason.encode!([raw_bootstrap]), []) end)

    assert bootstrap_out =~ "bootstrap claimed"
    assert bootstrap_out =~ "shown once"
    [admin_token] = Regex.run(~r/flzt_[a-z0-9]+/, bootstrap_out)

    # A second claim says the admin is claimed and names the recovery verb,
    # never an Elixir term or the file a claimed controller does not write.
    reclaim_err = cli_refusal("Namespace.bootstrap", [raw_bootstrap], [], 6)

    assert reclaim_err =~ "already claimed"
    assert reclaim_err =~ "flz auth rebootstrap"
    refute reclaim_err =~ ":bootstrap_already_completed"

    # `flz namespace create` with quota overrides (the CLI's `--X` JSON).
    create_out =
      capture_io(fn ->
        Cli.call(
          "Namespace.create",
          Jason.encode!(["render-ns", %{quotas: Jason.decode!(~s({"max_runners": "2"}))}]),
          token: admin_token
        )
      end)

    assert create_out =~ "namespace render-ns created"
    assert create_out =~ "max_runners: 2"

    # `flz namespace show` / `set-quota`.
    show_out =
      capture_io(fn ->
        Cli.call("Namespace.get", Jason.encode!(["render-ns"]), token: admin_token)
      end)

    assert show_out =~ "namespace: render-ns"

    set_out =
      capture_io(fn ->
        Cli.call(
          "Namespace.set_quotas",
          Jason.encode!(["render-ns", Jason.decode!(~s({"max_relays": 7}))]),
          token: admin_token
        )
      end)

    assert set_out =~ "max_relays: 7"

    # `flz role bind` / `list`.
    bind_out =
      capture_io(fn ->
        Cli.call("Namespace.bind_role", Jason.encode!(["carol", "ops", "render-ns"]),
          token: admin_token
        )
      end)

    assert bind_out =~ "bound carol as ops@render-ns"

    role_list_out =
      capture_io(fn ->
        Cli.call("Namespace.list_roles", Jason.encode!([]), token: admin_token)
      end)

    assert role_list_out =~ "carol  ops@render-ns"

    # `flz token issue` / `whoami` / `list` / `revoke`.
    issue_out =
      capture_io(fn ->
        Cli.call(
          "Namespace.issue_token",
          Jason.encode!(["carol", "render-ns", "ops", %{ttl: 3_600}]),
          token: admin_token
        )
      end)

    assert issue_out =~ "token issued for carol"
    [carol_token] = Regex.run(~r/flzt_[a-z0-9]+/, issue_out)
    [_, token_id] = Regex.run(~r/id:\s+([0-9a-f]+)/, issue_out)

    whoami_out =
      capture_io(fn -> Cli.call("Namespace.whoami", Jason.encode!([]), token: carol_token) end)

    assert whoami_out =~ "principal: carol"
    assert whoami_out =~ "namespace: render-ns"

    list_out =
      capture_io(fn -> Cli.call("Namespace.tokens", Jason.encode!([]), token: admin_token) end)

    assert list_out =~ "carol  ops@render-ns  active"

    # The row carries its times and issuer, so a console can say when and by whom.
    assert %{"created_at" => issued, "expires_at" => expires, "revoked_at" => nil} =
             carol = token_json_row(admin_token, token_id)

    assert expires == issued + 3_600
    assert is_binary(carol["created_by"])

    revoke_out =
      capture_io(fn ->
        Cli.call("Namespace.revoke_token", Jason.encode!([token_id]), token: admin_token)
      end)

    assert revoke_out =~ "revoked token"
    assert is_integer(token_json_row(admin_token, token_id)["revoked_at"])
    assert {:error, %Flezha.Error{code: "authz.token_revoked"}} = TokenStore.verify(carol_token)

    # `flz role unbind` + `flz namespace delete` close the loop.
    capture_io(fn ->
      Cli.call("Namespace.unbind_role", Jason.encode!(["carol", nil]), token: admin_token)
    end)

    delete_out =
      capture_io(fn ->
        Cli.call("Namespace.delete", Jason.encode!(["render-ns"]), token: admin_token)
      end)

    assert delete_out =~ "namespace render-ns deleted"
  end

  test "an unknown role string is refused by name" do
    :ok = AuthState.set_enabled()
    {:ok, admin, _} = TokenStore.issue("render-admin", "_system", :admin, 3_600)

    out =
      cli_refusal("Namespace.bind_role", ["dave", "superuser", "render-ns"], [token: admin], 2)

    assert out =~ "unknown_role"
  end

  describe "the role surface carries a set" do
    setup do
      :ok = AuthState.set_enabled()
      {:ok, admin, _} = TokenStore.issue("render-admin", "_system", :admin, 3_600)
      %{admin: admin}
    end

    test "a bind reports the whole set the identity now holds", %{admin: admin} do
      bind = fn role ->
        capture_io(fn ->
          Cli.call("Namespace.bind_role", Jason.encode!(["runner@render-ns", role, "render-ns"]),
            token: admin
          )
        end)
      end

      assert bind.("spawner") =~ "bound runner@render-ns as spawner@render-ns"
      out = bind.("stopper")
      assert out =~ "bound runner@render-ns as stopper@render-ns"
      assert out =~ "runner@render-ns  spawner,stopper@render-ns"
    end

    test "a bind in another namespace is refused naming the one it holds", %{admin: admin} do
      capture_io(fn ->
        Cli.call("Namespace.bind_role", Jason.encode!(["runner@render-ns", "app", "render-ns"]),
          token: admin
        )
      end)

      out =
        cli_refusal(
          "Namespace.bind_role",
          ["runner@render-ns", "viewer", "_system"],
          [token: admin],
          6
        )

      assert out =~ "render-ns"

      assert {:ok, %{roles: [:app], namespace: "render-ns"}} =
               RoleStore.lookup("runner@render-ns")
    end

    test "unbind takes one role, the last role, or every role", %{admin: admin} do
      for role <- ["spawner", "stopper"] do
        capture_io(fn ->
          Cli.call("Namespace.bind_role", Jason.encode!(["runner@render-ns", role, "render-ns"]),
            token: admin
          )
        end)
      end

      out =
        capture_io(fn ->
          Cli.call("Namespace.unbind_role", Jason.encode!(["runner@render-ns", "stopper"]),
            token: admin
          )
        end)

      assert out =~ "unbound stopper from runner@render-ns"
      assert out =~ "runner@render-ns  spawner@render-ns"

      # A role the identity does not hold succeeds and changes nothing.
      capture_io(fn ->
        Cli.call("Namespace.unbind_role", Jason.encode!(["runner@render-ns", "stopper"]),
          token: admin
        )
      end)

      assert {:ok, %{roles: [:spawner]}} = RoleStore.lookup("runner@render-ns")

      capture_io(fn ->
        Cli.call("Namespace.unbind_role", Jason.encode!(["runner@render-ns", "spawner"]),
          token: admin
        )
      end)

      assert :error = RoleStore.lookup("runner@render-ns")

      capture_io(fn ->
        Cli.call(
          "Namespace.bind_role",
          Jason.encode!(["runner@render-ns", "spawner", "render-ns"]),
          token: admin
        )
      end)

      out =
        capture_io(fn ->
          Cli.call("Namespace.unbind_role", Jason.encode!(["runner@render-ns", nil]),
            token: admin
          )
        end)

      assert out =~ "unbound runner@render-ns"
      assert :error = RoleStore.lookup("runner@render-ns")
    end

    test "a token caller's act record names the token's one role", %{admin: admin} do
      {:ok, caller, _} = TokenStore.issue("carol", "render-ns", :ops, 3_600)
      boundary = System.monotonic_time(:microsecond)

      # The stop itself fails (nothing is running); an authorized act that
      # failed is still an act, so the record is written either way.
      capture_io(:stderr, fn ->
        capture_io(fn ->
          catch_exit(Cli.call("Relay.stop", Jason.encode!(["render-ns", "o1"]), token: caller))
        end)
      end)

      TestSinks.settle(RingBufferSink)

      assert Enum.any?(RingBufferSink.query(:relay_service, boundary), fn signal ->
               signal.name == :stop and signal.metadata[:caller_roles] == [:ops]
             end)

      # The admin's own role verb names its role the same way.
      boundary = System.monotonic_time(:microsecond)

      capture_io(fn ->
        Cli.call("Namespace.unbind_role", Jason.encode!(["carol", nil]), token: admin)
      end)

      TestSinks.settle(RingBufferSink)

      assert Enum.any?(RingBufferSink.query(:namespace_service, boundary), fn signal ->
               signal.metadata[:caller_roles] == [:admin]
             end)
    end

    test "an unknown role on unbind is refused by name", %{admin: admin} do
      out = cli_refusal("Namespace.unbind_role", ["carol", "superuser"], [token: admin], 2)

      assert out =~ "unknown_role"
    end

    test "the list prints one line per identity, one role or many", %{admin: admin} do
      for {principal, role} <- [
            {"runner@render-ns", "spawner"},
            {"runner@render-ns", "stopper"},
            {"alice", "ops"}
          ] do
        capture_io(fn ->
          Cli.call("Namespace.bind_role", Jason.encode!([principal, role, "render-ns"]),
            token: admin
          )
        end)
      end

      out =
        capture_io(fn -> Cli.call("Namespace.list_roles", Jason.encode!([]), token: admin) end)

      assert out =~ "runner@render-ns  spawner,stopper@render-ns"
      assert out =~ "alice  ops@render-ns"
    end

    # Fails when: a tenant's viewer reads every namespace's principals and roles.
    test "a tenant-bound caller sees its own namespace's bindings only", %{admin: admin} do
      for {principal, namespace} <- [{"bob", "render-ns"}, {"carol", "other-ns"}] do
        capture_io(fn ->
          Cli.call("Namespace.bind_role", Jason.encode!([principal, "ops", namespace]),
            token: admin
          )
        end)
      end

      {:ok, viewer, _} = TokenStore.issue("render-viewer", "render-ns", :viewer, 3_600)

      out =
        capture_io(fn -> Cli.call("Namespace.list_roles", Jason.encode!([]), token: viewer) end)

      assert out =~ "bob  ops@render-ns"
      refute out =~ "carol"
      refute out =~ "render-admin"
    end
  end

  for role <- [:app, :spawner, :stopper] do
    test "a runner principal is bound to #{role}, in the order the CLI takes" do
      :ok = AuthState.set_enabled()
      {:ok, admin, _} = TokenStore.issue("render-admin", "_system", :admin, 3_600)
      role = unquote(role)

      out =
        capture_io(fn ->
          Cli.call(
            "Namespace.bind_role",
            Jason.encode!(["runner@render-ns", "#{role}", "render-ns"]),
            token: admin
          )
        end)

      assert out =~ "bound runner@render-ns as #{role}@render-ns"

      assert {:ok, %{roles: [^role], namespace: "render-ns"}} =
               RoleStore.lookup("runner@render-ns")
    end

    test "no credential is issued for #{role}, a role only a runner principal is bound to" do
      out = refused_token_issue("#{unquote(role)}")

      assert out =~ "authz.role_not_issuable"
      assert out =~ "'#{unquote(role)}' is a role a runner's principal is bound to"
      refute out =~ "unknown_role"
    end
  end

  test "a token for a name that is no role is refused as an unknown role" do
    assert refused_token_issue("superuser") =~ "authz.unknown_role"
  end

  # `flz token issue` sends the lifetime as written, so this is its only check.
  test "a token lifetime of zero is refused" do
    assert refused_token_issue("ops", 0) =~ "authz.invalid_ttl"
  end

  defp refused_token_issue(role, ttl \\ 3_600) do
    :ok = AuthState.set_enabled()
    {:ok, admin, _} = TokenStore.issue("render-admin", "_system", :admin, 3_600)

    cli_refusal(
      "Namespace.issue_token",
      ["carol", "render-ns", role, %{ttl: ttl}],
      [token: admin],
      2
    )
  end

  # The stderr of a CLI call that exits with `exit`.
  defp cli_refusal(verb, args, opts, exit) do
    capture_io(:stderr, fn ->
      assert catch_exit(Cli.call(verb, Jason.encode!(args), opts)) == {:shutdown, exit}
    end)
  end

  defp token_json_row(admin_token, token_id) do
    capture_io(fn ->
      Cli.call("Namespace.tokens", Jason.encode!([]), token: admin_token, output: "json")
    end)
    |> Jason.decode!()
    |> Enum.find(&String.starts_with?(token_id, &1["id"]))
  end
end

defmodule Flezha.Service.Cli.ReplayTest do
  @moduledoc """
  `flz relay replay <cell> --from <seq|ts>` on a Cell of a steps class:
  refused before anything is rewound, naming the journal as the instance's
  baseline and `flz relay rewind` as the verb to use.
  """
  # async: false: the class registry and the state tree are node-global.
  use FlezhaTest.Case, fixtures: [:tmp_app_data, :state_tree], async: false

  import ExUnit.CaptureIO

  alias Flezha.Relay.ClassRegistry
  alias Flezha.Service.Cli
  alias Flezha.State.{TestHelpers, WalWriter}

  test "a steps Cell is refused, and nothing is rewound" do
    class = "rp_#{System.unique_integer([:positive])}"
    cell = {class, "task", "key"}
    {:ok, writer} = Flezha.State.Supervisor.find_or_start(cell)
    :ok = WalWriter.append(writer, 0, TestHelpers.stub_header(), "x0")
    :ok = ClassRegistry.put(class, :steps)
    on_exit(fn -> ClassRegistry.delete(class) end)

    stderr =
      capture_io(:stderr, fn ->
        try do
          Cli.call("Relay.replay", Jason.encode!(["default", "#{class}/task/key", "0"]), [])
        catch
          :exit, _ -> :ok
        end
      end)

    assert stderr =~ "is a Cell of a steps relay"
    assert stderr =~ "a committed iteration across every Cell"
    assert stderr =~ "hint: flz relay rewind"
    assert WalWriter.outbox_epoch(writer) == 0, "no epoch was bumped"
  end
end

defmodule Flezha.Service.Cli.TargetTest do
  @moduledoc """
  A `flz state at` target or a `flz relay replay --from` that is neither a
  WAL seq nor an ISO 8601 time, or is a negative seq, is refused as a bad
  argument before anything is read or rewound, never read as seq 0.
  """
  # async: false: the state tree is node-global.
  use FlezhaTest.Case, fixtures: [:tmp_app_data, :state_tree], async: false

  import ExUnit.CaptureIO

  alias Flezha.Service.{Cli, Steps}
  alias Flezha.State.{TestHelpers, WalWriter}

  @garbage ["garbage", "12x", "", "seq:5", "2026-13-45T99:00:00Z"]

  test "the parser keeps a seq and a time" do
    assert Steps.target("7", "target") == {:ok, {:seq, 7}}
    assert Steps.target("0", "target") == {:ok, {:seq, 0}}
    assert {:ok, {:timestamp, _}} = Steps.target("2026-10-04T12:00:00Z", "target")
  end

  test "the parser refuses text and a negative seq, naming the argument" do
    for written <- @garbage do
      assert {:error, %Flezha.Error{code: "usage.invalid_number", details: details}} =
               Steps.target(written, "from")

      assert details["option"] == "from", inspect(details)
    end

    assert {:error, %Flezha.Error{code: "usage.negative", message: message}} =
             Steps.target("-5", "target")

    assert message == "target must be zero or more, got -5"
  end

  for written <- @garbage ++ ["-5"] do
    test "flz state at <cell> #{inspect(written)} is refused" do
      class = written_cell("tt")
      stderr = refusal("Steps.time_travel", ["#{class}/task/key", unquote(written)])
      assert stderr =~ ~r/target (must be zero or more|value)/, inspect(stderr)
    end

    test "flz relay replay --from #{inspect(written)} is refused before anything is rewound" do
      class = written_cell("rp")
      stderr = refusal("Relay.replay", ["default", "#{class}/task/key", unquote(written)])
      assert stderr =~ ~r/from (must be zero or more|value)/, inspect(stderr)
    end
  end

  defp written_cell(prefix) do
    class = "#{prefix}_#{System.unique_integer([:positive])}"
    {:ok, writer} = Flezha.State.Supervisor.find_or_start({class, "task", "key"})
    for n <- 0..3, do: :ok = WalWriter.append(writer, n, TestHelpers.stub_header(), "x")
    class
  end

  # The stderr of a CLI call refused as a bad argument: exit 2, nothing answered.
  defp refusal(verb, args) do
    capture_io(:stderr, fn ->
      assert catch_exit(Cli.call(verb, Jason.encode!(args), [])) == {:shutdown, 2}
    end)
  end
end
