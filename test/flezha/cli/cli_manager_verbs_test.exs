defmodule Flezha.Service.CliManager.MemTopTest do
  # async: false: the live controller and its fleet.
  use ExUnit.Case, async: false

  alias Flezha.Service.CliManager

  describe "build_mem_top_snapshot/0" do
    test "returns the documented keys with reasonable shapes" do
      snapshot = CliManager.build_mem_top_snapshot()

      assert is_binary(snapshot.captured_at)
      assert is_binary(snapshot.node)

      mem = snapshot.erlang_memory
      assert is_map(mem)
      assert is_integer(mem.total) and mem.total > 0
      assert is_integer(mem.binary) and mem.binary >= 0
      assert is_integer(mem.processes) and mem.processes > 0
      assert is_integer(mem.ets) and mem.ets >= 0

      top = snapshot.top_processes
      assert is_list(top)
      assert length(top) <= 20
      assert top != [], "expected ≥ 1 BEAM process in the snapshot"

      # Descending order by :memory.
      memories = Enum.map(top, & &1.memory)
      assert memories == Enum.sort(memories, :desc)

      first = hd(top)

      for k <- [
            :pid,
            :memory,
            :initial_call,
            :current_function,
            :message_queue_len,
            :heap_size,
            :total_heap_size
          ] do
        assert Map.has_key?(first, k), "top_processes missing #{k}: #{inspect(first)}"
      end

      ets = snapshot.ets_tables
      assert is_list(ets)
      ets_memories = Enum.map(ets, & &1.memory_bytes)
      assert ets_memories == Enum.sort(ets_memories, :desc)

      assert is_list(snapshot.dets_files)

      assert is_map(snapshot.recon_alloc_allocated_types)
    end

    test "snapshot is JSON-encodable" do
      json = snapshot_json()
      assert is_binary(json)
      assert {:ok, decoded} = Jason.decode(json)
      assert is_map(decoded)
      assert is_list(decoded["top_processes"])
      assert is_map(decoded["erlang_memory"])
    end

    defp snapshot_json do
      Jason.encode!(CliManager.build_mem_top_snapshot())
    end
  end
end

defmodule Flezha.Service.CliManagerSendTest do
  @moduledoc """
  `flz operator send <id> --text <payload> [--channel <ch>]` operations surface:
  the confirming verdict from `OperatorManager.send_io_confirming/5` is reported
  and an optional channel is threaded into the `:io_in` metadata. The verdict
  logic itself is covered in `overwatch_confirming_ingest_test`. The error
  verdicts additionally `exit({:shutdown, 1})` for a non-zero CLI exit, so only the
  success verdicts are asserted in-process; an error exit is exercised by a live
  CLI run in the change's verification.
  """
  # async: false: Mimic global mode; Mimic stubs; the live controller and its fleet.
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO
  import Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Service.CliManager

  setup :set_mimic_global_until_exit

  defp instance_id(), do: "snd_#{System.unique_integer([:positive])}"

  # Stub `send_io_confirming` to answer a fixed verdict and echo its args to the
  # test process, so both the output mapping and the metadata construction are
  # asserted without a live operator.
  defp run_send(inst, verdict, channel) do
    test = self()

    stub(Flezha.Service.OperatorManager, :send_io_confirming, fn id, ns, msg, md ->
      Kernel.send(test, {:sent, id, ns, msg, md})
      verdict
    end)

    capture_io(fn -> CliManager.operator_send("default", inst, "payload", channel) end)
  end

  test "a delivered payload is reported and carries empty metadata when no channel is given" do
    inst = instance_id()

    out = run_send(inst, :ok, "")

    assert out =~ "Delivered to '#{inst}'"
    assert_received {:sent, ^inst, "default", "payload", %{}}
  end

  test "a channel is threaded into the io_in metadata" do
    inst = instance_id()

    run_send(inst, :ok, "in")

    assert_received {:sent, ^inst, "default", "payload", %{"channel" => "in"}}
  end

  test "an absorbed duplicate is reported" do
    inst = instance_id()

    assert run_send(inst, {:ok, :absorbed}, "") =~ "Absorbed (duplicate)"
  end

  test "a payload journaled for a suspended instance is reported" do
    inst = instance_id()

    assert run_send(inst, {:ok, :suspended}, "") =~ "delivers on wake"
  end
end

defmodule Flezha.Service.CliManagerRewindTest do
  @moduledoc """
  `flz operator rewind <instance> --to-iteration N` operations surface: how a
  rewind that went through, and each verdict the Overwatch guard can return, is
  reported. The guard's own logic is covered in `overwatch_suspension_test`.
  """
  # async: false: Mimic global mode; Mimic stubs.
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO
  import Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Service.CliManager

  setup :set_mimic_global_until_exit

  defp instance_id(), do: "rw_#{System.unique_integer([:positive])}"

  # `operator_rewind` resolves a pid and calls `{:rewind, n}` on it, so a process
  # that answers with a fixed verdict exercises the whole CLI surface.
  defp stub_overwatch(inst, verdict) do
    pid = spawn_link(fn -> reply_loop(verdict) end)
    stub(Flezha.Service.OperatorManager, :get_operator_pid, fn _ns, ^inst -> {:ok, pid} end)
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
    stderr =
      capture_io(:stderr, fn ->
        stdout =
          capture_io(fn ->
            try do
              CliManager.operator_rewind("default", inst, to_iter)
            catch
              :exit, _ -> :ok
            end
          end)

        send(self(), {:stdout, stdout})
      end)

    assert_received {:stdout, stdout}
    stdout <> stderr
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
    assert out =~ "(operator.iteration_not_committed)"
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
    assert out =~ "no compensate hook, so it is refused (operator.rewind_uncovered)"
  end

  test "an unknown operator is reported, not crashed on" do
    inst = instance_id()
    stub(Flezha.Service.OperatorManager, :get_operator_pid, fn _ns, ^inst -> {:error, :nf} end)

    assert rewind(inst, 1) =~ "(operator.not_running)"
  end
end

defmodule Flezha.Service.CliManagerDlqTest do
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
  alias Flezha.Operator.ClassRegistry
  alias Flezha.Service.CliManager
  alias FlezhaTest.Helper.DlqFixtures

  setup do
    ClassRegistry.ensure_table()

    rc = "dlqcli_" <> Integer.to_string(System.unique_integer([:positive]))

    entry = %{
      operator_class: rc,
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

    listing = capture_io(fn -> CliManager.dlq_list(entry.operator_class) end)
    assert listing =~ ref
    assert listing =~ "input"
    assert listing =~ "kaboom"

    shown = capture_io(fn -> CliManager.dlq_show(ref) end)
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

    output = capture_io(fn -> CliManager.dlq_replay(ref) end)

    assert_received {:"$gen_cast", {:io_in, "poison-bytes", %{"channel" => "input"}}}
    assert output =~ "replay succeeded"
    assert {:error, :not_found} = Store.get(ref)
  end

  test "failing replay re-parks with generation 2", %{rc: rc, entry: entry} do
    {:ok, ref} = Store.append(entry)

    overwatch = DlqFixtures.spawn_reparking_overwatch(entry, self())

    ClassRegistry.put_overwatch(rc, overwatch, nil)

    output = capture_io(fn -> CliManager.dlq_replay(ref) end)

    assert_received :re_parked
    assert output =~ "re-parked with generation 2"
    assert {:ok, still_there, _payload} = Store.get(ref)
    assert still_there["generation"] == 2
    assert still_there["status"] == "parked"
  end

  test "drop removes the entry and audits it on the dlq service source", %{entry: entry} do
    {:ok, ref} = Store.append(entry)
    boundary = System.monotonic_time(:microsecond)

    output = capture_io(fn -> CliManager.dlq_drop(ref) end)

    assert output =~ "dropped #{ref}"
    TestSinks.settle(RingBufferSink)

    assert Enum.any?(RingBufferSink.query(:dlq_service, boundary), fn signal ->
             signal.name == :drop and signal.metadata[:entry] == ref and
               signal.metadata[:caller_source] == :cli
           end)

    assert {:error, :not_found} = Store.get(ref)
  end
end

defmodule Flezha.Service.CliManager.ClusterTest do
  @moduledoc """
  CLI surface for `flz operator list` and `flz cluster status`. The data
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
  alias Flezha.Service.CliManager
  alias Flezha.Service.HubManager
  alias FlezhaTest.Helper.ConvergenceFixtures

  describe "list_operators/2 (health column)" do
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

      text = capture_io(fn -> CliManager.list_operators("trading") end)
      assert text =~ ~r/prices\s+recovering\s+3\/3/
      assert text =~ ~r/quotes\s+recovering\s/
      refute text =~ "failed"

      %{"operators" => rows} =
        Jason.decode!(capture_io(fn -> CliManager.list_operators("trading", output: "json") end))

      assert Enum.map(rows, &{&1["id"], &1["health"]}) ==
               [{"prices", "recovering"}, {"quotes", "recovering"}]
    end

    test "a group still failed past the grace reads failed" do
      ConvergenceFixtures.stub_registry_rows("trading", partition_group(:failed), 500_000)

      assert capture_io(fn -> CliManager.list_operators("trading") end) =~
               ~r/prices\s+failed\s+3\/3/
    end

    test "json output carries per-operator health, runner, and node" do
      ConvergenceFixtures.stub_registry_rows(
        "shop",
        [
          %{id: "r1", health: :healthy, runner: "e1", node: :a@h},
          %{id: "r2", health: :failed, runner: nil, node: nil}
        ],
        500_000
      )

      out = capture_io(fn -> CliManager.list_operators("shop", output: "json") end)

      assert {:ok, decoded} = Jason.decode(out)
      assert decoded["namespace"] == "shop"

      assert decoded["operators"] == [
               %{"id" => "r1", "health" => "healthy", "runner" => "e1", "node" => "a@h"},
               %{"id" => "r2", "health" => "failed", "runner" => nil, "node" => nil}
             ]
    end

    test "text output shows a HEALTH column and surfaces a FAILED, unplaced operator" do
      ConvergenceFixtures.stub_registry_rows(
        "shop",
        [%{id: "r2", health: :failed, runner: nil, node: nil}],
        500_000
      )

      out = capture_io(fn -> CliManager.list_operators("shop") end)

      assert out =~ "HEALTH"
      assert out =~ "r2"
      assert out =~ "failed"
      assert out =~ "(unplaced)"
    end

    test "empty registry renders a friendly message, not a crash" do
      stub(HubManager, :operator_rows, fn _ns -> [] end)
      out = capture_io(fn -> CliManager.list_operators("shop") end)
      assert out =~ "No operators found"
    end
  end

  describe "cluster_status/2" do
    test "json reports the local node as leader, per-node operator counts, unplaced, and a health rollup" do
      stub(HubManager, :get_hub_nodes, fn _ns, true -> [node()] end)

      stub(HubManager, :operator_rows, fn _ns ->
        [
          %{id: "r1", health: :healthy, runner: "e1", node: node()},
          %{id: "r2", health: :degraded, runner: "e2", node: node()},
          %{id: "r4", health: :failed, runner: nil, node: nil}
        ]
      end)

      stub(HubManager, :recovery_state, fn _hub_id -> {:ok, :normal} end)

      out = capture_io(fn -> CliManager.cluster_status("shop", output: "json") end)

      assert {:ok, d} = Jason.decode(out)
      assert d["leader"] == to_string(node())
      assert d["is_leader"] == true
      assert d["unplaced_operators"] == 1
      assert d["operator_health"] == %{"healthy" => 1, "degraded" => 1, "failed" => 1}
    end

    test "text output names the local node as leader" do
      stub(HubManager, :get_hub_nodes, fn _ns, true -> [node()] end)

      stub(HubManager, :operator_rows, fn _ns ->
        [%{id: "r1", health: :healthy, runner: "e1", node: node()}]
      end)

      stub(HubManager, :recovery_state, fn _hub_id -> {:ok, :normal} end)

      out = capture_io(fn -> CliManager.cluster_status("shop") end)

      assert out =~ "Cluster status for namespace 'shop'"
      assert out =~ "Leader:"
      assert out =~ to_string(node())
      assert out =~ "1 operator(s)"
      assert out =~ "[leader]"
      assert out =~ "healthy=1"
    end
  end
end

defmodule Flezha.Service.CliManagerAuthzRenderTest do
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

  alias Flezha.Service.CliManager

  test "the whole verb-family flow: bootstrap → namespace → role → token → whoami" do
    # `flz auth bootstrap <token>` — claims and prints the minted admin token.
    :ok = Bootstrap.check_and_write()
    raw_bootstrap = File.read!(Config.bootstrap_token_path()) |> String.trim()

    bootstrap_out = capture_io(fn -> CliManager.authz_render_bootstrap(raw_bootstrap) end)
    assert bootstrap_out =~ "bootstrap claimed"
    assert bootstrap_out =~ "shown once"
    [admin_token] = Regex.run(~r/flzt_[a-z0-9]+/, bootstrap_out)

    # A second claim says the admin is claimed and names the recovery verb,
    # never an Elixir term or the file a claimed controller does not write.
    reclaim_err =
      capture_io(:stderr, fn ->
        assert catch_exit(CliManager.authz_render_bootstrap(raw_bootstrap)) == {:shutdown, 6}
      end)

    assert reclaim_err =~ "already claimed"
    assert reclaim_err =~ "flz auth rebootstrap"
    refute reclaim_err =~ ":bootstrap_already_completed"

    # `flz namespace create` with quota overrides (the CLI's `--X` JSON).
    create_out =
      capture_io(fn ->
        CliManager.authz_render_namespace_create(
          "render-ns",
          ~s({"max_runners": "2"}),
          token: admin_token
        )
      end)

    assert create_out =~ "namespace render-ns created"
    assert create_out =~ "max_runners: 2"

    # `flz namespace show` / `set-quota`.
    show_out =
      capture_io(fn ->
        CliManager.authz_render_namespace_show("render-ns", token: admin_token)
      end)

    assert show_out =~ "namespace: render-ns"

    set_out =
      capture_io(fn ->
        CliManager.authz_render_namespace_set_quota(
          "render-ns",
          ~s({"max_operators": 7}),
          token: admin_token
        )
      end)

    assert set_out =~ "max_operators: 7"

    # `flz role bind` / `list`.
    bind_out =
      capture_io(fn ->
        CliManager.authz_render_role_bind("carol", "ops", "render-ns", token: admin_token)
      end)

    assert bind_out =~ "bound carol as ops@render-ns"

    role_list_out = capture_io(fn -> CliManager.authz_render_role_list(token: admin_token) end)
    assert role_list_out =~ "carol  ops@render-ns"

    # `flz token issue` / `whoami` / `list` / `revoke`.
    issue_out =
      capture_io(fn ->
        CliManager.authz_render_token_issue("carol", "render-ns", "ops",
          ttl: 3_600,
          token: admin_token
        )
      end)

    assert issue_out =~ "token issued for carol"
    [carol_token] = Regex.run(~r/flzt_[a-z0-9]+/, issue_out)
    [_, token_id] = Regex.run(~r/id:\s+([0-9a-f]+)/, issue_out)

    whoami_out = capture_io(fn -> CliManager.authz_render_whoami(token: carol_token) end)
    assert whoami_out =~ "principal: carol"
    assert whoami_out =~ "namespace: render-ns"

    list_out = capture_io(fn -> CliManager.authz_render_token_list(token: admin_token) end)
    assert list_out =~ "carol  ops@render-ns  active"

    revoke_out =
      capture_io(fn -> CliManager.authz_render_token_revoke(token_id, token: admin_token) end)

    assert revoke_out =~ "revoked token"
    assert {:error, %Flezha.Error{code: "authz.token_revoked"}} = TokenStore.verify(carol_token)

    # `flz role unbind` + `flz namespace delete` close the loop.
    capture_io(fn -> CliManager.authz_render_role_unbind("carol", "", token: admin_token) end)

    delete_out =
      capture_io(fn ->
        CliManager.authz_render_namespace_delete("render-ns", token: admin_token)
      end)

    assert delete_out =~ "namespace render-ns deleted"
  end

  test "an unknown role string is refused by name" do
    :ok = AuthState.set_enabled()
    {:ok, admin, _} = TokenStore.issue("render-admin", "_system", :admin, 3_600)

    out =
      capture_io(:stderr, fn ->
        assert catch_exit(
                 CliManager.authz_render_role_bind("dave", "superuser", "render-ns", token: admin)
               ) == {:shutdown, 2}
      end)

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
          CliManager.authz_render_role_bind("runner@render-ns", role, "render-ns", token: admin)
        end)
      end

      assert bind.("spawner") =~ "bound runner@render-ns as spawner@render-ns"
      out = bind.("stopper")
      assert out =~ "bound runner@render-ns as stopper@render-ns"
      assert out =~ "runner@render-ns  spawner,stopper@render-ns"
    end

    test "a bind in another namespace is refused naming the one it holds", %{admin: admin} do
      capture_io(fn ->
        CliManager.authz_render_role_bind("runner@render-ns", "app", "render-ns", token: admin)
      end)

      out =
        capture_io(:stderr, fn ->
          assert catch_exit(
                   CliManager.authz_render_role_bind("runner@render-ns", "viewer", "_system",
                     token: admin
                   )
                 ) == {:shutdown, 6}
        end)

      assert out =~ "render-ns"

      assert {:ok, %{roles: [:app], namespace: "render-ns"}} =
               RoleStore.lookup("runner@render-ns")
    end

    test "unbind takes one role, the last role, or every role", %{admin: admin} do
      for role <- ["spawner", "stopper"] do
        capture_io(fn ->
          CliManager.authz_render_role_bind("runner@render-ns", role, "render-ns", token: admin)
        end)
      end

      out =
        capture_io(fn ->
          CliManager.authz_render_role_unbind("runner@render-ns", "stopper", token: admin)
        end)

      assert out =~ "unbound stopper from runner@render-ns"
      assert out =~ "runner@render-ns  spawner@render-ns"

      # A role the identity does not hold succeeds and changes nothing.
      capture_io(fn ->
        CliManager.authz_render_role_unbind("runner@render-ns", "stopper", token: admin)
      end)

      assert {:ok, %{roles: [:spawner]}} = RoleStore.lookup("runner@render-ns")

      capture_io(fn ->
        CliManager.authz_render_role_unbind("runner@render-ns", "spawner", token: admin)
      end)

      assert :error = RoleStore.lookup("runner@render-ns")

      capture_io(fn ->
        CliManager.authz_render_role_bind("runner@render-ns", "spawner", "render-ns",
          token: admin
        )
      end)

      out =
        capture_io(fn ->
          CliManager.authz_render_role_unbind("runner@render-ns", "", token: admin)
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
          catch_exit(CliManager.operator_stop("render-ns", "o1", token: caller))
        end)
      end)

      TestSinks.settle(RingBufferSink)

      assert Enum.any?(RingBufferSink.query(:operator_service, boundary), fn signal ->
               signal.name == :stop and signal.metadata[:caller_roles] == [:ops]
             end)

      # The admin's own role verb names its role the same way.
      boundary = System.monotonic_time(:microsecond)

      capture_io(fn ->
        CliManager.authz_render_role_unbind("carol", "", token: admin)
      end)

      TestSinks.settle(RingBufferSink)

      assert Enum.any?(RingBufferSink.query(:namespace_service, boundary), fn signal ->
               signal.metadata[:caller_roles] == [:admin]
             end)
    end

    test "an unknown role on unbind is refused by name", %{admin: admin} do
      out =
        capture_io(:stderr, fn ->
          assert catch_exit(
                   CliManager.authz_render_role_unbind("carol", "superuser", token: admin)
                 ) == {:shutdown, 2}
        end)

      assert out =~ "unknown_role"
    end

    test "the list prints one line per identity, one role or many", %{admin: admin} do
      for {principal, role} <- [
            {"runner@render-ns", "spawner"},
            {"runner@render-ns", "stopper"},
            {"alice", "ops"}
          ] do
        capture_io(fn ->
          CliManager.authz_render_role_bind(principal, role, "render-ns", token: admin)
        end)
      end

      out = capture_io(fn -> CliManager.authz_render_role_list(token: admin) end)

      assert out =~ "runner@render-ns  spawner,stopper@render-ns"
      assert out =~ "alice  ops@render-ns"
    end
  end

  for role <- [:app, :spawner, :stopper] do
    test "a runner principal is bound to #{role}, in the order the CLI takes" do
      :ok = AuthState.set_enabled()
      {:ok, admin, _} = TokenStore.issue("render-admin", "_system", :admin, 3_600)
      role = unquote(role)

      out =
        capture_io(fn ->
          CliManager.authz_render_role_bind("runner@render-ns", "#{role}", "render-ns",
            token: admin
          )
        end)

      assert out =~ "bound runner@render-ns as #{role}@render-ns"

      assert {:ok, %{roles: [^role], namespace: "render-ns"}} =
               RoleStore.lookup("runner@render-ns")
    end

    test "no credential is issued for #{role}, a role only a runner principal is bound to" do
      :ok = AuthState.set_enabled()
      {:ok, admin, _} = TokenStore.issue("render-admin", "_system", :admin, 3_600)
      role = unquote(role)

      out =
        capture_io(:stderr, fn ->
          assert catch_exit(
                   CliManager.authz_render_token_issue("carol", "render-ns", "#{role}",
                     ttl: 3_600,
                     token: admin
                   )
                 ) == {:shutdown, 2}
        end)

      assert out =~ "unknown_role"
    end
  end
end

defmodule Flezha.Service.CliManagerReplayTest do
  @moduledoc """
  `flz operator replay <cell> --from <seq|ts>` on a Cell of a bounded class:
  refused before anything is rewound, naming the journal as the instance's
  baseline and `flz operator rewind` as the verb to use.
  """
  # async: false: the class registry and the state tree are node-global.
  use FlezhaTest.Case, fixtures: [:tmp_app_data, :state_tree], async: false

  import ExUnit.CaptureIO

  alias Flezha.Operator.ClassRegistry
  alias Flezha.Service.CliManager
  alias Flezha.State.{TestHelpers, WalWriter}

  test "a bounded Cell is refused, and nothing is rewound" do
    class = "rp_#{System.unique_integer([:positive])}"
    cell = {class, "task", "key"}
    {:ok, writer} = Flezha.State.Supervisor.find_or_start(cell)
    :ok = WalWriter.append(writer, 0, TestHelpers.stub_header(), "x0")
    :ok = ClassRegistry.put(class, :bounded)
    on_exit(fn -> ClassRegistry.delete(class) end)

    stderr =
      capture_io(:stderr, fn ->
        try do
          CliManager.operator_replay("default", "#{class}/task/key", "0")
        catch
          :exit, _ -> :ok
        end
      end)

    assert stderr =~ "is a Cell of a bounded operator"
    assert stderr =~ "a committed iteration across every Cell"
    assert stderr =~ "hint: flz operator rewind"
    assert WalWriter.outbox_epoch(writer) == 0, "no epoch was bumped"
  end
end
