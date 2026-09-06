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
  `flz operative send <id> --text <payload> [--channel <ch>]` operator surface:
  the confirming verdict from `OperativeManager.send_io_confirming/5` is reported
  and an optional channel is threaded into the `:io_in` metadata. The verdict
  logic itself is covered in `overwatch_confirming_ingest_test`. The error
  verdicts additionally `exit(:cli_error)` for a non-zero CLI exit, so only the
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
  # asserted without a live operative.
  defp run_send(inst, verdict, channel) do
    test = self()

    stub(Flezha.Service.OperativeManager, :send_io_confirming, fn id, ns, msg, md ->
      Kernel.send(test, {:sent, id, ns, msg, md})
      verdict
    end)

    capture_io(fn -> CliManager.operative_send("default", inst, "payload", channel) end)
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
  `flz operative rewind <instance> --to-iteration N` operator surface: how a
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

  # `operative_rewind` resolves a pid and calls `{:rewind, n}` on it, so a process
  # that answers with a fixed verdict exercises the whole CLI surface.
  defp stub_overwatch(inst, verdict) do
    pid = spawn_link(fn -> reply_loop(verdict) end)
    stub(Flezha.Service.OperativeManager, :get_operative_pid, fn _ns, ^inst -> {:ok, pid} end)
  end

  defp reply_loop(verdict) do
    receive do
      {:"$gen_call", from, {:rewind, _to_iter}} ->
        GenServer.reply(from, verdict)
        reply_loop(verdict)
    end
  end

  # A rewind that refuses exits `:cli_error` and renders on stderr; one that
  # succeeds returns normally and reports on stdout. Both streams are captured
  # so a test can assert on whichever this rewind produced.
  defp rewind(inst, to_iter) do
    stderr =
      capture_io(:stderr, fn ->
        stdout =
          capture_io(fn ->
            try do
              CliManager.operative_rewind("default", inst, to_iter)
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
    stub_overwatch(inst, {:error, {:uncommitted_iteration, [0, 1, 2]}})

    out = rewind(inst, 7)

    assert out =~ "iteration 7 has not committed"
    assert out =~ "Committed iterations: 0..2"
  end

  test "a loop with nothing committed says so rather than printing an empty range" do
    inst = instance_id()
    stub_overwatch(inst, {:error, {:uncommitted_iteration, []}})

    assert rewind(inst, 0) =~ "no committed iterations"
  end

  test "an uncompensable emit is refused, naming the channels" do
    inst = instance_id()
    stub_overwatch(inst, {:error, {:uncovered, ["replies", "audit"]}})

    out = rewind(inst, 1)

    assert out =~ "refusing to rewind"
    assert out =~ "replies, audit"
    assert out =~ "no `compensate` hook"
  end

  test "an unknown operative is reported, not crashed on" do
    inst = instance_id()
    stub(Flezha.Service.OperativeManager, :get_operative_pid, fn _ns, ^inst -> {:error, :nf} end)

    assert rewind(inst, 1) =~ "not found"
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
  alias Flezha.Operative.ClassRegistry
  alias Flezha.Service.CliManager
  alias FlezhaTest.Helper.DlqFixtures

  setup do
    ClassRegistry.ensure_table()

    rc = "dlqcli_" <> Integer.to_string(System.unique_integer([:positive]))

    entry = %{
      operative_class: rc,
      procedure_id: "proc",
      entity_key: "k",
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

    listing = capture_io(fn -> CliManager.dlq_list(entry.operative_class) end)
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
    ClassRegistry.put_overwatch(rc, self())

    output = capture_io(fn -> CliManager.dlq_replay(ref) end)

    assert_received {:"$gen_cast", {:io_in, "poison-bytes", %{"channel" => "input"}}}
    assert output =~ "replay succeeded"
    assert {:error, :not_found} = Store.get(ref)
  end

  test "failing replay re-parks with generation 2", %{rc: rc, entry: entry} do
    {:ok, ref} = Store.append(entry)

    overwatch = DlqFixtures.spawn_reparking_overwatch(entry, self())

    ClassRegistry.put_overwatch(rc, overwatch)

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
  CLI surface for `flz operative list` and `flz cluster status`. The data
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

  alias Flezha.Service.CliManager
  alias Flezha.Service.HubManager

  describe "list_operatives/2 (health column)" do
    test "json output carries per-operative health, runner, and node" do
      stub(HubManager, :operative_rows, fn _ns ->
        [
          %{id: "r1", health: :healthy, runner: "e1", node: :a@h},
          %{id: "r2", health: :failed, runner: nil, node: nil}
        ]
      end)

      out = capture_io(fn -> CliManager.list_operatives("shop", "json") end)

      assert {:ok, decoded} = Jason.decode(out)
      assert decoded["namespace"] == "shop"

      assert decoded["operatives"] == [
               %{"id" => "r1", "health" => "healthy", "runner" => "e1", "node" => "a@h"},
               %{"id" => "r2", "health" => "failed", "runner" => nil, "node" => nil}
             ]
    end

    test "text output shows a HEALTH column and surfaces a FAILED, unplaced operative" do
      stub(HubManager, :operative_rows, fn _ns ->
        [%{id: "r2", health: :failed, runner: nil, node: nil}]
      end)

      out = capture_io(fn -> CliManager.list_operatives("shop", "text") end)

      assert out =~ "HEALTH"
      assert out =~ "r2"
      assert out =~ "failed"
      assert out =~ "(unplaced)"
    end

    test "empty registry renders a friendly message, not a crash" do
      stub(HubManager, :operative_rows, fn _ns -> [] end)
      out = capture_io(fn -> CliManager.list_operatives("shop", "text") end)
      assert out =~ "No operatives found"
    end
  end

  describe "cluster_status/2" do
    test "json reports the local node as leader, per-node operative counts, unplaced, and a health rollup" do
      stub(HubManager, :get_hub_nodes, fn _ns, true -> [node()] end)

      stub(HubManager, :operative_rows, fn _ns ->
        [
          %{id: "r1", health: :healthy, runner: "e1", node: node()},
          %{id: "r2", health: :degraded, runner: "e2", node: node()},
          %{id: "r4", health: :failed, runner: nil, node: nil}
        ]
      end)

      stub(HubManager, :recovery_state, fn _hub_id -> {:ok, :normal} end)

      out = capture_io(fn -> CliManager.cluster_status("shop", "json") end)

      assert {:ok, d} = Jason.decode(out)
      assert d["leader"] == to_string(node())
      assert d["is_leader"] == true
      assert d["unplaced_operatives"] == 1
      assert d["operative_health"] == %{"healthy" => 1, "degraded" => 1, "failed" => 1}
    end

    test "text output names the local node as leader" do
      stub(HubManager, :get_hub_nodes, fn _ns, true -> [node()] end)

      stub(HubManager, :operative_rows, fn _ns ->
        [%{id: "r1", health: :healthy, runner: "e1", node: node()}]
      end)

      stub(HubManager, :recovery_state, fn _hub_id -> {:ok, :normal} end)

      out = capture_io(fn -> CliManager.cluster_status("shop", "text") end)

      assert out =~ "Cluster status for namespace 'shop'"
      assert out =~ "Leader:"
      assert out =~ to_string(node())
      assert out =~ "1 operative(s)"
      assert out =~ "[leader]"
      assert out =~ "healthy=1"
    end
  end
end

defmodule Flezha.Service.CliManagerAuthzRenderTest do
  @moduledoc """
  finish-platform-authn-authz §8.1: the `flz auth|token|role|namespace` verb
  families as the operator sees them — thin renderers over the authorized
  wrappers, driven end to end against live auth stores.
  """
  # async: false: a shared on-disk root.
  use FlezhaTest.Case, fixtures: [{:auth_stores, prefix: "flz_authz_render"}], async: false

  import ExUnit.CaptureIO

  alias Flezha.AuthZ.{AuthState, Bootstrap, Config, TokenStore}

  alias Flezha.Service.CliManager

  test "the whole verb-family flow: bootstrap → namespace → role → token → whoami" do
    # `flz auth bootstrap <token>` — claims and prints the minted admin token.
    :ok = Bootstrap.check_and_write()
    raw_bootstrap = File.read!(Config.bootstrap_token_path()) |> String.trim()

    bootstrap_out = capture_io(fn -> CliManager.authz_render_bootstrap(raw_bootstrap) end)
    assert bootstrap_out =~ "bootstrap claimed"
    assert bootstrap_out =~ "shown once"
    [admin_token] = Regex.run(~r/flzt_[a-z0-9]+/, bootstrap_out)

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
          ~s({"max_operatives": 7}),
          token: admin_token
        )
      end)

    assert set_out =~ "max_operatives: 7"

    # `flz role bind` / `list`.
    bind_out =
      capture_io(fn ->
        CliManager.authz_render_role_bind("carol", "operator", "render-ns", token: admin_token)
      end)

    assert bind_out =~ "bound carol as operator@render-ns"

    role_list_out = capture_io(fn -> CliManager.authz_render_role_list(token: admin_token) end)
    assert role_list_out =~ "carol  operator@render-ns"

    # `flz token issue` / `whoami` / `list` / `revoke`.
    issue_out =
      capture_io(fn ->
        CliManager.authz_render_token_issue("carol", "render-ns", "operator",
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
    assert list_out =~ "carol  operator@render-ns  active"

    revoke_out =
      capture_io(fn -> CliManager.authz_render_token_revoke(token_id, token: admin_token) end)

    assert revoke_out =~ "revoked token"
    assert {:error, :token_revoked} = TokenStore.verify(carol_token)

    # `flz role unbind` + `flz namespace delete` close the loop.
    capture_io(fn -> CliManager.authz_render_role_unbind("carol", token: admin_token) end)

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
               ) == :cli_error
      end)

    assert out =~ "unknown_role"
  end
end
