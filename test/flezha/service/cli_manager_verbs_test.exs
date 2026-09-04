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

  # Error verdicts exit(:cli_error) for the CLI exit code (see cli_error!/1) while
  # a successful rewind returns normally; swallow the exit either way so the
  # printed report stays assertable.
  defp rewind(inst, to_iter) do
    capture_io(fn ->
      try do
        CliManager.operative_rewind("default", inst, to_iter)
      catch
        :exit, _ -> :ok
      end
    end)
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
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  alias Flezha.Dlq.Store
  alias Flezha.Operative.ClassRegistry
  alias Flezha.Service.CliManager
  alias FlezhaTest.Helper.DlqFixtures

  setup do
    DlqFixtures.isolated_dir(replay_grace_ms: 400)
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

  test "drop removes the entry and emits the audit event", %{entry: entry} do
    {:ok, ref} = Store.append(entry)

    handler = :"dlq_cli_test_#{System.unique_integer([:positive])}"
    parent = self()

    :telemetry.attach(
      handler,
      [:flezha, :dlq, :operator_drop],
      fn _e, m, md, _c -> send(parent, {:audit, m, md}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    output = capture_io(fn -> CliManager.dlq_drop(ref) end)

    assert output =~ "dropped #{ref}"
    assert_received {:audit, %{count: 1}, %{entry: ^ref, caller_source: :cli}}
    assert {:error, :not_found} = Store.get(ref)
  end
end

defmodule Flezha.Service.CliManagerGatedVerbsTest do
  @moduledoc """
  The four verb families that used to reach their stores with no credential:
  `flz dlq list | show`, `flz schedule add | list | remove | run`, `flz operative
  scale` and `flz operative rewind`.

  Driven from the RPC surface `flz` calls, because the credential's whole
  journey is the point — `--token` arrives as `opts`, becomes a principal in
  `Flezha.Service.Cli.Context`, and is what the service guard judges. A verb
  asked without one must refuse and say where the token goes.
  """
  # async: false: a shared on-disk root; the auth stores are singletons; Mimic
  # stubs.
  use ExUnit.Case, async: false
  use Mimic

  import ExUnit.CaptureIO

  alias Flezha.AuthZ.TokenStore
  alias Flezha.Dlq.Store, as: DlqStore
  alias Flezha.Schedule.Store, as: ScheduleStore
  alias Flezha.Service.CliManager
  alias Flezha.Service.OperativeManager
  alias FlezhaTest.Helper.AuthTestHelper
  alias FlezhaTest.Helper.DlqFixtures
  alias FlezhaTest.Helper.ScheduleTestHelper

  setup do
    AuthTestHelper.setup_auth_stores(prefix: "flz_gated_cli", enabled: true, claim: true)
    ScheduleTestHelper.isolated_store_setup()

    DlqFixtures.isolated_dir()

    # An operative class nothing runs and nothing has state for resolves to the
    # system namespace, which is where the schedule verbs are judged too — so
    # one `operator@_system` token answers for every family here.
    operative_class = "gatedcli_#{System.unique_integer([:positive])}"

    {:ok, ref, _record} =
      TokenStore.issue("gated-op", Flezha.AuthZ.Namespace.system_name(), :operator, 3600)

    {:ok, entry_ref} =
      DlqStore.append(%{
        operative_class: operative_class,
        procedure_id: "proc",
        entity_key: "k",
        channel: "input",
        origin_offset: nil,
        error: "kaboom",
        attempts: 3,
        payload: "poison-bytes"
      })

    %{token: ref, entry_ref: entry_ref, operative_class: operative_class}
  end

  # A refused verb exits `:cli_error` after printing its refusal; swallow the
  # exit so the message stays assertable. `device` is the verb family's own
  # stream — the dlq and schedule verbs print to stderr, the operative verbs to
  # stdout, and that split is observable behaviour.
  defp refusal(device \\ :stderr, fun) do
    capture_io(device, fn ->
      assert catch_exit(fun.()) == :cli_error
    end)
  end

  test "the dead-letter reads refuse without a token and answer with one", ctx do
    assert refusal(fn -> CliManager.dlq_list(ctx.operative_class) end) =~ "dlq.list: refused"
    assert refusal(fn -> CliManager.dlq_show(ctx.entry_ref) end) =~ "dlq.show: refused"

    listing = capture_io(fn -> CliManager.dlq_list(ctx.operative_class, token: ctx.token) end)
    assert listing =~ ctx.entry_ref

    shown = capture_io(fn -> CliManager.dlq_show(ctx.entry_ref, token: ctx.token) end)
    assert shown =~ "poison-bytes"
  end

  test "the schedule verbs refuse without a token and go through with one", ctx do
    add = fn opts ->
      CliManager.schedule_add(
        "digest",
        [cron: "0 8 * * *", tz: "Etc/UTC", factory: "ai_digest"] ++ opts
      )
    end

    assert refusal(fn -> add.([]) end) =~ "schedule.add: refused"
    assert refusal(fn -> CliManager.schedule_list() end) =~ "schedule.list: refused"
    assert refusal(fn -> CliManager.schedule_remove("digest") end) =~ "schedule.remove: refused"

    assert capture_io(fn -> add.(token: ctx.token) end) =~ "Schedule digest added"
    assert capture_io(fn -> CliManager.schedule_list(token: ctx.token) end) =~ "digest"

    assert capture_io(fn -> CliManager.schedule_remove("digest", token: ctx.token) end) =~
             "Schedule digest removed"
  end

  # Firing a schedule by hand starts an operative, so it is judged as
  # `operative.start` where the instance starts — the stored row's namespace,
  # not the `_system` scope the other schedule verbs share.
  test "schedule run is refused without a token and judged in the schedule's namespace",
       ctx do
    {:ok, _entry} = ScheduleStore.add(ScheduleTestHelper.entry_fields(%{name: "digest"}))
    test_pid = self()

    stub(OperativeManager, :start_workflow, fn "default", id, _factory, _init, _opts ->
      send(test_pid, {:spawned, id})
      {:ok, %{operative_id: id}}
    end)

    assert refusal(fn -> CliManager.schedule_run("digest") end) =~ "operative.start: refused"

    # The `_system` operator that manages schedules is not bound where this
    # one starts.
    assert refusal(fn -> CliManager.schedule_run("digest", token: ctx.token) end) =~
             "operative.start: refused"

    refute_received {:spawned, _}

    {:ok, bound_here, _} = TokenStore.issue("gated-op-default", "default", :operator, 3600)

    assert capture_io(fn -> CliManager.schedule_run("digest", token: bound_here) end) =~
             "Spawned digest@manual-"

    assert_received {:spawned, "digest@manual-" <> _}
  end

  test "the two verbs that move live work refuse without a token", ctx do
    assert refusal(:stdio, fn -> CliManager.operative_scale("_system", "grp", "2") end) =~
             "operative.scale: refused"

    assert refusal(:stdio, fn ->
             CliManager.operative_rewind("_system", ctx.operative_class, 1)
           end) =~ "operative.rewind: refused"

    # With one they reach their own machinery, which refuses for its own reason.
    scale_opts = ~s({"token": "#{ctx.token}"})

    assert refusal(:stdio, fn ->
             CliManager.operative_scale("_system", "grp", "2", scale_opts)
           end) =~ "Error scaling operative"

    assert refusal(:stdio, fn ->
             CliManager.operative_rewind("_system", ctx.operative_class, 1, token: ctx.token)
           end) =~ "not found"
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
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  alias Flezha.AuthZ.{AuthState, Bootstrap, Config, TokenStore}
  alias FlezhaTest.Helper.AuthTestHelper

  alias Flezha.Service.CliManager

  setup do
    _ = AuthTestHelper.setup_auth_stores(prefix: "flz_authz_render")
    :ok
  end

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
        CliManager.authz_render_token_issue("carol", "render-ns", "operator", 3_600,
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

  test "a gated verb without identity renders the token requirement" do
    :ok = AuthState.set_enabled()

    out =
      capture_io(:stderr, fn ->
        assert catch_exit(CliManager.authz_render_token_list([])) == :cli_error
      end)

    assert out =~ "--token"
  end

  test "an unknown role string is refused by name" do
    :ok = AuthState.set_enabled()

    out =
      capture_io(:stderr, fn ->
        assert catch_exit(CliManager.authz_render_role_bind("dave", "superuser", "render-ns", [])) ==
                 :cli_error
      end)

    assert out =~ "unknown_role"
  end
end
