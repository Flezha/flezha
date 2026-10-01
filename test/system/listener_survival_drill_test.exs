defmodule FlezhaTest.System.ListenerSurvivalDrillTest do
  @moduledoc """
  A listener (`Flezha.Service.Listener.listen/4`) across everything that
  happens to the operator it listens to: sleep and wake, a runner's death, an
  Overwatch restart, the end of a `once` run, a rescale, and, under
  `:multi_node`, a move to another controller. Each drill ends the listener
  and asserts that no member carries its plugin row afterwards, and that the
  convergence loop does not start one again.

      mix test --include system test/system/listener_survival_drill_test.exs
      mix test --include system --include multi_node test/system/listener_survival_drill_test.exs
  """

  use FlezhaTest.SystemCase, capability: :runner_lifecycle

  import ExUnit.CaptureIO
  import ExUnit.CaptureLog
  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]
  import FlezhaTest.Helper.ListenerHarness
  import FlezhaTest.Helper.TierE2EHarness, only: [cleanup: 3, kill_overwatch: 2, unique: 1]

  import FlezhaTest.Helper.StepsResumeHarness,
    only: [boot_runner: 4, replace_runner: 4, suspended?: 2]

  alias Flezha.Operator.{Builder, PartitionGroup}
  alias Flezha.Partition
  alias Flezha.SDK.KeyGroup
  alias Flezha.Service.{CliManager, HubManager, Listener, OperatorManager, PluginManager}
  alias FlezhaTest.Helper.{FleetStateHelper, NodeTestHelper, TierE2EHarness}

  @moduletag reference_runtime: true
  @moduletag transport: :uds
  @moduletag timeout: 300_000

  # The convergence loop the drills prove quiet runs on every node, ticked on
  # demand; the test config leaves it off.
  @loop_env [convergence_health_enabled: true, convergence_tick_ms: 600_000]

  setup ctx do
    start_supervised!({Flezha.Convergence.Loop, tick_ms: 600_000})
    namespace = unique("listener_drill")
    id_path = TierE2EHarness.unique_id_path("listener_drill")
    on_exit(fn -> cleanup(namespace, nil, id_path) end)

    {port, runner} =
      boot_runner(namespace, ctx, id_path,
        release?: false,
        extra_env: [{"FLEZHA_FIXTURE_SLEEP_AFTER_MS", "700"}]
      )

    {:ok, namespace: namespace, id_path: id_path, port: port, runner: runner}
  end

  @tag capability: :state_persistence, skip: FlezhaTest.SystemCase.deferral(:state_persistence)
  test "a sleeper restarted asleep wakes with its declared sink beside the listener",
       %{namespace: ns} do
    path = "#{unique("drill_sink")}.ndjson"
    id = start!(ns, "sleeping_counter", :flow, [file_sink("drill_sink", path)])

    send!(ns, id, "a:2")
    assert wait_until(fn -> sink_lines(path) == ["a=2"] end, 30_000)
    assert wait_until(fn -> suspended?(ns, id) end, 30_000)

    # The restart finds it asleep and brings nothing of its table up.
    assert capture_log(fn ->
             kill_overwatch(ns, id)
             assert wait_until(fn -> suspended?(ns, id) end, 30_000)
           end) =~ "terminated"

    listener = listen(ns, id)
    send!(ns, id, "a:3")

    assert_receive {:heard, {:output, "a=5", _}}, 60_000

    assert wait_until(fn -> sink_lines(path) == ["a=2", "a=5"] end, 10_000),
           "the declared sink missed the woken output: #{inspect(sink_lines(path))}"

    stop_listening(listener, ns, [id])
  end

  @tag capability: :state_persistence, skip: FlezhaTest.SystemCase.deferral(:state_persistence)
  test "a listener hears a flow sleeper and a per_input session wake, unattached again",
       %{namespace: ns} do
    stream = start!(ns, "sleeping_counter", :flow)
    session = start!(ns, "part_echo_steps", :steps)
    stream_listener = listen(ns, stream)
    session_listener = listen(ns, session)

    send!(ns, stream, "a:1")
    send!(ns, session, "1")
    assert_receive {:heard, {:output, "a=1", _}}, 30_000
    assert_receive {:heard, {:output, "done:1", _}}, 30_000

    assert wait_until(fn -> suspended?(ns, stream) end, 30_000)
    assert wait_until(fn -> suspended?(ns, session) end, 30_000)

    send!(ns, stream, "a:2")
    send!(ns, session, "2")
    assert_receive {:heard, {:output, "a=3", _}}, 60_000
    assert_receive {:heard, {:output, "done:2", _}}, 60_000

    stop_listening(stream_listener, ns, [stream])
    stop_listening(session_listener, ns, [session])
  end

  @tag capability: :parts, skip: FlezhaTest.SystemCase.deferral(:parts)
  test "a listener hears the outputs after its runner dies and the operator is rebound",
       ctx do
    %{namespace: ns, port: port, runner: dead, id_path: id_path} = ctx
    id = start!(ns, "part_echo", :flow)
    listener = listen(ns, id)

    replace_runner(ns, ctx, {port, dead}, id_path)
    assert wait_until(fn -> rebound?(ns, id, dead) end, 60_000)
    send!(ns, id, "1")
    assert_receive {:heard, {:output, "done:1", _}}, 60_000

    stop_listening(listener, ns, [id])
  end

  @tag capability: :parts, skip: FlezhaTest.SystemCase.deferral(:parts)
  test "a listener hears the outputs after the Overwatch is killed and restarted",
       %{namespace: ns} do
    id = start!(ns, "part_echo", :flow)
    listener = listen(ns, id)

    assert capture_log(fn ->
             TierE2EHarness.force_fresh_rebind(ns, id)
             assert wait_until(fn -> bound?(ns, id) end, 60_000)
           end) =~ "terminated"

    send!(ns, id, "1")
    assert_receive {:heard, {:output, "done:1", _}}, 60_000

    stop_listening(listener, ns, [id])
  end

  @tag capability: :steps, skip: FlezhaTest.SystemCase.deferral(:steps)
  test "a listener on a once steps operator ends when the operator completes",
       %{namespace: ns} do
    id = start!(ns, "echo_steps", :steps)
    %{task: task} = listen(ns, id)
    send!(ns, id, "hi")

    assert_receive {:heard, {:output, "echo:hi", _}}, 30_000
    assert Task.await(task, 60_000) == {:ok, :operator_ended}
  end

  @tag capability: :partitioned, skip: FlezhaTest.SystemCase.deferral(:partitioned)
  test "a group listener across a rescale hears every member and leaves none behind",
       %{namespace: ns} do
    rescale_drill(ns)
  end

  describe "across controllers" do
    @describetag :multi_node
    @describetag capability: :parts, skip: FlezhaTest.SystemCase.deferral(:parts)

    # This node (A) and a peer (B), a runner on each. An operator of
    # `part_echo` placed on A moves to B once A's runner stops advertising it,
    # with both controllers up.
    setup %{namespace: ns, runner: local_runner} do
      {peer_pid, peer} = NodeTestHelper.start_peer_node(:listener_drill_b, app_env: @loop_env)
      on_exit(fn -> NodeTestHelper.stop_peer_node(peer_pid, peer) end)
      NodeTestHelper.start_runner_remote(peer, ns)
      assert wait_until(fn -> peer in HubManager.get_hub_nodes(ns, false) end, 30_000)
      id = start_on!(ns, "part_echo", node())
      %{peer: peer, id: id, local_runner: local_runner}
    end

    test "a listener attached through A survives the operator's move to B and leaves no row",
         %{namespace: ns, peer: peer, id: id} = ctx do
      listener = listen(ns, id)
      move_off_this_node(ctx)
      assert wait_until(fn -> hosted_on?(ns, id, peer) and bound?(ns, id) end, 90_000)

      send!(ns, id, "2")
      assert_receive {:heard, {:output, "done:2", _}}, 60_000
      stop_listening(listener, ns, [id])
    end

    test "a killed listener on a moved operator is detached by its heartbeat",
         %{namespace: ns, peer: peer, id: id} = ctx do
      %{task: task} = listen(ns, id)
      move_off_this_node(ctx)
      assert wait_until(fn -> hosted_on?(ns, id, peer) and bound?(ns, id) end, 90_000)

      Process.unlink(task.pid)
      Process.exit(task.pid, :kill)
      assert wait_until(fn -> listeners(ns, id) == [] end, 30_000)
      after_ticks(3)
      assert listeners(ns, id) == []
    end

    @tag capability: :partitioned, skip: FlezhaTest.SystemCase.deferral(:partitioned)
    test "a group listener across a rescale over two controllers leaves no member a row",
         %{namespace: ns} do
      rescale_drill(ns)
    end

    test "a listener on A hears an operator hosted by B after B leaves the ring",
         %{namespace: ns, peer: peer} do
      id = start_on!(ns, "part_echo", peer)
      listener = listen(ns, id)

      assert :ok =
               :erpc.call(
                 peer,
                 Flezha.Shutdown.Coordinator,
                 :shutdown,
                 [:cli, [timeout_ms: 30_000]],
                 45_000
               )

      assert wait_until(fn -> hosted_on?(ns, id, node()) and bound?(ns, id) end, 90_000)
      send!(ns, id, "1")
      assert_receive {:heard, {:output, "done:1", _}}, 60_000
      stop_listening(listener, ns, [id])
    end

    test "a file sink on an operator moved to B is disabled through a controller that never held it",
         %{namespace: ns, peer: peer, id: id} = ctx do
      {c_pid, c_node} = NodeTestHelper.start_peer_node(:listener_drill_c, app_env: @loop_env)
      on_exit(fn -> NodeTestHelper.stop_peer_node(c_pid, c_node) end)

      sink = %{
        file_sink("moved_sink", "#{unique("moved_sink")}.ndjson")
        | namespace: ns,
          operator_id: id
      }

      assert {:ok, _} = PluginManager.enable_plugin("moved_sink_c", sink)
      move_off_this_node(ctx)
      assert wait_until(fn -> hosted_on?(ns, id, peer) and bound?(ns, id) end, 90_000)

      assert {:ok, "plugin disabled"} =
               :erpc.call(c_node, PluginManager, :disable_plugin_group, ["moved_sink_c"], 30_000)

      assert wait_until(fn -> plugin_ids(ns, id) == [] end, 30_000)
      after_ticks(3)
      assert plugin_ids(ns, id) == []
    end

    test "a file sink enabled through A on an operator moved to B is disabled through A",
         %{namespace: ns, peer: peer, id: id} = ctx do
      sink = %{
        file_sink("moved_sink", "#{unique("moved_sink")}.ndjson")
        | namespace: ns,
          operator_id: id
      }

      assert {:ok, _} = PluginManager.enable_plugin("moved_sink", sink)
      move_off_this_node(ctx)
      assert wait_until(fn -> hosted_on?(ns, id, peer) and bound?(ns, id) end, 90_000)

      assert {:ok, "plugin disabled"} = PluginManager.disable_plugin_group("moved_sink")
      assert wait_until(fn -> plugin_ids(ns, id) == [] end, 30_000)
      after_ticks(3)
      assert plugin_ids(ns, id) == []
    end
  end

  # ---------- helpers ----------

  # `n` convergence ticks on every controller node, each run to its end: a row
  # a loop would start again has been started by then.
  defp after_ticks(n) do
    for node <- [node() | Node.list()], _ <- 1..n do
      assert :erpc.call(node, Flezha.Convergence.Loop, :tick_now, [], 60_000) == :ok
    end
  end

  defp rescale_drill(ns) do
    group = unique("drill_grp")

    assert {:ok, %{members: members}} =
             PartitionGroup.start(ns, %Builder{id: group, factory_id: "partition_relay"}, 2)

    for member <- members, do: assert(wait_until(fn -> bound?(ns, member) end, 30_000))
    listener = listen(ns, group, on: members)

    assert {:ok, %{members: rescaled}} = PartitionGroup.rescale(ns, group, 3)
    assert length(rescaled) == 3
    for member <- rescaled, do: assert(wait_until(fn -> bound?(ns, member) end, 60_000))

    # A key the member the rescale added owns.
    new_member = Partition.member_id(group, 2)
    key = Enum.find(1..10_000, &(owner(group, 3, "K#{&1}") == new_member))
    send!(ns, new_member, "K#{key}:heard")
    assert_receive {:heard, {:output, "K" <> _, _}}, 60_000

    stop_listening(listener, ns, rescaled)
  end

  defp plugin_ids(ns, id) do
    case HubManager.operator_plugins(ns, id) do
      {:ok, plugins} -> Enum.map(plugins, &elem(&1, 0))
      _ -> []
    end
  end

  # Probe ids until one is placed on `node`; the others are stopped again.
  defp start_on!(ns, factory, node) do
    Enum.find_value(1..30, fn i ->
      id = "#{unique(factory)}_#{i}"

      assert {:ok, %{node: placed}} =
               OperatorManager.start_operator(ns, %Builder{id: id, factory_id: factory})

      if placed == node do
        assert wait_until(fn -> bound?(ns, id) end, 30_000)
        id
      else
        OperatorManager.stop_operator(ns, id)
        nil
      end
    end) || flunk("no #{factory} landed on #{node}")
  end

  # This node's runner stops advertising `part_echo`, so its operators move.
  defp move_off_this_node(%{namespace: ns, local_runner: runner}),
    do: FleetStateHelper.sync(ns, runner, ["do_nothing", "sleeping_counter"])

  defp hosted_on?(ns, id, node) do
    match?({:ok, pid} when node(pid) == node, OperatorManager.get_operator_pid(ns, id))
  end

  defp start!(ns, factory, kind, plugins \\ []) do
    id = unique(factory)
    builder = %Builder{id: id, factory_id: factory, run: kind, opts: [plugins: plugins]}
    assert {:ok, _} = OperatorManager.start_operator(ns, builder)
    id
  end

  defp send!(ns, id, text) do
    assert capture_io(fn -> CliManager.operator_send(ns, id, text, "", []) end) =~ "'#{id}'"
  end

  # A listener in a task of its own: every event comes back as `{:heard, _}`,
  # and a `:stop` sent to the task ends it at its next idle call.
  defp listen(ns, id, opts \\ []) do
    test = self()

    task =
      Task.async(fn ->
        Listener.listen(ns, id, [window_ms: 240_000, idle_ms: 100], fn
          :idle ->
            receive do
              :stop -> :done
            after
              0 -> :continue
            end

          event ->
            send(test, {:heard, event})
            :continue
        end)
      end)

    for target <- Keyword.get(opts, :on, [id]) do
      assert wait_until(fn -> listeners(ns, target) != [] end, 30_000),
             "no listener was attached to #{target}"
    end

    %{task: task}
  end

  # The listener ends, and neither the rows nor the convergence loop keep it.
  defp stop_listening(%{task: task}, ns, targets) do
    send(task.pid, :stop)
    assert Task.await(task, 30_000) == {:ok, :done}

    for target <- targets do
      assert wait_until(fn -> listeners(ns, target) == [] end, 30_000),
             "a listener row was left on #{target}"
    end

    after_ticks(3)
    for target <- targets, do: assert(listeners(ns, target) == [], "#{target} got it back")
  end

  defp bound?(ns, id) do
    match?(
      {:ok, %{runner_id: runner, suspended: false}} when is_binary(runner),
      OperatorManager.get_overwatch_state(ns, id)
    )
  end

  defp rebound?(ns, id, old_runner) do
    match?(
      {:ok, %{runner_id: runner}} when is_binary(runner) and runner != old_runner,
      OperatorManager.get_overwatch_state(ns, id)
    )
  end

  defp owner(group, count, key),
    do:
      Partition.member_id(group, Partition.owner(Partition.ranges(count), KeyGroup.group_of(key)))
end
