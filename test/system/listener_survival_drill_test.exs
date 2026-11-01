defmodule FlezhaTest.System.ListenerSurvivalDrillTest do
  @moduledoc """
  A listener (`Flezha.Service.Listener.listen/4`) across everything that
  happens to the relay it listens to: sleep and wake, a runner's death, an
  Overwatch restart, the end of a `once` run, a rescale, and, under
  `:multi_node`, a move to another controller. Each drill ends the listener
  and asserts that no member carries its plugin row afterwards, and that the
  convergence loop does not start one again.

      mix test --include system test/system/listener_survival_drill_test.exs
      mix test --include system --include multi_node test/system/listener_survival_drill_test.exs
  """

  use FlezhaTest.SystemCase, capability: :runner_lifecycle

  import ExUnit.CaptureIO
  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]
  import FlezhaTest.Helper.ListenerHarness
  import FlezhaTest.Helper.TierE2EHarness, only: [cleanup: 3, kill_overwatch: 2, unique: 1]

  import FlezhaTest.Helper.StepsResumeHarness,
    only: [boot_runner: 4, replace_runner: 4, suspended?: 2]

  alias Flezha.Cluster.Directory
  alias Flezha.Relay.{Builder, PartitionGroup}
  alias Flezha.Partition
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Plugin.Home
  alias Flezha.Relay.KeyGroup
  alias Flezha.Service.{Cli, HubManager, Listener, RelayManager, PluginManager}
  alias Flezha.State.TestHelpers
  alias FlezhaTest.Fixtures.RelayFixture
  alias FlezhaTest.Helper.{FleetStateHelper, NodeTestHelper, TierE2EHarness}

  @moduletag reference_runtime: true
  @moduletag transport: :uds
  @moduletag timeout: 300_000

  # The convergence loop the drills prove quiet runs on every node, ticked on
  # demand; the test config leaves it off.
  @loop_env [convergence_health_enabled: true, convergence_tick_ms: 600_000]

  # The drills across controllers run in the `:multi_node` tier, not in every
  # `--include system` pass.
  @multi_node_skip FlezhaTest.Tiers.skip_unless_included(:multi_node)

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
    path = sink_path("drill_sink", ns)
    id = start!(ns, "sleeping_counter", :flow, [file_sink("drill_sink", path)])

    send!(ns, id, "a:2")
    assert wait_until(fn -> sink_lines(path) == ["a=2"] end, 30_000)
    assert wait_until(fn -> suspended?(ns, id) end, 30_000)

    # The restart finds it asleep and brings nothing of its table up.
    kill_overwatch(ns, id)
    assert wait_until(fn -> suspended?(ns, id) end, 30_000)

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
  test "a listener hears the outputs after its runner dies and the relay is rebound",
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

    TierE2EHarness.force_fresh_rebind(ns, id)
    assert wait_until(fn -> bound?(ns, id) end, 60_000)

    send!(ns, id, "1")
    assert_receive {:heard, {:output, "done:1", _}}, 60_000

    stop_listening(listener, ns, [id])
  end

  @tag capability: :steps, skip: FlezhaTest.SystemCase.deferral(:steps)
  test "a listener on a once steps relay ends when the relay completes",
       %{namespace: ns} do
    # The end is read from the registry, twice, this far apart.
    on_exit(TestHelpers.override_env(:listener_release_check_ms, 200))
    id = start!(ns, "echo_steps", :steps)
    %{task: task} = listen(ns, id)
    send!(ns, id, "hi")

    assert_receive {:heard, {:output, "echo:hi", _}}, 30_000
    assert Task.await(task, 60_000) == {:ok, :relay_ended}
  end

  @tag capability: :partitioned, skip: FlezhaTest.SystemCase.deferral(:partitioned)
  test "a group listener across a rescale hears every member and leaves none behind",
       %{namespace: ns} do
    rescale_drill(ns)
  end

  describe "across controllers" do
    @describetag :multi_node
    @describetag capability: :parts,
                 skip: @multi_node_skip || FlezhaTest.SystemCase.deferral(:parts)

    # This node (A) and a peer (B), a runner on each. A relay of
    # `part_echo` placed on A moves to B once A's runner stops advertising it,
    # with both controllers up.
    setup %{namespace: ns, runner: local_runner} do
      # A runner that stops serving a factory keeps it placeable this long;
      # every move here waits it out.
      on_exit(TestHelpers.override_env(:group_withdraw_grace_ms, 500))
      {peer_pid, peer} = NodeTestHelper.start_peer_node(:listener_drill_b, app_env: @loop_env)
      on_exit(fn -> NodeTestHelper.stop_peer_node(peer_pid, peer) end)
      NodeTestHelper.start_runner_remote(peer, ns)
      assert wait_until(fn -> peer in HubManager.get_hub_nodes(ns, false) end, 30_000)
      id = start_on!(ns, "part_echo", node())
      %{peer: peer, id: id, local_runner: local_runner}
    end

    test "a listener attached through A survives the relay's move to B and leaves no row",
         %{namespace: ns, peer: peer, id: id} = ctx do
      listener = listen(ns, id)
      move_off_this_node(ctx)
      assert wait_until(fn -> hosted_on?(ns, id, peer) and bound?(ns, id) end, 90_000)

      send!(ns, id, "2")
      assert_receive {:heard, {:output, "done:2", _}}, 60_000
      stop_listening(listener, ns, [id])
    end

    test "a killed listener on a moved relay is detached by its heartbeat",
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

    @tag capability: :partitioned,
         skip: @multi_node_skip || FlezhaTest.SystemCase.deferral(:partitioned)
    test "a group listener across a rescale over two controllers leaves no member a row",
         %{namespace: ns} do
      rescale_drill(ns)
    end

    # A listener's plugin home is placed by its generated name, so over a few
    # rounds some land on B, whose row A learns of only after the enable.
    test "listeners on A attach to a relay B hosts in every direction and leave nothing",
         %{namespace: ns, peer: peer} do
      id = start_on!(ns, "part_echo", peer)

      homes =
        for round <- 1..4, direction <- [:output, :calls, :tally] do
          listener = listen(ns, id, direction: direction)
          [name] = Enum.uniq(listeners(ns, id))
          {_spec, [{home, _pid} | _]} = Home.row(ns, name)
          hear(direction, ns, id, round)
          stop_listening(listener, ns, [id])
          home
        end

      assert peer in homes, "no listener's home was placed on #{peer}"

      for node <- [node(), peer] do
        assert for(
                 %{id: "io_listener_" <> _ = name} <-
                   :erpc.call(node, HubManager, :plugin_entries, [ns]),
                 do: name
               ) == [],
               "a listener's plugin was left enabled on #{node}"
      end
    end

    # A namespace only B's runner serves has its hub on B alone: A reads
    # nothing of it locally and asks B.
    test "listeners on A attach in every direction to a namespace whose hub only B hosts",
         %{peer: peer} do
      ns = unique("listener_drill_far")
      NodeTestHelper.start_runner_remote(peer, ns)
      assert wait_until(fn -> Directory.hub_nodes(ns) == [peer] end, 30_000)
      :erpc.call(peer, NodeTestHelper, :await_serving, [peer, ns, "part_echo"])
      id = unique("part_echo")
      builder = %Builder{id: id, factory_id: "part_echo"}
      assert {:ok, _} = :erpc.call(peer, RelayManager, :start_relay, [ns, builder])
      assert wait_until(fn -> bound?(ns, id) end, 30_000)

      for direction <- [:output, :calls, :tally] do
        listener = listen(ns, id, direction: direction)
        hear(direction, ns, id, 1)
        stop_listening(listener, ns, [id])
      end
    end

    test "a listener on A hears a relay hosted by B after B leaves the ring",
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

    test "a file sink on a relay moved to B is disabled through a controller that never held it",
         %{namespace: ns, peer: peer, id: id} = ctx do
      {c_pid, c_node} = NodeTestHelper.start_peer_node(:listener_drill_c, app_env: @loop_env)
      on_exit(fn -> NodeTestHelper.stop_peer_node(c_pid, c_node) end)

      sink = %{
        file_sink("moved_sink", sink_path("moved_sink", ns))
        | namespace: ns,
          relay_id: id
      }

      assert {:ok, _} = PluginManager.enable_plugin("moved_sink_c", sink)
      move_off_this_node(ctx)
      assert wait_until(fn -> hosted_on?(ns, id, peer) and bound?(ns, id) end, 90_000)
      assert wait_until(fn -> running_plugins(ns, id) != [] end, 30_000)

      assert {:ok, "plugin disabled"} =
               :erpc.call(c_node, PluginManager, :disable_plugin, [ns, "moved_sink_c"], 30_000)

      assert wait_until(fn -> running_plugins(ns, id) == [] end, 30_000)
      after_ticks(3)
      assert running_plugins(ns, id) == []
    end

    test "a file sink enabled through A on a relay moved to B is disabled through A",
         %{namespace: ns, peer: peer, id: id} = ctx do
      sink = %{
        file_sink("moved_sink", sink_path("moved_sink", ns))
        | namespace: ns,
          relay_id: id
      }

      assert {:ok, _} = PluginManager.enable_plugin("moved_sink", sink)
      move_off_this_node(ctx)
      assert wait_until(fn -> hosted_on?(ns, id, peer) and bound?(ns, id) end, 90_000)
      assert wait_until(fn -> running_plugins(ns, id) != [] end, 30_000)

      assert {:ok, "plugin disabled"} = PluginManager.disable_plugin(ns, "moved_sink")
      assert wait_until(fn -> running_plugins(ns, id) == [] end, 30_000)
      after_ticks(3)
      assert running_plugins(ns, id) == []
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
    NodeTestHelper.await_serving(node(), ns, "partition_pass")

    assert {:ok, %{members: members}} =
             PartitionGroup.start(ns, %Builder{id: group, factory_id: "partition_pass"}, 2)

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

  # A disable stops every running copy and leaves the binding in place.
  defp running_plugins(ns, id) do
    case RelayManager.get_overwatch_state(ns, id) do
      {:ok, %{extensions: table}} -> Table.running_ids(table)
      _ -> []
    end
  end

  defp start_on!(ns, factory, node) do
    id = RelayFixture.start_on(node, unique(factory), factory, namespace: ns, attempts: 30)
    assert wait_until(fn -> bound?(ns, id) end, 30_000)
    id
  end

  # This node's runner stops advertising `part_echo`, so its relays move.
  defp move_off_this_node(%{namespace: ns, local_runner: runner}),
    do: FleetStateHelper.sync(ns, runner, ["do_nothing", "sleeping_counter"])

  defp hosted_on?(ns, id, node) do
    match?({:ok, pid} when node(pid) == node, RelayManager.get_relay_pid(ns, id))
  end

  defp start!(ns, factory, kind, plugins \\ []) do
    id = unique(factory)
    bindings = RelayFixture.bindings(ns, id, plugins)
    builder = %Builder{id: id, factory_id: factory, run: kind, opts: [bindings: bindings]}
    assert {:ok, _} = RelayManager.start_relay(ns, builder)
    id
  end

  defp send!(ns, id, text) do
    assert capture_io(fn -> Cli.call("Relay.send", Jason.encode!([ns, id, text, ""]), []) end) =~
             "'#{id}'"
  end

  # A listener in a task of its own: every event comes back as `{:heard, _}`,
  # and a `:stop` sent to the task ends it at its next idle call.
  defp listen(ns, id, opts \\ []) do
    test = self()

    task =
      Task.async(fn ->
        Listener.listen(
          ns,
          id,
          [
            window_ms: 240_000,
            idle_ms: 100,
            before: fn ->
              send(test, {:attached, self()})
              :ok
            end
          ] ++ Keyword.take(opts, [:direction]),
          fn
            :idle ->
              receive do
                :stop -> :done
              after
                0 -> :continue
              end

            event ->
              send(test, {:heard, event})
              :continue
          end
        )
      end)

    # The binding row is written before the relay subscribes, so only the
    # attach returning says a frame sent now is heard.
    listener = task.pid
    assert_receive {:attached, ^listener}, 30_000, "no listener was attached to #{id}"

    for target <- Keyword.get(opts, :on, [id]) do
      assert wait_until(fn -> listeners(ns, target) != [] end, 30_000),
             "no listener was attached to #{target}"
    end

    %{task: task}
  end

  # What a listener of `direction` hears once `round` reaches the relay.
  defp hear(:output, ns, id, round) do
    send!(ns, id, "#{round}")
    done = "done:#{round}"
    assert_receive {:heard, {:output, ^done, _}}, 60_000
  end

  defp hear(:calls, ns, id, round) do
    request = "#{round}"
    done = "done:#{round}"
    assert {:ok, ^done, _} = RelayManager.call_relay(ns, id, request, %{}, 30_000, nil, "cli")
    assert_receive {:heard, {:call, ^request, _}}, 30_000
    assert_receive {:heard, {:answered, ^done, _}}, 30_000
  end

  defp hear(:tally, ns, id, round) do
    send!(ns, id, "#{round}")
    assert_receive {:heard, {:tally, %{"answer" => counted}, _}} when counted > 0, 30_000
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
      {:ok, %{runner_id: runner, phase: phase}} when is_binary(runner) and phase != :suspended,
      RelayManager.get_overwatch_state(ns, id)
    )
  end

  defp rebound?(ns, id, old_runner) do
    match?(
      {:ok, %{runner_id: runner}} when is_binary(runner) and runner != old_runner,
      RelayManager.get_overwatch_state(ns, id)
    )
  end

  defp owner(group, count, key),
    do:
      Partition.member_id(group, Partition.owner(Partition.ranges(count), KeyGroup.group_of(key)))
end
