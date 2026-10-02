defmodule FlezhaTest.Plugin.SharedTest do
  @moduledoc """
  A shareable plugin bound to several operators on one controller runs once:
  one upstream connection, every record to every operator, a slow operator
  falling behind alone and losing nothing, a stop when its last operator leaves, and a live option
  changed on the open connection.
  """

  use FlezhaTest.Case, fixtures: [:env], async: false

  import ExUnit.CaptureLog

  alias Flezha.Cluster.Directory
  alias Flezha.Operator.Builder
  alias Flezha.Operator.PartitionGroup
  alias Flezha.Plugin
  alias Flezha.Plugin.Host
  alias Flezha.Plugin.Provider.BeamMsgForward
  alias Flezha.Plugin.Provider.HttpPoll
  alias Flezha.Plugin.Provider.Kafka
  alias Flezha.Plugin.Provider.Websocket
  alias Flezha.Plugin.Provider.WorkerSup
  alias Flezha.Plugin.Shared
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Plugin.ProviderBehaviour.Options, as: ProviderOptions
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.HubManager
  alias Flezha.Service.OperatorManager
  alias Flezha.Service.PluginManager
  alias Flezha.State.TestHelpers
  alias FlezhaTest.Fixtures.OperatorFixture
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.WebsocketServer

  @namespace "shared_plugin_ns"
  @factory "store_input"

  # One runner for the module; each test's operators and plugins go with it.
  setup_all do
    TestHelpers.reset_all_cells()
    runner_id = NodeTestHelper.start_runner(@namespace)

    on_exit(fn ->
      NodeTestHelper.stop_runner(@namespace, runner_id)
      FleetManager.stop_namespace(@namespace)
    end)

    :ok
  end

  # The server is started first so the test's operators and plugins are gone
  # before it closes.
  setup do
    server = WebsocketServer.start()

    on_exit(fn ->
      OperatorManager.stop_all_operators(@namespace)
      Enum.each(["trades", "poll", "solo"], &PluginManager.disable_plugin(@namespace, &1))
    end)

    %{server: server}
  end

  # An operator whose every ingested record reaches the test as `{id, record}`.
  defp operator(id) do
    {:ok, _} = id |> OperatorFixture.get(@factory) |> OperatorFixture.start(@namespace)
    test = self()

    collector = spawn_link(fn -> collect(test, id) end)

    {:ok, _} =
      PluginManager.enable_plugin("in_#{id}", %Plugin{
        id: "in_#{id}",
        provider: BeamMsgForward,
        namespace: @namespace,
        operator_id: id,
        opts: %{recv_pid: collector},
        hooks: [{:on_input, []}]
      })

    id
  end

  defp collect(test, id) do
    receive do
      {:message_forward, message} -> send(test, {:ingested, id, message})
    end

    collect(test, id)
  end

  defp trades(server, opts \\ %{}, name \\ "trades", single \\ false) do
    {:ok, _} =
      PluginManager.enable_plugin(name, %Plugin{
        id: name,
        provider: Websocket,
        namespace: @namespace,
        single: single,
        opts: Map.merge(%{uri: "ws://127.0.0.1:#{server.port}/feed", channel: "input"}, opts)
      })
  end

  defp ingested(id) do
    receive do
      {:ingested, ^id, frame} -> frame
    after
      5_000 -> flunk("#{id} ingested nothing")
    end
  end

  defp copies, do: Directory.plugin_hosts(@namespace, "trades")

  defp table(operator_id) do
    {:ok, %{extensions: table}} = OperatorManager.get_overwatch_state(@namespace, operator_id)
    table
  end

  test "two operators on one controller share one connection and each ingests a frame once",
       %{server: server} do
    a = operator("share_a")
    b = operator("share_b")
    trades(server)

    :ok = PluginManager.bind_plugin(@namespace, "trades", a)
    :ok = PluginManager.bind_plugin(@namespace, "trades", b)
    assert_receive {:ws_open, _socket}, 5_000

    WebsocketServer.broadcast(server, "frame-1")

    assert_receive {:ingested, ^a, "frame-1"}, 5_000
    assert_receive {:ingested, ^b, "frame-1"}, 5_000
    refute_receive {:ingested, _, "frame-1"}, 200
    assert WebsocketServer.connections(server) == 1
    assert [_one] = copies()
  end

  test "the copy stops at once when its last operator is stopped", %{server: server} do
    a = operator("stop_a")
    b = operator("stop_b")
    trades(server)
    :ok = PluginManager.bind_plugin(@namespace, "trades", a)
    :ok = PluginManager.bind_plugin(@namespace, "trades", b)
    assert_receive {:ws_open, socket}, 5_000

    :ok = OperatorManager.stop_operator(@namespace, a)
    refute_receive {:ws_closed, ^socket}, 300

    :ok = OperatorManager.stop_operator(@namespace, b)
    assert_receive {:ws_closed, ^socket}, 2_000
    assert copies() == []

    assert %{controllers: []} =
             Enum.find(PluginManager.plugins(@namespace), &(&1.name == "trades"))
  end

  test "a killed Overwatch back inside the idle stop keeps the one connection",
       %{server: server} do
    Application.put_env(:flezha, :plugin_idle_stop_ms, 5_000)
    a = operator("kill_a")
    trades(server)
    :ok = PluginManager.bind_plugin(@namespace, "trades", a)
    assert_receive {:ws_open, socket}, 5_000
    [copy] = copies()

    {:ok, ow} = HubManager.get_pid(@namespace, a)

    log =
      capture_log(fn ->
        Process.exit(ow, :kill)

        assert NodeTestHelper.wait_until(
                 fn ->
                   match?({:ok, pid} when pid != ow, HubManager.get_pid(@namespace, a)) and
                     Table.running?(table(a), "trades")
                 end,
                 5_000
               )
      end)

    assert log =~ ~s(Child "kill_a")

    refute_received {:ws_closed, ^socket}
    assert copies() == [copy]
    assert WebsocketServer.connections(server) == 1
  end

  test "an idle plugin starts again for an operator bound to it", %{server: server} do
    a = operator("idle_a")
    trades(server)
    :ok = PluginManager.bind_plugin(@namespace, "trades", a)
    assert_receive {:ws_open, _}, 5_000
    :ok = PluginManager.unbind_plugin(@namespace, "trades", a)
    assert_receive {:ws_closed, _}, 2_000
    assert copies() == []

    b = operator("idle_b")
    :ok = PluginManager.bind_plugin(@namespace, "trades", b)
    assert_receive {:ws_open, _}, 5_000
    assert [_one] = copies()
  end

  test "a stalled operator falls behind alone and loses nothing", %{server: server} do
    a = operator("lag_a")
    b = operator("lag_b")
    trades(server)
    :ok = PluginManager.bind_plugin(@namespace, "trades", a)
    :ok = PluginManager.bind_plugin(@namespace, "trades", b)
    assert_receive {:ws_open, _}, 5_000
    {:ok, ow_a} = HubManager.get_pid(@namespace, a)

    # Each frame goes once `b` took the one before: `b` keeps up, `a` cannot.
    :sys.suspend(ow_a)

    Enum.each(1..100, fn n ->
      frame = "f#{n}"
      WebsocketServer.broadcast(server, frame)
      assert_receive {:ingested, ^b, ^frame}, 5_000
    end)

    :sys.resume(ow_a)
    assert Enum.map(1..100, fn _ -> ingested(a) end) == Enum.map(1..100, &"f#{&1}")
  end

  test "a live option changes the subscription on the same connection", %{server: server} do
    a = operator("live_a")
    b = operator("live_b")

    trades(server, %{
      subscription_items: ["a", "b"],
      subscribe_template: ~s({"op":"sub","items":{{items}},"id":{{req_id}}}),
      unsubscribe_template: ~s({"op":"unsub","items":{{items}},"id":{{req_id}}}),
      subscription_ack_mode: "none"
    })

    :ok = PluginManager.bind_plugin(@namespace, "trades", a)
    :ok = PluginManager.bind_plugin(@namespace, "trades", b)
    assert_receive {:ws_frame, ~s({"op":"sub","items":["a","b"],"id":1})}, 5_000
    socket_process = WorkerSup.worker_pid(Websocket, "trades")

    assert {:ok, %{applied: :live}} =
             PluginManager.update_plugin(@namespace, "trades", %{subscription_items: ["a", "c"]})

    assert_receive {:ws_frame, ~s({"op":"sub","items":["c"],"id":2})}, 5_000
    assert_receive {:ws_frame, ~s({"op":"unsub","items":["b"],"id":3})}, 5_000
    assert WorkerSup.worker_pid(Websocket, "trades") == socket_process

    WebsocketServer.broadcast(server, "after-update")
    assert_receive {:ingested, ^a, "after-update"}, 5_000
    assert_receive {:ingested, ^b, "after-update"}, 5_000
  end

  test "any other option restarts the copy and names the bound operators", %{server: server} do
    other = WebsocketServer.start()
    on_exit(fn -> PluginManager.disable_plugin(@namespace, "trades") end)
    a = operator("restart_a")
    b = operator("restart_b")
    trades(server)
    :ok = PluginManager.bind_plugin(@namespace, "trades", a)
    :ok = PluginManager.bind_plugin(@namespace, "trades", b)
    assert_receive {:ws_open, old}, 5_000

    assert {:ok, %{applied: :restarted, operators: operators}} =
             PluginManager.update_plugin(@namespace, "trades", %{
               uri: "ws://127.0.0.1:#{other.port}/feed"
             })

    assert Enum.sort(operators) == Enum.sort([a, b])
    assert_receive {:ws_closed, ^old}, 5_000
    assert_receive {:ws_open, _new}, 5_000
    assert WebsocketServer.connections(other) == 1
  end

  test "disable stops every copy, the bindings stay and come up when it is enabled again",
       %{server: server} do
    a = operator("off_a")
    b = operator("off_b")
    trades(server)
    :ok = PluginManager.bind_plugin(@namespace, "trades", a)
    :ok = PluginManager.bind_plugin(@namespace, "trades", b)
    assert_receive {:ws_open, socket}, 5_000

    assert {:ok, _} = PluginManager.disable_plugin(@namespace, "trades")

    assert_receive {:ws_closed, ^socket}, 2_000
    assert copies() == []

    for op <- [a, b] do
      assert Table.phase(table(op), "trades") == {:not_started, {:plugin_not_found, "trades"}}
      assert {:ok, [_ | _]} = HubManager.operator_bindings(@namespace, op)
    end

    trades(server)

    assert_receive {:ws_open, _}, 5_000
    assert Table.running?(table(a), "trades") and Table.running?(table(b), "trades")
    WebsocketServer.broadcast(server, "back")
    assert_receive {:ingested, ^a, "back"}, 5_000
    assert_receive {:ingested, ^b, "back"}, 5_000
  end

  test "an http_poll plugin bound to two operators runs one poller" do
    a = operator("poll_a")
    b = operator("poll_b")

    {:ok, _} =
      PluginManager.enable_plugin("poll", %Plugin{
        id: "poll",
        provider: HttpPoll,
        namespace: @namespace,
        opts: %{url: "http://127.0.0.1:9/feed", channel: "input", interval_ms: 60_000}
      })

    capture_log(fn ->
      assert :ok = PluginManager.bind_plugin(@namespace, "poll", a)
      assert :ok = PluginManager.bind_plugin(@namespace, "poll", b)
    end)

    assert Table.running?(table(a), "poll") and Table.running?(table(b), "poll")
    assert [_one] = Directory.plugin_hosts(@namespace, "poll")
  end

  test "a Kafka producer's copy and the operators bound to it name one worker" do
    worker_key = fn operator_id ->
      opts = %{
        hosts: "localhost:9092",
        topic: "alerts",
        source_plugin_id: "alerts",
        source_operator_id: operator_id,
        subscribed_extensions: [:on_output]
      }

      {:ok, normalised} = ProviderOptions.normalise_opts(Kafka.Producer, opts)
      {:ok, prepared} = Kafka.Producer.init_provider(normalised)
      [{key, _spec}] = Kafka.Producer.worker_specs(prepared)
      key
    end

    assert worker_key.(nil) == worker_key.("op_a")
  end

  test "an operator bound with an output hook finds the outbound half on the shared connection",
       %{server: server} do
    a = operator("send_a")
    trades(server)
    :ok = PluginManager.bind_plugin(@namespace, "trades", a, hooks: [{:on_output, []}])
    assert_receive {:ws_open, _}, 5_000

    assert {_status, %{inner_state: %{sink: %Websocket.Sink{}}}} =
             :sys.get_state(WorkerSup.worker_pid(Websocket, "trades"))
  end

  test "a single plugin started again after an update runs the updated options",
       %{server: server} do
    other = WebsocketServer.start()
    on_exit(fn -> PluginManager.disable_plugin(@namespace, "solo") end)
    a = operator("solo_a")
    trades(server, %{}, "solo", true)
    assert_receive {:ws_open, first}, 5_000
    :ok = PluginManager.bind_plugin(@namespace, "solo", a)

    assert {:ok, %{applied: :restarted}} =
             PluginManager.update_plugin(@namespace, "solo", %{
               uri: "ws://127.0.0.1:#{other.port}/feed"
             })

    assert_receive {:ws_closed, ^first}, 5_000
    assert_receive {:ws_open, _}, 5_000
    :ok = PluginManager.unbind_plugin(@namespace, "solo", a)
    assert_receive {:ws_closed, _}, 5_000

    :ok = PluginManager.bind_plugin(@namespace, "solo", a)
    assert_receive {:ws_open, _}, 5_000
    assert WebsocketServer.connections(server) == 1
    assert WebsocketServer.connections(other) == 2
    :ok = PluginManager.unbind_plugin(@namespace, "solo", a)
  end

  describe "a retiring single copy" do
    setup %{server: server} do
      Process.flag(:trap_exit, true)

      plugin = %Plugin{
        id: "lone",
        provider: Websocket,
        namespace: @namespace,
        single: true,
        opts: %{uri: "ws://127.0.0.1:#{server.port}/feed", channel: "input"}
      }

      {:ok, copy} = Shared.start_link(%{namespace: @namespace, plugin: Shared.entry(plugin)})
      assert_receive {:ws_open, socket}, 5_000
      :ok = Shared.adopt(copy, [Host.target(spawn_link(&NodeTestHelper.park/0), nil, false)])
      successor = spawn_link(fn -> successor(true) end)
      Directory.join_plugin(successor, @namespace, "lone")
      Shared.retire(copy)

      assert NodeTestHelper.wait_until(
               fn -> Map.has_key?(:sys.get_state(copy).timers, :successor) end,
               5_000
             )

      %{copy: copy, socket: socket, successor: successor}
    end

    test "keeps its connection until its successor's is open", ctx do
      refute_received {:ws_closed, _}
      send(ctx.successor, {:connecting, false})
      assert_receive {:ws_closed, socket}, 5_000
      assert socket == ctx.socket
      assert_receive {:EXIT, copy, {:shutdown, :retired}}, 5_000
      assert copy == ctx.copy
    end

    test "stops after the retire bound when its successor never opens", ctx do
      restore = NodeTestHelper.override_log_level(:warning)

      log =
        capture_log(fn ->
          send(ctx.copy, :retired)
          assert_receive {:EXIT, copy, {:shutdown, :retired}}, 5_000
          assert copy == ctx.copy
        end)

      restore.()
      assert log =~ "lone retired before its successor opened"
    end
  end

  # A copy that replaces a retiring one, answering whether its upstream is open.
  defp successor(connecting) do
    receive do
      {:"$gen_call", from, :connecting?} ->
        GenServer.reply(from, connecting)
        successor(connecting)

      {:connecting, now} ->
        successor(now)
    end
  end

  test "a join that reaches a copy as it stops idle is served by a new copy",
       %{server: server} do
    a = operator("late_a")
    b = operator("late_b")
    trades(server)
    :ok = PluginManager.bind_plugin(@namespace, "trades", a)
    assert_receive {:ws_open, _}, 5_000
    [copy] = copies()
    {:ok, ow_a} = HubManager.get_pid(@namespace, a)
    # Its socket has read its operators from the copy, which it does on connect.
    _ = :sys.get_state(WorkerSup.worker_pid(Websocket, "trades"))

    # The copy takes, in this order: its last operator leaving, its idle stop,
    # and a new operator's join.
    :sys.suspend(copy)
    Shared.leave(copy, ow_a, :idle)
    send(copy, :idle_stop)
    bind = Task.async(fn -> PluginManager.bind_plugin(@namespace, "trades", b) end)

    assert NodeTestHelper.wait_until(
             fn -> Process.info(copy, :message_queue_len) == {:message_queue_len, 3} end,
             5_000
           )

    :sys.resume(copy)

    assert :ok = Task.await(bind, 10_000)
    assert NodeTestHelper.wait_until(fn -> match?([new] when new != copy, copies()) end, 5_000)
    Enum.each([a, b], &(:ok = PluginManager.unbind_plugin(@namespace, "trades", &1)))
    assert_receive {:ws_closed, _}, 5_000
  end

  # Under `ranges(2)`, SOL-USD hashes to key group 60 (p0), BTC-USD to 129 and
  # ETH-USD to 213 (both p1).
  test "a partitioned operator runs a copy per member beside the shared copy",
       %{server: server} do
    a = operator("member_a")
    group = "member_grp"
    on_exit(fn -> OperatorManager.stop_operator(@namespace, group) end)

    {:ok, _} =
      PartitionGroup.start(
        @namespace,
        %Builder{id: group, factory_id: "partition_relay", init_values: %{}, opts: []},
        2
      )

    trades(server, %{
      channel: "in",
      subscription_items: ["SOL-USD", "BTC-USD", "ETH-USD"],
      subscribe_template: ~s({"op":"sub","items":{{items}}}),
      unsubscribe_template: ~s({"op":"unsub","items":{{items}}}),
      subscription_ack_mode: "none"
    })

    :ok = PluginManager.bind_plugin(@namespace, "trades", a)
    :ok = PluginManager.bind_plugin(@namespace, "trades", group)

    subscribed =
      for _ <- 1..3 do
        assert_receive {:ws_frame, frame}, 5_000
        frame |> Jason.decode!() |> Map.fetch!("items") |> Enum.sort()
      end

    assert Enum.sort(subscribed) == [
             ["BTC-USD", "ETH-USD"],
             ["BTC-USD", "ETH-USD", "SOL-USD"],
             ["SOL-USD"]
           ]

    assert WebsocketServer.open(server) == 3
    assert [_shared] = copies()

    for index <- [0, 1] do
      member = "#{group}#p#{index}"
      assert Table.running?(table(member), "trades#p#{index}")
      refute Map.has_key?(Table.workers(table(member), "trades#p#{index}"), :shared)
    end

    :ok = PluginManager.unbind_plugin(@namespace, "trades", group)
    assert_receive {:ws_closed, _}, 5_000
    assert_receive {:ws_closed, _}, 5_000
    refute_receive {:ws_closed, _}, 300
    assert WebsocketServer.open(server) == 1
    assert [_shared] = copies()
  end

  test "an update that changes no option restarts nothing", %{server: server} do
    a = operator("same_a")
    trades(server)
    :ok = PluginManager.bind_plugin(@namespace, "trades", a)
    assert_receive {:ws_open, socket}, 5_000

    assert {:ok, %{applied: :unchanged}} =
             PluginManager.update_plugin(@namespace, "trades", %{channel: "input"})

    refute_receive {:ws_closed, ^socket}, 500
    assert WebsocketServer.connections(server) == 1
  end
end
