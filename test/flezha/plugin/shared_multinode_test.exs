defmodule FlezhaTest.Plugin.SharedMultinodeTest do
  @moduledoc """
  A shareable plugin across controllers: one copy on each controller its
  relays run on, a connection kept open across a relay's move, a
  `single` plugin's one copy serving relays on every controller and moving
  with its controller, and the verbs taking effect from a controller that never
  held the plugin.
  """

  use FlezhaTest.Case, fixtures: [:env], async: false

  alias Flezha.Cluster.Directory
  alias Flezha.Identificator
  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Plugin.Provider.BeamMsgForward
  alias Flezha.Plugin.Provider.Websocket
  alias Flezha.Plugin.Shared
  alias Flezha.Service.HubManager
  alias Flezha.Service.RelayManager
  alias Flezha.Service.PluginManager
  alias Flezha.State.TestHelpers
  alias FlezhaTest.Fixtures.RelayFixture
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.WebsocketServer

  import NodeTestHelper, only: [with_only_logged: 2]
  import FlezhaTest.Helper.MessageTestHelper, only: [collect: 1]

  @moduletag :multi_node
  @namespace "default"
  @factory "store_input"

  # A test tagged `:loop` runs the convergence loop on every controller.
  @loop_env [convergence_health_enabled: true, convergence_tick_ms: 1_000]

  setup context do
    TestHelpers.reset_all_cells()
    # Started before the teardown is registered, so it closes after it: no copy
    # is left to dial a closed server.
    server = WebsocketServer.start()
    app_env = if context[:loop], do: @loop_env, else: []
    {pid1, p1} = NodeTestHelper.start_peer_node(:share_p1, app_env: app_env)
    {pid2, p2} = NodeTestHelper.start_peer_node(:share_p2, app_env: app_env)

    on_exit(fn ->
      NodeTestHelper.stop_peers_and_namespace([{pid1, p1}, {pid2, p2}], @namespace)
    end)

    for peer <- [p1, p2] do
      # A controller that runs the loop reclaims a hub no runner serves.
      if context[:loop], do: NodeTestHelper.start_runner_remote(peer, @namespace)

      assert NodeTestHelper.wait_until(fn -> peer in hub_nodes() end, 30_000),
             "#{peer} never joined the hub cluster"
    end

    %{p1: p1, p2: p2, peers: %{p1 => pid1, p2 => pid2}, server: server}
  end

  defp hub_nodes, do: HubManager.get_hub_nodes(@namespace, false)

  # A relay the hub placed on `node`, whose every ingested record reaches
  # the test as `{:ingested, id, record}`.
  defp relay_on(node, prefix) do
    id = RelayFixture.start_on(node, prefix, @factory, attempts: 30)

    test = self()
    collector = spawn_link(fn -> collect(test, id) end)

    {:ok, _} =
      PluginManager.enable_plugin("in_#{id}", %Plugin{
        id: "in_#{id}",
        provider: BeamMsgForward,
        namespace: @namespace,
        relay_id: id,
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

  defp websocket(name, server, single \\ false) do
    {:ok, _} =
      PluginManager.enable_plugin(name, %Plugin{
        id: name,
        provider: Websocket,
        namespace: @namespace,
        single: single,
        opts: %{uri: "ws://127.0.0.1:#{server.port}/feed", channel: "input"}
      })
  end

  defp copies_on(name), do: Enum.sort(Directory.plugin_nodes(@namespace, name))

  # Whether `op` takes input from `name` again. A relay whose runner went
  # with a stopped controller is parked until the loop binds it to another,
  # and a record that reaches it before then is not taken.
  defp serving?(op, name) do
    case RelayManager.get_overwatch_state(@namespace, op) do
      {:ok, %{halt_reason: nil, extensions: table}} ->
        Table.running?(table, name) and Table.running?(table, "in_" <> op)

      _ ->
        false
    end
  end

  defp home_node(name) do
    {_spec, [{node, _pid} | _]} =
      ProcessHub.child_lookup(Identificator.hub(@namespace), {:plugin, name})

    node
  end

  # Drain `peer` the way `flz controller stop` does: its children move away
  # first. `leave/2` then takes the controller out.
  defp drain(peer) do
    assert :ok =
             :erpc.call(
               peer,
               Flezha.Shutdown.Coordinator,
               :shutdown,
               [:cli, [timeout_ms: 30_000]],
               45_000
             )
  end

  defp leave(peers, peer) do
    drain(peer)
    NodeTestHelper.stop_peer_node(Map.fetch!(peers, peer), peer)
  end

  # Every connection event the server reported, in the order it reported them.
  defp connection_events() do
    collect do
      {kind, socket} when kind in [:ws_open, :ws_closed] -> {kind, socket}
    end
  end

  # The connection events up to the moment `first` closes: the one that
  # replaced it must be among them.
  defp opened_before_closing(first, acc \\ []) do
    receive do
      {:ws_closed, ^first} ->
        Enum.any?(acc, &match?({:ws_open, other} when other != first, &1))

      {kind, socket} when kind in [:ws_open, :ws_closed] ->
        opened_before_closing(first, [{kind, socket} | acc])
    after
      20_000 -> flunk("the first connection never closed")
    end
  end

  test "a copy on each controller a relay runs on, and the verbs work from any",
       %{p1: p1, p2: p2, server: server} do
    a = relay_on(p1, "pc_a")
    b = relay_on(p2, "pc_b")
    websocket("trades", server)

    :ok = PluginManager.bind_plugin(@namespace, "trades", a)
    :ok = PluginManager.bind_plugin(@namespace, "trades", b)

    assert NodeTestHelper.wait_until(fn -> copies_on("trades") == Enum.sort([p1, p2]) end, 10_000)
    assert NodeTestHelper.wait_until(fn -> WebsocketServer.open(server) == 2 end, 10_000)

    WebsocketServer.broadcast(server, "both")
    assert_receive {:ingested, ^a, "both"}, 5_000
    assert_receive {:ingested, ^b, "both"}, 5_000

    # This node holds neither relay nor copy.
    :ok = PluginManager.unbind_plugin(@namespace, "trades", b)
    assert NodeTestHelper.wait_until(fn -> copies_on("trades") == [p1] end, 5_000)
    assert NodeTestHelper.wait_until(fn -> WebsocketServer.open(server) == 1 end, 5_000)

    assert {:ok, _} = PluginManager.disable_plugin(@namespace, "trades")
    assert NodeTestHelper.wait_until(fn -> copies_on("trades") == [] end, 5_000)
    assert NodeTestHelper.wait_until(fn -> WebsocketServer.open(server) == 0 end, 5_000)
  end

  test "a relay that moves keeps the connection open across the move",
       %{p1: p1, server: server} = ctx do
    a = relay_on(p1, "mv_a")
    websocket("moving", server)
    :ok = PluginManager.bind_plugin(@namespace, "moving", a)
    assert_receive {:ws_open, first}, 10_000

    # The relay moves first: the copy it left keeps its connection through
    # the idle stop, so both are open together before its controller is gone.
    drain(p1)
    assert NodeTestHelper.wait_until(fn -> WebsocketServer.open(server) == 2 end, 15_000)
    NodeTestHelper.stop_peer_node(Map.fetch!(ctx.peers, p1), p1)
    assert opened_before_closing(first), "the new connection did not open before the old closed"
    assert NodeTestHelper.wait_until(fn -> serving?(a, "moving") end, 30_000)

    WebsocketServer.broadcast(server, "after-move")
    assert_receive {:ingested, ^a, "after-move"}, 10_000
  end

  test "a single plugin serves relays on every controller with one connection, and moves",
       %{p1: p1, p2: p2, server: server} = ctx do
    # The hub places the home by its name: take one it placed on a peer, so
    # that peer can be drained.
    name =
      Enum.find_value(1..30, fn i ->
        name = "one_#{i}"
        websocket(name, server, true)
        assert NodeTestHelper.wait_until(fn -> WebsocketServer.open(server) == 1 end, 10_000)

        if home_node(name) == p1 do
          name
        else
          PluginManager.disable_plugin(@namespace, name)
          assert NodeTestHelper.wait_until(fn -> WebsocketServer.open(server) == 0 end, 10_000)
          nil
        end
      end) || flunk("no single home landed on #{inspect(p1)}")

    assert NodeTestHelper.wait_until(fn -> WebsocketServer.open(server) == 1 end, 10_000)
    [first] = Agent.get(server.agent, & &1.open)
    _ = connection_events()

    ops = [relay_on(p1, "s_a"), relay_on(p2, "s_b"), relay_on(node(), "s_c")]
    Enum.each(ops, &(:ok = PluginManager.bind_plugin(@namespace, name, &1)))

    WebsocketServer.broadcast(server, "one-frame")
    for op <- ops, do: assert_receive({:ingested, ^op, "one-frame"}, 5_000)
    refute_receive {:ingested, _, "one-frame"}, 300
    assert WebsocketServer.open(server) == 1

    leave(ctx.peers, p1)
    assert opened_before_closing(first), "the new connection did not open before the old closed"
    assert NodeTestHelper.wait_until(fn -> home_node(name) != p1 end, 10_000)

    # Every relay, the one that moved with p1 too, is subscribed to the new
    # copy before the next frame goes.
    assert NodeTestHelper.wait_until(
             fn ->
               match?([copy] when is_pid(copy), Directory.plugin_hosts(@namespace, name)) and
                 length(Shared.subscribers(hd(Directory.plugin_hosts(@namespace, name)))) == 3
             end,
             15_000
           )

    WebsocketServer.broadcast(server, "after-drain")
    for op <- ops, do: assert_receive({:ingested, ^op, "after-drain"}, 5_000)
  end

  # The convergence loop restarts a binding whose copy went with its controller.
  @tag :loop
  test "a single plugin whose controller dies comes up elsewhere and serves again",
       %{p1: p1, p2: p2, server: server} = ctx do
    {:ok, _} = start_supervised({Flezha.Convergence.Loop, tick_ms: 1_000})

    name =
      Enum.find_value(1..30, fn i ->
        name = "lost_#{i}"
        websocket(name, server, true)
        assert NodeTestHelper.wait_until(fn -> WebsocketServer.open(server) == 1 end, 10_000)

        if home_node(name) == p1 do
          name
        else
          PluginManager.disable_plugin(@namespace, name)
          assert NodeTestHelper.wait_until(fn -> WebsocketServer.open(server) == 0 end, 10_000)
          nil
        end
      end) || flunk("no single home landed on #{inspect(p1)}")

    ops = [relay_on(p2, "l_b"), relay_on(node(), "l_c")]
    Enum.each(ops, &(:ok = PluginManager.bind_plugin(@namespace, name, &1)))

    # Stopping the controller may stop the copy's worker before the copy, which
    # then reports it ended.
    with_only_logged([{~r/subscriber #{name} crashed: :shutdown/, :maybe}], fn ->
      NodeTestHelper.stop_peer_node(Map.fetch!(ctx.peers, p1), p1)

      assert NodeTestHelper.wait_until(
               fn ->
                 match?([_], Directory.plugin_hosts(@namespace, name)) and
                   length(Shared.subscribers(hd(Directory.plugin_hosts(@namespace, name)))) ==
                     2 and
                   Enum.all?(ops, &serving?(&1, name))
               end,
               30_000
             ),
             "the relays were not served again without a new bind"
    end)

    WebsocketServer.broadcast(server, "after-loss")
    for op <- ops, do: assert_receive({:ingested, ^op, "after-loss"}, 15_000)
  end
end
