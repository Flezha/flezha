defmodule Flezha.Service.RelayGraphTest do
  @moduledoc """
  `relay-graph` on the controller: the factory table keeps the graph each
  runner announces, from the connect frame or an `UpdateFactories`, beside its
  API and out of the factory rows; `Relay.graph` reads one relay's, and
  `flz relay graph` prints it.
  """
  # async: false: Mimic global mode, and the live FleetManager.
  use FlezhaTest.Case, async: false
  use Mimic

  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.AuthZ.RoleStore
  alias Flezha.Cluster.Directory
  alias Flezha.Error
  alias Flezha.Partition
  alias Flezha.Relay.PartitionGroup
  alias Flezha.Runner.ControlChannel.Requests
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.{Cli, HubManager, Relay}
  alias FlezhaTest.Helper.{CliCapture, FleetStateHelper}

  setup :set_mimic_global_until_exit

  @context %{source: :ui, principal: "console@_system"}

  setup do
    original = :sys.get_state(FleetManager)
    namespace = "relay_graph_#{System.unique_integer([:positive])}"
    FleetStateHelper.seed(namespace, %{"r1" => [], "r2" => []})

    on_exit(fn ->
      FleetStateHelper.forget_established(namespace, ["market", "legacy"])
      :sys.replace_state(FleetManager, fn _ -> original end)
    end)

    # `market-eu` is a `market` relay, `demo` a group of two of them, and
    # `legacy-1` a relay of a factory that announced no graph.
    stub(HubManager, :relay_spec, fn
      _namespace, id when id in ["market-eu", "demo#p0", "demo#p1"] ->
        {:ok, %{factory_id: "market", opts: []}, %{runner: nil, node: nil}}

      _namespace, "legacy-1" ->
        {:ok, %{factory_id: "legacy", opts: []}, %{runner: nil, node: nil}}

      namespace, id ->
        {:error, {:relay_not_found, namespace, id}}
    end)

    # The rows are this node's, so it hosts the namespace's hub.
    stub(Directory, :hub_nodes, fn
      ^namespace -> [node()]
      other -> Mimic.call_original(Directory, :hub_nodes, [other])
    end)

    stub(PartitionGroup, :members, fn
      _namespace, "demo" ->
        for i <- 0..1,
            do: %{id: Partition.member_id("demo", i), partition: %{range: nil}}

      _namespace, _id ->
        []
    end)

    %{namespace: namespace}
  end

  # A `market` class as the connect frame carries it: `ohlc` reads the entry
  # and feeds `atr` over a hybrid channel; `atr` calls `pricing`.
  defp graph(tasklets) do
    %{
      "channels" => [
        channel("trades_in", "entry", "", ["ohlc"], "none"),
        channel("ohlc_out", "inter_tasklet", "ohlc", ["atr"], "output", overflow: "drop_newest"),
        channel("ask", "plugin_bound", "atr", [], "call", call_target: "factory:pricing")
      ],
      "tasklets" =>
        for name <- tasklets do
          %{"name" => name, "mode" => "single", "cell_keys" => [], "mailbox_capacity" => 1024}
        end
    }
  end

  defp channel(name, kind, producer, consumers, plugin_end, opts \\ []) do
    %{
      "name" => name,
      "kind" => kind,
      "producer" => producer,
      "consumers" => consumers,
      "plugin_end" => plugin_end,
      "call_target" => opts[:call_target] || "",
      "overflow" => opts[:overflow] || "drop_nothing"
    }
  end

  defp factory(id, graph) do
    %{"id" => id, "sdk_lang" => 1, "run" => "flow"}
    |> then(&if graph, do: Map.put(&1, "graph", graph), else: &1)
  end

  defp tasklet_names(%{graph: %{tasklets: tasklets}}), do: Enum.map(tasklets, & &1.name)

  describe "the factory table" do
    test "keeps the graph the connect frame carries, beside the API and out of the rows",
         %{namespace: ns} do
      FleetStateHelper.announce(ns, "r1", [factory("market", graph(["ohlc", "atr"]))])
      {:ok, rows} = FleetManager.list_factories(ns)

      assert %{graph: graph, disagree: false} = FleetManager.factory_graph(ns, "market")

      assert %{
               name: "ohlc_out",
               kind: "inter_tasklet",
               producer: "ohlc",
               consumers: ["atr"],
               plugin_end: "output",
               call_target: nil,
               overflow: "drop_newest"
             } = Enum.at(graph.channels, 1)

      assert %{producer: nil, consumers: ["ohlc"]} = hd(graph.channels)
      assert %{plugin_end: "call", call_target: "factory:pricing"} = List.last(graph.channels)

      assert [%{name: "ohlc", mode: "single", cell_keys: [], mailbox_capacity: 1024}, _atr] =
               graph.tasklets

      assert [row] = rows
      refute Map.has_key?(row, :graph)
    end

    test "keeps the graph an UpdateFactories carries, in the same shape", %{namespace: ns} do
      FleetStateHelper.announce(ns, "r1", [factory("market", graph(["ohlc", "atr"]))])
      from_json = FleetManager.factory_graph(ns, "market")

      g = graph(["ohlc", "atr"])

      decl = %RunnerCommands.FactoryDecl{
        id: "market",
        sdk_lang: 1,
        run: "flow",
        graph: %RunnerCommands.RelayGraph{
          channels: for(c <- g["channels"], do: struct(RunnerCommands.GraphChannel, atomize(c))),
          tasklets: for(t <- g["tasklets"], do: struct(RunnerCommands.GraphTasklet, atomize(t)))
        }
      }

      Requests.update_factories(
        %{namespace: ns, runner_id: "r1"},
        %RunnerCommands.UpdateFactories{factories: [decl]}
      )

      {:ok, _rows} = FleetManager.list_factories(ns)
      assert FleetManager.factory_graph(ns, "market") == from_json
    end

    test "a reconnect replaces the graph", %{namespace: ns} do
      FleetStateHelper.announce(ns, "r1", [factory("market", graph(["ohlc", "atr"]))])
      FleetStateHelper.announce(ns, "r1", [factory("market", graph(["ohlc", "atr", "trades"]))])

      assert tasklet_names(FleetManager.factory_graph(ns, "market")) == ["ohlc", "atr", "trades"]
      refute FleetManager.factory_graph(ns, "market").disagree
    end

    test "runners announcing different graphs disagree, and the latest is served",
         %{namespace: ns} do
      FleetStateHelper.announce(ns, "r1", [factory("market", graph(["ohlc", "atr"]))])
      FleetStateHelper.announce(ns, "r2", [factory("market", graph(["ohlc", "atr", "trades"]))])

      read = FleetManager.factory_graph(ns, "market")
      assert read.disagree
      assert tasklet_names(read) == ["ohlc", "atr", "trades"]
    end

    test "a factory loses its graph when its last runner leaves", %{namespace: ns} do
      FleetStateHelper.announce(ns, "r1", [factory("market", graph(["ohlc", "atr"]))])
      FleetStateHelper.disconnected(ns, "r1")

      assert FleetManager.factory_graph(ns, "market") == nil
    end
  end

  describe "Relay.graph" do
    test "a member and its group both read their factory's graph", %{namespace: ns} do
      FleetStateHelper.announce(ns, "r1", [factory("market", graph(["ohlc", "atr"]))])

      for relay <- ["demo#p0", "demo", "market-eu"] do
        assert {:ok, %{relay: ^relay, factory: "market", disagree: false, reason: nil} = read} =
                 Relay.graph(ns, relay, @context)

        assert tasklet_names(read) == ["ohlc", "atr"]
      end
    end

    test "a factory that announced none answers no graph, not an error", %{namespace: ns} do
      FleetStateHelper.announce(ns, "r1", [factory("legacy", nil)])

      assert {:ok, %{relay: "legacy-1", factory: "legacy", graph: nil, reason: "not_announced"}} =
               Relay.graph(ns, "legacy-1", @context)

      assert FleetManager.runners_for_factory(ns, "legacy") == ["r1"]
    end

    test "a mixed fleet whose latest runner announced none still says it disagrees",
         %{namespace: ns} do
      FleetStateHelper.announce(ns, "r1", [factory("market", graph(["ohlc", "atr"]))])
      FleetStateHelper.announce(ns, "r2", [factory("market", nil)])

      assert {:ok, %{graph: nil, disagree: true, reason: "not_announced"}} =
               Relay.graph(ns, "market-eu", @context)
    end

    test "a node that could not be asked is an error, not an unannounced graph",
         %{namespace: ns} do
      stub(Flezha.Service.RelayManager, :factory_graph, fn _ns, _factory, _host ->
        {:error, {:routed_call_failed, :"peer@127.0.0.1", :exit, :timeout}}
      end)

      assert {:error, %Error{code: "controller.peer_unreachable"}} =
               Relay.graph(ns, "market-eu", @context)
    end

    test "a malformed graph stores only what reads as text, and still prints",
         %{namespace: ns} do
      bad = %{
        "channels" => [
          %{"name" => 7},
          channel("in", "entry", "", "x", "none") |> Map.put("overflow", 3)
        ],
        "tasklets" => [%{"name" => "t", "cell_keys" => [1, "k"], "mailbox_capacity" => "big"}]
      }

      FleetStateHelper.announce(ns, "r1", [factory("market", bad)])

      assert %{graph: %{channels: [%{name: "in", consumers: [], overflow: nil}], tasklets: [t]}} =
               FleetManager.factory_graph(ns, "market")

      assert %{cell_keys: ["k"], mailbox_capacity: nil, mode: nil} = t

      {_outcome, out, ""} =
        CliCapture.run(fn -> Cli.call("Relay.graph", Jason.encode!([ns, "market-eu"]), []) end)

      assert out =~ "entry: in"
    end

    test "an unknown relay is refused as Relay.api refuses it", %{namespace: ns} do
      assert {:error, %Error{code: "relay.not_found"}} = Relay.graph(ns, "ghost", @context)
      assert {:error, %Error{code: "relay.not_found"}} = Relay.api(ns, "ghost", @context)
    end

    test "flz relay graph prints each tasklet with its channels and their ends", %{namespace: ns} do
      FleetStateHelper.announce(ns, "r1", [factory("market", graph(["ohlc", "atr"]))])

      {_outcome, out, ""} =
        CliCapture.run(fn -> Cli.call("Relay.graph", Jason.encode!([ns, "market-eu"]), []) end)

      assert out == """
             Graph of relay 'market-eu' (factory 'market'):
               entry: trades_in
               ohlc  single, mailbox 1024
                 in:  trades_in
                 out: ohlc_out → output (drop_newest)
               atr  single, mailbox 1024
                 in:  ohlc_out
                 out: ask → call factory:pricing
             """
    end

    test "flz relay graph says when no graph was announced", %{namespace: ns} do
      FleetStateHelper.announce(ns, "r1", [factory("legacy", nil)])

      {_outcome, out, ""} =
        CliCapture.run(fn -> Cli.call("Relay.graph", Jason.encode!([ns, "legacy-1"]), []) end)

      assert out =~ "No graph announced"
    end
  end

  describe "under enforcement" do
    setup %{namespace: ns} do
      FlezhaTest.Case.install([{:auth_stores, enabled: true, claim: true}])
      :ok = RoleStore.bind("eve", ns, :stopper)
      :ok = RoleStore.bind("vic", ns, :viewer)
      FleetStateHelper.announce(ns, "r1", [factory("market", graph(["ohlc", "atr"]))])
      :ok
    end

    test "a caller whose role lacks relay_info is refused, and a viewer reads",
         %{namespace: ns} do
      assert {:error, %Error{code: "authz.role_insufficient"}} =
               Relay.graph(ns, "market-eu", %{source: :cli, principal: "eve"})

      assert {:ok, %{factory: "market"}} =
               Relay.graph(ns, "market-eu", %{source: :cli, principal: "vic"})
    end
  end

  defp atomize(map),
    do: Map.new(map, fn {key, value} -> {String.to_existing_atom(key), value} end)
end
