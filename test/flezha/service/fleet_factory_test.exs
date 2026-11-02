defmodule Flezha.Service.FleetFactoryTest do
  @moduledoc """
  `runner-factory-sync` on one controller: a factory's announcement carries its
  class declarations and the richer graph, the factory rows carry them, and
  `Fleet.factory` describes one factory whether or not an instance of it runs.
  """
  # async: false: Mimic global mode, and the live FleetManager.
  use FlezhaTest.Case, async: false
  use Mimic

  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.AuthZ.RoleStore
  alias Flezha.Cluster.Directory
  alias Flezha.Error
  alias Flezha.Runner.ControlChannel.Requests
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.{Cli, Fleet, HubManager, Relay}
  alias FlezhaTest.Helper.{CliCapture, FleetStateHelper}

  import FleetStateHelper, only: [graph_channel: 5, graph_channel: 6]

  setup :set_mimic_global_until_exit

  @context %{source: :http, principal: "console@_system"}

  setup do
    original = :sys.get_state(FleetManager)
    namespace = "fleet_factory_#{System.unique_integer([:positive])}"
    FleetStateHelper.seed(namespace, %{"r1" => [], "r2" => []})

    on_exit(fn ->
      FleetStateHelper.forget_established(namespace, ["fan_trade", "market", "legacy"])
      :sys.replace_state(FleetManager, fn _ -> original end)
    end)

    stub(HubManager, :relay_spec, fn
      _namespace, "fan-1" ->
        {:ok, %{factory_id: "fan_trade", opts: []}, %{runner: nil, node: nil}}

      namespace, id ->
        {:error, {:relay_not_found, namespace, id}}
    end)

    stub(Directory, :hub_nodes, fn
      ^namespace -> [node()]
      other -> Mimic.call_original(Directory, :hub_nodes, [other])
    end)

    %{namespace: namespace}
  end

  # A saga as the connect frame carries it: `reserve` and `place` compensate,
  # `notify` does not; `place` calls `pricing` with a deadline.
  defp fan_trade(declarations \\ %{"durable" => "memory", "compensable_tasks" => "0,1"}) do
    %{
      "id" => "fan_trade",
      "sdk_lang" => 1,
      "run" => "steps",
      "each" => "once",
      "on_failure" => "undo_all",
      "declarations" => declarations,
      "graph" => %{
        "channels" => [
          graph_channel("in", "entry", "", ["reserve"], "none"),
          graph_channel("ask", "plugin_bound", "place", [], "call", %{
            "call_target" => "factory:pricing",
            "call_deadline_ms" => 2000,
            "call_create" => true
          }),
          graph_channel("s1", "inter_tasklet", "reserve", ["place", "notify"], "none", %{
            "retry_max" => 3,
            "retry_backoff_ms" => 50
          })
        ],
        "tasklets" => [
          tasklet("reserve", %{"compensates" => true}),
          tasklet("place", %{"compensates" => true}),
          tasklet("notify", %{
            "compensates" => false,
            "mode" => "dynamic",
            "idle_ttl_ms" => 60_000,
            "max_cells" => 100
          })
        ]
      }
    }
  end

  defp tasklet(name, extra) do
    Map.merge(
      %{"name" => name, "mode" => "single", "cell_keys" => [], "mailbox_capacity" => 1024},
      extra
    )
  end

  defp row(ns, id) do
    {:ok, rows} = FleetManager.list_factories(ns)
    Enum.find(rows, &(&1.id == id))
  end

  describe "the factory table" do
    test "keeps the declarations and the new graph fields the connect frame carries",
         %{namespace: ns} do
      FleetStateHelper.announce(ns, "r1", [fan_trade()])

      assert %{
               each: :once,
               on_failure: :undo_all,
               declarations: %{durable: :memory, sleep_after_ms: nil, keyed: nil},
               declarations_disagree: false
             } = row(ns, "fan_trade")

      %{graph: graph} = FleetManager.factory_graph(ns, "fan_trade")

      assert [
               %{name: "reserve", compensates: true, idle_ttl_ms: nil, max_cells: nil},
               %{name: "place", compensates: true},
               %{name: "notify", compensates: false, idle_ttl_ms: 60_000, max_cells: 100}
             ] = graph.tasklets

      assert %{retry_max: nil, call_deadline_ms: 2000, call_create: true} =
               Enum.find(graph.channels, &(&1.name == "ask"))

      assert %{retry_max: 3, retry_backoff_ms: 50, call_deadline_ms: nil, call_create: nil} =
               Enum.find(graph.channels, &(&1.name == "s1"))
    end

    test "an UpdateFactories frame stores the same, and a re-announcement replaces it",
         %{namespace: ns} do
      FleetStateHelper.announce(ns, "r1", [fan_trade()])
      from_json = {row(ns, "fan_trade").declarations, FleetManager.factory_graph(ns, "fan_trade")}

      decl = FleetStateHelper.factory_decl(fan_trade())

      update = fn decl ->
        Requests.update_factories(
          %{namespace: ns, runner_id: "r1"},
          %RunnerCommands.UpdateFactories{factories: [decl]}
        )
      end

      update.(decl)

      assert {row(ns, "fan_trade").declarations, FleetManager.factory_graph(ns, "fan_trade")} ==
               from_json

      update.(%{decl | declarations: %{"durable" => "remote", "max_duration_ms" => "120000"}})
      assert %{durable: :remote, max_duration_ms: 120_000} = row(ns, "fan_trade").declarations
    end

    test "a frame without declarations or the new fields stores them as not announced",
         %{namespace: ns} do
      legacy =
        update_in(fan_trade(), ["graph", "tasklets"], fn tasklets ->
          Enum.map(tasklets, &Map.drop(&1, ["compensates", "idle_ttl_ms", "max_cells"]))
        end)

      FleetStateHelper.announce(ns, "r1", [Map.delete(legacy, "declarations")])
      assert %{declarations: nil} = row(ns, "fan_trade")

      FleetStateHelper.announce(ns, "r1", [%{legacy | "declarations" => %{}}])
      assert %{declarations: nil} = row(ns, "fan_trade")

      %{graph: graph} = FleetManager.factory_graph(ns, "fan_trade")
      assert Enum.all?(graph.tasklets, &(&1.compensates == nil and &1.max_cells == nil))
    end

    test "a FactoryDecl without the new fields decodes each of them as absent" do
      {:ok, wire, _size} =
        RunnerCommands.FactoryDecl.encode(%RunnerCommands.FactoryDecl{
          id: "x",
          graph: %RunnerCommands.RelayGraph{
            tasklets: [%RunnerCommands.GraphTasklet{name: "t"}],
            channels: [%RunnerCommands.GraphChannel{name: "c"}]
          }
        })

      assert {:ok, %{declarations: declarations, graph: %{tasklets: [t], channels: [c]}}} =
               RunnerCommands.FactoryDecl.decode(IO.iodata_to_binary(wire))

      assert declarations == %{}
      assert %{compensates: nil, idle_ttl_ms: nil, max_cells: nil} = t
      assert %{retry_max: nil, retry_backoff_ms: nil, call_deadline_ms: nil, call_create: nil} = c
    end

    test "a malformed declaration map stores what it can read and keeps the manager up",
         %{namespace: ns} do
      FleetStateHelper.announce(ns, "r1", [
        fan_trade(%{"durable" => 7, "compensable_tasks" => "x,1", "sleep_after_ms" => "soon"})
      ])

      assert %{declarations: %{durable: :local, sleep_after_ms: nil}} = row(ns, "fan_trade")
    end

    test "a flow factory's row carries the flow declarations", %{namespace: ns} do
      flow = %{
        "id" => "market",
        "sdk_lang" => 1,
        "run" => "flow",
        "declarations" => %{
          "durable" => "local",
          "sleep_after_ms" => "2000",
          "exactly" => "effectively_once",
          "ack" => "checkpoint",
          "keyed" => "false",
          "correction_ms" => "5000"
        }
      }

      FleetStateHelper.announce(ns, "r1", [flow])

      assert %{
               each: nil,
               on_failure: nil,
               declarations: %{
                 durable: :local,
                 sleep_after_ms: 2000,
                 max_duration_ms: nil,
                 effectively_once: true,
                 ack: :checkpoint,
                 correction_ms: 5000,
                 keyed: false
               }
             } = row(ns, "market")
    end
  end

  describe "Fleet.factory" do
    test "describes a factory no instance of runs", %{namespace: ns} do
      FleetStateHelper.announce(ns, "r1", [fan_trade()])

      assert {:ok, read} = Fleet.factory(ns, "fan_trade", @context)

      assert %{
               id: "fan_trade",
               run: :steps,
               each: :once,
               on_failure: :undo_all,
               declarations: %{durable: :memory},
               nodes: [_this_node],
               runners: ["r1"],
               disagree: false
             } = read

      assert Enum.map(read.graph.tasklets, &{&1.name, &1.compensates}) ==
               [{"reserve", true}, {"place", true}, {"notify", false}]
    end

    test "a member relay's graph and the factory's are the same", %{namespace: ns} do
      FleetStateHelper.announce(ns, "r1", [fan_trade()])

      assert {:ok, %{graph: graph}} = Fleet.factory(ns, "fan_trade", @context)
      assert {:ok, %{graph: ^graph}} = Relay.graph(ns, "fan-1", @context)
    end

    test "runners announcing different declarations disagree", %{namespace: ns} do
      FleetStateHelper.announce(ns, "r1", [fan_trade()])
      FleetStateHelper.announce(ns, "r2", [fan_trade(%{"durable" => "remote"})])

      assert %{declarations_disagree: true} = row(ns, "fan_trade")
      assert {:ok, %{disagree: true}} = Fleet.factory(ns, "fan_trade", @context)

      {_outcome, out, ""} =
        CliCapture.run(fn -> Cli.call("Fleet.factory", Jason.encode!([ns, "fan_trade"]), []) end)

      assert out =~ "Connected runners announce this factory differently; this is the latest."
    end

    test "a factory that announced no graph answers graph nil", %{namespace: ns} do
      FleetStateHelper.announce(ns, "r1", [%{"id" => "legacy", "sdk_lang" => 1, "run" => "flow"}])

      assert {:ok, %{id: "legacy", graph: nil, declarations: nil}} =
               Fleet.factory(ns, "legacy", @context)
    end

    test "an unknown factory is refused, naming it", %{namespace: ns} do
      assert {:error, %Error{code: "relay.factory_unknown", message: message}} =
               Fleet.factory(ns, "ghost", @context)

      assert message =~ "ghost"
    end

    test "flz relay factory prints the run, the declarations and the compensating tasklets",
         %{namespace: ns} do
      FleetStateHelper.announce(ns, "r1", [
        fan_trade(%{
          "durable" => "local",
          "sleep_after_ms" => "2000",
          "max_duration_ms" => "120000"
        })
      ])

      {_outcome, out, ""} =
        CliCapture.run(fn -> Cli.call("Fleet.factory", Jason.encode!([ns, "fan_trade"]), []) end)

      assert out == """
             Factory 'fan_trade':
               Run: steps (each: once, on_failure: undo_all)
               Durable: local (sleep_after: 2s)
               Time limit: 2m
               Runners: r1
               entry: in
               reserve  single, mailbox 1024, compensates
                 in:  in
                 out: s1
               place  single, mailbox 1024, compensates
                 in:  s1
                 out: ask → call factory:pricing
               notify  dynamic, mailbox 1024
                 in:  s1
                 out: -
             """
    end

    test "flz relay graph marks the compensating tasklets", %{namespace: ns} do
      FleetStateHelper.announce(ns, "r1", [fan_trade()])

      {_outcome, out, ""} =
        CliCapture.run(fn -> Cli.call("Relay.graph", Jason.encode!([ns, "fan-1"]), []) end)

      assert out =~ "  reserve  single, mailbox 1024, compensates\n"
      assert out =~ "  notify  dynamic, mailbox 1024\n"
    end
  end

  describe "under enforcement" do
    setup %{namespace: ns} do
      FlezhaTest.Case.install([{:auth_stores, enabled: true, claim: true}])
      :ok = RoleStore.bind("eve", ns, :stopper)
      :ok = RoleStore.bind("vic", ns, :viewer)
      FleetStateHelper.announce(ns, "r1", [fan_trade()])
      :ok
    end

    test "a caller whose role lacks the factories verb is refused, and a viewer reads",
         %{namespace: ns} do
      assert {:error, %Error{code: "authz.role_insufficient"}} =
               Fleet.factory(ns, "fan_trade", %{source: :cli, principal: "eve"})

      assert {:ok, %{id: "fan_trade"}} =
               Fleet.factory(ns, "fan_trade", %{source: :cli, principal: "vic"})
    end
  end
end
