defmodule FlezhaTest.HookRecorder do
  @moduledoc false
  # A controller plugin that sends every event it is handed to `recv_pid`, and
  # with `block` waits for `:go` before it returns.
  use Flezha.Plugin.ProviderBehaviour,
    type: :controller,
    options: [
      recv_pid: [type: :pid, required: true, doc: "where every event goes"],
      block: [type: :bool, default: false, doc: "wait for :go after each event"]
    ]

  for hook <- Flezha.Plugin.ProviderBehaviour.controller_callbacks() do
    @impl true
    def unquote(hook)(event, opts), do: record(event, opts)
  end

  defp record(event, %{recv_pid: pid} = opts) do
    send(pid, {:hook, event, self()})

    if opts[:block] do
      receive do
        :go -> :ok
      end
    end

    :ok
  end
end

defmodule FlezhaTest.HookStartOnly do
  @moduledoc false
  use Flezha.Plugin.ProviderBehaviour, type: :controller

  @impl true
  def on_relay_started(_event, _opts), do: :ok
end

defmodule FlezhaTest.WorkerOnly do
  @moduledoc false
  # A controller plugin whose provider runs a worker and defines no hook; the
  # worker tells `recv_pid` it runs.
  use GenServer

  use Flezha.Plugin.ProviderBehaviour,
    type: :controller,
    options: [recv_pid: [type: :pid, required: true, doc: "told when the worker runs"]],
    worker: [module: __MODULE__, restart: :transient]

  def start_link(%{name: name} = opts), do: GenServer.start_link(__MODULE__, opts, name: name)

  @impl GenServer
  def init(opts) do
    send(opts.recv_pid, {:worker_up, self()})
    {:ok, nil}
  end
end

defmodule FlezhaTest.VacantOnly do
  @moduledoc false
  use Flezha.Plugin.ProviderBehaviour,
    type: :controller,
    namespace: "controller_plugin_vacant",
    options: [recv_pid: [type: :pid, required: true, doc: "told when the worker runs"]],
    worker: [module: FlezhaTest.WorkerOnly, restart: :transient]
end

defmodule FlezhaTest.Plugin.ControllerPluginTest do
  @moduledoc """
  A controller plugin on one controller: enabled with its hooks and no
  relay, never bound, running until it is disabled, and told what happened
  in its namespace, once, from a bounded queue.
  """

  use FlezhaTest.Case, fixtures: [:env], async: false

  import ExUnit.CaptureLog

  alias Flezha.Cluster.Directory
  alias Flezha.Convergence.Loop
  alias Flezha.Convergence.Verdict
  alias Flezha.Observability.HookSink
  alias Flezha.Relay.Builder
  alias Flezha.Relay.PartitionGroup
  alias Flezha.Observability.Registry
  alias Flezha.Observability.RingBufferSink
  alias Flezha.Plugin
  alias Flezha.Plugin.Provider.WorkerSup
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.HubManager
  alias Flezha.Service.RelayManager
  alias Flezha.Service.PluginManager
  alias Flezha.State.TestHelpers
  alias FlezhaTest.Fixtures.RelayFixture
  alias FlezhaTest.HookRecorder
  alias FlezhaTest.HookStartOnly
  alias FlezhaTest.VacantOnly
  alias FlezhaTest.WorkerOnly
  alias FlezhaTest.SickProvider
  alias FlezhaTest.Helper.HookCopy
  alias FlezhaTest.Helper.NodeTestHelper

  @namespace "controller_plugin_ns"
  @factory "store_input"

  setup_all do
    TestHelpers.reset_all_cells()
    start_supervised!({WorkerSup, name: WorkerSup.family(WorkerOnly)}, id: :worker_only)
    start_supervised!({WorkerSup, name: WorkerSup.family(VacantOnly)}, id: :vacant_only)
    runner_id = NodeTestHelper.start_runner(@namespace)

    on_exit(fn ->
      NodeTestHelper.stop_runner(@namespace, runner_id)
      FleetManager.stop_namespace(@namespace)
    end)

    :ok
  end

  setup do
    SickProvider.reset()

    on_exit(fn ->
      RelayManager.stop_all_relays(@namespace)

      Enum.each(
        ["notify", "partial", "ill", "worker"],
        &PluginManager.disable_plugin(@namespace, &1)
      )
    end)

    :ok
  end

  defp notify(hooks, opts \\ %{}) do
    enabled =
      PluginManager.enable_plugin("notify", %Plugin{
        id: "notify",
        provider: HookRecorder,
        namespace: @namespace,
        opts: Map.merge(%{recv_pid: self()}, opts),
        hooks: Enum.map(hooks, &{&1, []})
      })

    with {:ok, _} <- enabled, do: HookCopy.await_listening()
    enabled
  end

  defp start_relay(id) do
    {:ok, _} = id |> RelayFixture.get(@factory) |> RelayFixture.start(@namespace)
    id
  end

  defp code(reason), do: Flezha.Error.from_reason(reason, :"plugin.enable")

  describe "enable, hooks and no binding" do
    test "an enable stores the hooks, binds no relay and starts one copy until disabled" do
      {:ok, _} = notify(["on_relay_failed"])

      row = Enum.find(PluginManager.plugins(@namespace), &(&1.name == "notify"))
      assert row.hooks == ["on_relay_failed"]
      assert row.relays == []
      assert [copy] = Directory.plugin_hosts(@namespace, "notify")
      assert node(copy) == node()

      {:ok, _} = PluginManager.disable_plugin(@namespace, "notify")
      assert Directory.plugin_hosts(@namespace, "notify") == []
    end

    test "a relay hook is plugin.unknown_hook, naming the hooks a controller plugin takes" do
      assert {:error, reason} = notify(["on_output"])
      error = code(reason)
      assert error.code == "plugin.unknown_hook"
      assert error.message =~ "on_relay_started"
      refute error.message =~ "on_input"
    end

    test "a provider with a worker is enabled with no hook and its worker runs until disabled" do
      assert {:ok, _} =
               PluginManager.enable_plugin("worker", %Plugin{
                 id: "worker",
                 provider: WorkerOnly,
                 namespace: @namespace,
                 opts: %{recv_pid: self()}
               })

      assert_receive {:worker_up, worker}, 5_000
      ref = Process.monitor(worker)
      {:ok, _} = PluginManager.disable_plugin(@namespace, "worker")
      assert_receive {:DOWN, ^ref, :process, ^worker, _}, 5_000
    end

    test "a provider bound to a namespace no runner serves gets its hub from the enable" do
      vacant = "controller_plugin_vacant"

      on_exit(fn ->
        PluginManager.disable_plugin(vacant, "worker")
        HubManager.stop_hub(vacant)
      end)

      enable = fn provider ->
        PluginManager.enable_plugin("worker", %Plugin{
          id: "worker",
          provider: provider,
          namespace: vacant,
          opts: %{recv_pid: self()}
        })
      end

      assert Directory.hub_nodes(vacant) == []
      assert {:error, :registry_unavailable} = enable.(WorkerOnly)
      assert Directory.hub_nodes(vacant) == []

      assert {:ok, _} = enable.(VacantOnly)
      assert_receive {:worker_up, _worker}, 5_000
      assert Directory.hub_nodes(vacant) == [node()]
    end

    test "a provider with no worker and no hook is plugin.hooks_missing" do
      assert {:error, reason} =
               PluginManager.enable_plugin("notify", %Plugin{
                 id: "notify",
                 provider: HookRecorder,
                 namespace: @namespace,
                 opts: %{recv_pid: self()}
               })

      assert code(reason).code == "plugin.hooks_missing"
    end

    test "a provider with a worker that takes hooks still needs one" do
      assert {:error, reason} =
               PluginManager.enable_plugin("notify", %Plugin{
                 id: "notify",
                 provider: Flezha.Plugin.Provider.HookWebhook,
                 namespace: @namespace,
                 opts: %{url: "https://hooks.test/x"}
               })

      assert code(reason).code == "plugin.hooks_missing"
    end

    test "a provider bound to one namespace is refused elsewhere, naming it" do
      assert {:error, reason} =
               PluginManager.enable_plugin("worker", %Plugin{
                 id: "worker",
                 provider: VacantOnly,
                 namespace: @namespace,
                 opts: %{recv_pid: self()}
               })

      error = code(reason)
      assert error.code == "plugin.namespace_refused"
      assert error.details["namespace"] == "controller_plugin_vacant"
      assert Directory.plugin_hosts(@namespace, "worker") == []
    end

    test "a hook its provider does not define is plugin.hook_not_implemented" do
      assert {:error, reason} =
               PluginManager.enable_plugin("partial", %Plugin{
                 id: "partial",
                 provider: HookStartOnly,
                 namespace: @namespace,
                 hooks: [{"on_dead_letter", []}]
               })

      assert code(reason).code == "plugin.hook_not_implemented"
    end

    test "a bind, and an enable naming a relay, are plugin.not_bindable" do
      op = start_relay("not_bindable_op")
      {:ok, _} = notify(["on_relay_failed"])

      assert {:error, reason} = PluginManager.bind_plugin(@namespace, "notify", op)
      assert code(reason).code == "plugin.not_bindable"

      assert {:error, reason} =
               PluginManager.enable_plugin("notify", %Plugin{
                 id: "notify",
                 provider: HookRecorder,
                 namespace: @namespace,
                 relay_id: op,
                 opts: %{recv_pid: self()}
               })

      assert code(reason).code == "plugin.not_bindable"
    end

    test "an update changes the hooks the copy takes" do
      {:ok, _} = notify(["on_relay_failed"])

      assert {:ok, %{applied: :restarted}} =
               PluginManager.update_plugin(@namespace, "notify", %{}, [
                 {"on_relay_started", []}
               ])

      row = Enum.find(PluginManager.plugins(@namespace), &(&1.name == "notify"))
      assert row.hooks == ["on_relay_started"]

      op = start_relay("updated_hooks_op")
      assert_receive {:hook, %{hook: :on_relay_started, relay: ^op}, _}, 5_000
    end

    test "the copy keeps running past two idle-stop periods with no relay" do
      Application.put_env(:flezha, :plugin_idle_stop_ms, 100)
      {:ok, _} = notify(["on_relay_failed"])
      assert [copy] = Directory.plugin_hosts(@namespace, "notify")

      Process.sleep(300)
      assert Process.alive?(copy)
      assert Directory.plugin_hosts(@namespace, "notify") == [copy]
    end
  end

  describe "the five hooks" do
    test "a deliberate start and stop each tell the plugin once; a restart tells it nothing" do
      {:ok, _} = notify(["on_relay_started", "on_relay_stopped"])
      op = start_relay("told_op")

      assert_receive {:hook, started, _}, 5_000
      controller = node()

      assert %{
               hook: :on_relay_started,
               namespace: @namespace,
               relay: ^op,
               factory: @factory,
               controller: ^controller,
               text: text,
               at: at
             } = started

      assert text =~ op
      assert {:ok, _, 0} = DateTime.from_iso8601(at)

      {:ok, pid} = RelayManager.get_relay_pid(@namespace, op)

      log =
        capture_log(fn ->
          Process.exit(pid, :kill)

          assert NodeTestHelper.wait_until(
                   fn ->
                     match?(
                       {:ok, new} when new != pid,
                       RelayManager.get_relay_pid(@namespace, op)
                     )
                   end,
                   5_000
                 )
        end)

      assert log =~ "#{op}"
      refute_receive {:hook, %{hook: :on_relay_started}, _}, 1_000

      :ok = RelayManager.stop_relay(@namespace, op)

      assert_receive {:hook, %{hook: :on_relay_stopped, relay: ^op, reason: :stopped}, _},
                     5_000
    end

    test "a start and a stop reach the event stream and each hook once" do
      {:ok, _} = notify(["on_relay_started", "on_relay_stopped"])
      since = System.monotonic_time(:microsecond)
      op = start_relay("streamed_op")
      :ok = RelayManager.stop_relay(@namespace, op)

      assert_receive {:hook, %{hook: :on_relay_started, relay: ^op}, _}, 5_000
      assert_receive {:hook, %{hook: :on_relay_stopped, relay: ^op}, _}, 5_000
      refute_receive {:hook, %{relay: ^op}, _}, 300

      streamed = fn ->
        for %{name: name, metadata: %{relay: ^op}} <-
              RingBufferSink.query(:flezha_relay, since),
            do: name
      end

      assert NodeTestHelper.wait_until(
               fn -> Enum.sort(streamed.()) == [:relay_started, :relay_stopped] end,
               2_000
             )
    end

    test "a park retried over several ticks is one failure, naming the park's code" do
      {:ok, _} = start_supervised({Loop, tick_ms: 60_000})
      {:ok, _} = notify(["on_relay_failed"])
      op = start_relay("parked_op")
      Loop.tick_now()

      reason = {:plugin_prepare_failed, "a_plugin", :boom}
      {:ok, pid} = RelayManager.get_relay_pid(@namespace, op)
      :sys.replace_state(pid, &%{&1 | init_error: reason})
      for _ <- 1..3, do: Loop.tick_now()

      expected = "plugin.prepare_failed"
      assert_receive {:hook, %{hook: :on_relay_failed, relay: ^op, error: ^expected}, _}
      refute_receive {:hook, %{hook: :on_relay_failed}, _}, 300
    end

    test "a park read unknown or unbound for a tick between is one failure until it recovers" do
      {:ok, _} = notify(["on_relay_failed"])
      subject = {:relay, {@namespace, "flaky_op"}}
      reason = {:parked, {:plugin_prepare_failed, "p", :boom}}
      read = &Verdict.new(subject, &1, reason: reason, facts: %{node: node()})

      # The ticks the loop would judge, through the function the loop calls.
      tick = fn states ->
        Enum.reduce(states, %{}, fn state, previous ->
          current = %{subject => read.(state)}
          Verdict.emit_transitions(previous, current)
          current
        end)
      end

      tick.([:healthy, :parked, :unknown, :parked, :unbound, :parked])
      assert_receive {:hook, %{hook: :on_relay_failed, relay: "flaky_op"}, _}, 2_000
      refute_receive {:hook, %{hook: :on_relay_failed}, _}, 300

      tick.([:parked, :healthy, :parked])
      assert_receive {:hook, %{hook: :on_relay_failed, relay: "flaky_op"}, _}, 2_000
      refute_receive {:hook, %{hook: :on_relay_failed}, _}, 300
    end

    test "a rescale tells nothing: its members are the group it moves" do
      group = "rescaled_grp"

      {:ok, %{members: [_, _]}} =
        PartitionGroup.start(
          @namespace,
          %Builder{id: group, factory_id: "partition_pass", init_values: %{}, opts: []},
          2
        )

      {:ok, _} = notify(["on_relay_started", "on_relay_stopped"])

      assert {:ok, %{members: grown}} = PartitionGroup.rescale(@namespace, group, 4)
      assert length(grown) == 4
      assert {:ok, %{members: shrunk}} = PartitionGroup.rescale(@namespace, group, 2)
      assert length(shrunk) == 2
      refute_receive {:hook, _, _}, 1_000
    end

    test "a plugin unhealthy for three probe windows is one event naming it" do
      {:ok, _} = start_supervised({Loop, tick_ms: 60_000})
      {:ok, _} = notify(["on_plugin_unhealthy"])
      op = start_relay("ill_op")

      {:ok, _} =
        PluginManager.enable_plugin("ill", %Plugin{
          id: "ill",
          provider: SickProvider,
          namespace: @namespace,
          relay_id: op,
          hooks: [{:on_output, []}]
        })

      Loop.tick_now()
      SickProvider.set_health({:unhealthy, :sick})
      for _ <- 1..3, do: Loop.tick_now()

      assert_receive {:hook, %{hook: :on_plugin_unhealthy, plugin: "ill", relay: ^op} = event, _}

      assert event.error == "internal.uncatalogued"
      refute_receive {:hook, %{hook: :on_plugin_unhealthy}, _}, 300
    end

    test "a dead letter names its entry and carries no payload" do
      {:ok, _} = notify(["on_dead_letter"])
      op = start_relay("dead_op")
      payload = "poison-#{System.unique_integer([:positive])}"

      entry =
        Flezha.Dlq.Entry.new!(%{
          relay_class: op,
          tasklet_id: "t",
          cell_key: "k",
          channel: "in",
          payload: payload,
          error: "boom"
        })

      {:ok, ref} = Flezha.Dlq.Store.append(entry)
      on_exit(fn -> Flezha.Dlq.Store.drop(ref) end)

      assert_receive {:hook, %{hook: :on_dead_letter, relay: ^op} = event, _}, 5_000
      assert %{channel: "in", error: "boom", entry: ^ref, namespace: @namespace} = event
      refute inspect(event) =~ payload
    end

    test "an event of another namespace reaches nothing" do
      {:ok, _} = notify(["on_relay_started"])

      :telemetry.execute([:flezha, :relay, :started], %{count: 1}, %{
        namespace: "another_ns",
        relay: "elsewhere",
        factory: @factory
      })

      refute_receive {:hook, _, _}, 300
    end
  end

  describe "delivery" do
    test "with no controller plugin no hook sink is registered and a start and a park send none" do
      # The sink leaves the registry once the last copy, an earlier test's
      # included, is gone.
      assert NodeTestHelper.wait_until(
               fn -> Registry.lookup_sink(HookSink.sink_name()) == :error end,
               2_000
             )

      sink = Process.whereis(HookSink)
      :erlang.trace(sink, true, [:receive])
      before = RingBufferSink.count(:flezha_hooks)

      op = start_relay("unheard_op")
      {:ok, pid} = RelayManager.get_relay_pid(@namespace, op)
      :sys.replace_state(pid, &%{&1 | init_error: {:plugin_prepare_failed, "p", :boom}})
      :ok = RelayManager.stop_relay(@namespace, op)

      :erlang.trace(sink, false, [:receive])
      refute_receive {:trace, ^sink, :receive, {:signal, _}}, 100
      assert RingBufferSink.count(:flezha_hooks) == before
    end

    test "a blocked plugin falls behind alone: every emit returns, its queue holds ten" do
      Application.put_env(:flezha, :plugin_backlog_frames, 10)
      {:ok, _} = notify(["on_dead_letter"], %{block: true})
      [copy] = Directory.plugin_hosts(@namespace, "notify")

      for n <- 1..100 do
        assert :ok =
                 :telemetry.execute([:flezha, :dlq, :dead_lettered], %{count: 1}, %{
                   namespace: @namespace,
                   relay: "op#{n}",
                   entry: "e#{n}",
                   channel: "in",
                   error: "boom"
                 })
      end

      assert_receive {:hook, %{hook: :on_dead_letter}, task}, 5_000
      HookCopy.assert_lagging(copy, 10)
      send(task, :go)
    end
  end
end
