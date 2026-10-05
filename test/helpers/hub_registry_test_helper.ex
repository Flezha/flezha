defmodule FlezhaTest.Helper.HubRegistryTestHelper do
  @moduledoc """
  Seed a real ProcessHub and inject synthetic registry entries so hub-facing
  tests can exercise registry reads and hub lifecycle without a live SDK or
  runner process.
  """
  import ExUnit.Callbacks, only: [on_exit: 1]

  alias Flezha.Identificator
  alias Flezha.Layout.Table, as: Layout
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.HubManager
  alias FlezhaTest.Helper.FleetStateHelper
  alias FlezhaTest.Helper.NodeTestHelper
  alias ProcessHub.Service.ProcessRegistry

  # A redistribution really does start the injected child, so the default spec
  # must name something startable. An Overwatch is not: it would bind a runner
  # these tests do not have.
  defmodule StubChild do
    @moduledoc false
    use GenServer

    def start_link({_ns, _relay} = arg), do: GenServer.start_link(__MODULE__, arg)
    def start_link({_ns, _runner, _relay} = arg), do: GenServer.start_link(__MODULE__, arg)

    @doc """
    The start argument this instance was handed. An Overwatch reads its plugin
    set out of exactly this argument (`Flezha.Relay.Lifecycle.complete_init/4`
    hands `relay.opts[:plugins]` to `Plugins.prepare/2`), so a test that
    cannot afford a live runner asks the stub what it would have subscribed.
    """
    def started_with(pid), do: GenServer.call(pid, :started_with)

    @impl true
    def init(arg), do: {:ok, %{arg: arg}}

    # Stands in for an Overwatch, which absorbs the binding re-assert nudge.
    @impl true
    def handle_info(_msg, state), do: {:noreply, state}

    @impl true
    def handle_call(:started_with, _from, state), do: {:reply, state.arg, state}

    # The runtime half of an enable/disable. The stub has no extension table;
    # answering `:ok` is what lets a test drive the durable half of
    # `PluginManager.enable_plugin/2` without a bound runner.
    def handle_call({:plugin_subscribe, _id, _binding, _entry}, _from, state),
      do: {:reply, :ok, state}

    def handle_call({:plugin_unsubscribe, _id}, _from, state), do: {:reply, :ok, state}

    # Overwatch verbs (`:get_overwatch_state`, quota reads) probed by the stop
    # path must not kill the stub — answer as an unreadable relay instead.
    def handle_call(_msg, _from, state), do: {:reply, {:error, :stub}, state}
  end

  # The runner a synthetic row records by default. `recorded_runner: nil` gives
  # the two-element shape a real `ProcessHub.start_child/3` declares, which is
  # what the reconcile replays.
  @recorded_runner "test_recorded_runner"

  @doc "Start (or reuse) the namespace's hub, normalising the start-result shapes."
  def ensure_hub(ns) do
    case HubManager.start_hub(ns) do
      {:ok, pid} -> {:ok, pid}
      {:ok, pid, _info} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
      other -> other
    end
  end

  @doc """
  Stop the namespace's hub the way a controller restart does: its registry
  directory stays on disk, where `HubManager.stop_hub/1` would wipe it.
  """
  def stop_hub_keeping_registry(ns) do
    hub_id = Identificator.hub(ns)
    Supervisor.terminate_child(HubManager, hub_id)
    Supervisor.delete_child(HubManager, hub_id)
  end

  @doc "Where the namespace's hub keeps its durable registry under the configured root."
  def registry_file(ns),
    do: Layout.file(:process_hub, [to_string(Identificator.hub(ns)), Layout.hub_registry_file()])

  @doc """
  On exit, stop every hub started after this call and only those: a hub that
  was already running belongs to the suite, and later files depend on it.
  """
  def stop_new_hubs_on_exit() do
    inherited = HubManager.get_running_namespaces()

    ExUnit.Callbacks.on_exit(fn ->
      Enum.each(HubManager.get_running_namespaces() -- inherited, &HubManager.stop_hub/1)
    end)
  end

  @doc """
  Inject a synthetic registry entry with an explicit `node_pids` list, so a
  multi-node relay is reproduced without a real peer node + runner.
  """
  def inject_relay(ns, relay_id, node_pids) do
    inject_relay(ns, relay_id, node_pids, %{
      id: relay_id,
      start:
        {StubChild, :start_link, [{ns, @recorded_runner, %{id: relay_id, factory_id: "market"}}]}
    })
  end

  @doc """
  Inject a synthetic registry entry carrying an explicit `child_spec`, for tests
  that need a start-args shape other than the default one.
  """
  def inject_relay(ns, relay_id, node_pids, child_spec) do
    hub_id = Identificator.hub(ns)

    :ok = ProcessRegistry.bulk_insert(hub_id, %{relay_id => {child_spec, node_pids, %{}}})
  end

  @doc """
  Inject a registered relay bound to a live local pid — the common case,
  and the shape a durably-suspended relay keeps (only its runner binding is
  cleared, its Overwatch pid stays in the registry).
  """
  def inject_relay(ns, relay_id),
    do:
      inject_relay(ns, relay_id, [
        {node(), spawn(&NodeTestHelper.park/0)}
      ])

  @doc """
  Start `relay_id` durably in `namespace`, through the same declared-set
  precommit every real start rides — so the hub's declared set holds it,
  whether or not anything is running it.

  Options: `:factory_id`, and `:recorded_runner` for the runner position of the
  start args. Passing `recorded_runner: nil` declares the two-element shape a
  real relay start submits, so the declared entry the reconcile replays is
  the production one; the default names a runner instead. `:opts` puts an
  option list on the declared relay, which is where a plugin manifest
  lives; without it the relay carries no `:opts` key at all.
  """
  def start_durable(namespace, relay_id, opts \\ []) do
    factory_id = Keyword.get(opts, :factory_id, "market")

    relay =
      case Keyword.fetch(opts, :opts) do
        {:ok, relay_opts} -> %{id: relay_id, factory_id: factory_id, opts: relay_opts}
        :error -> %{id: relay_id, factory_id: factory_id}
      end

    arg =
      case Keyword.get(opts, :recorded_runner, @recorded_runner) do
        nil -> {namespace, relay}
        runner -> {namespace, runner, relay}
      end

    spec = %{
      id: relay_id,
      start: {StubChild, :start_link, [arg]},
      restart: :transient
    }

    %ProcessHub.StartResult{status: :ok} =
      namespace
      |> Identificator.hub()
      |> ProcessHub.start_child(spec, awaitable: true, durable: true, timeout: 5_000)
      |> ProcessHub.Future.await()

    :ok
  end

  @doc """
  Seed a synthetic active runner — its fleet entry, factory advertisement, and
  control-unit registration — so placement hands out its id without a live
  SDK. The caller owns FleetManager state restoration (snapshot +
  `:sys.replace_state` on exit).
  """
  def seed_runner(ns, runner_id, factory_id), do: seed_runners(ns, [runner_id], factory_id)

  @doc """
  Isolate a test that stops and restarts `namespace`'s hub: the fleet state is
  put back and the hub stopped when the test ends, the registry lives under
  `registry_root`, auto-restore is on, and the first reconcile round opens after
  `grace_ms`. A hub with no peers opens that round at the settle window or the
  grace, whichever comes first, so a short grace keeps every boot off the 2 s
  window. The caller declares the `:env` fixture, which puts the keys back.
  """
  def recovering_hub(namespace, registry_root, grace_ms) do
    fleet_state = :sys.get_state(FleetManager)
    on_exit(fn -> :sys.replace_state(FleetManager, fn _ -> fleet_state end) end)

    Application.put_env(:flezha, :auto_restore_enabled, true)
    Application.put_env(:flezha, :process_hub_registry_root, registry_root)
    Application.put_env(:flezha, :process_hub_reconcile_grace_ms, grace_ms)
    Application.put_env(:flezha, :process_hub_reconcile_interval_ms, 1_000)

    on_exit(fn -> HubManager.stop_hub(namespace) end)
    :ok
  end

  @doc """
  The bindings a restored relay was started with. An Overwatch builds its
  extension table from exactly these (`Flezha.Relay.Lifecycle.complete_init/4`
  hands `relay.opts[:bindings]` to `Plugins.prepare/2`), and `StubChild`
  stands in for it, so the stub is asked what it was handed.
  """
  def started_plugins(hub_id, relay_id) do
    relay =
      ProcessRegistry.dump(hub_id)
      |> Map.fetch!(relay_id)
      |> elem(1)
      |> HubManager.live_pid()
      |> StubChild.started_with()
      |> then(fn
        {_ns, relay} -> relay
        {_ns, _runner, relay} -> relay
      end)

    Keyword.get(relay.opts, :bindings, [])
  end

  @doc "The node `relay_id`'s registry row names a live pid on, awaited up to `timeout_ms`."
  def await_bound_node(hub_id, relay_id, timeout_ms) do
    NodeTestHelper.wait_until(
      fn ->
        case Map.get(ProcessRegistry.dump(hub_id), relay_id) do
          {_cspec, node_pids, _meta} -> HubManager.live_node(node_pids)
          _ -> nil
        end
      end,
      timeout_ms
    )
  end

  @doc """
  Seed a whole synthetic fleet at once — every runner in `runner_ids` advertises
  `factory_id`. `FleetStateHelper.seed/2` replaces the namespace's runner set,
  so a fleet has to be declared in one call rather than one runner at a time.
  """
  def seed_runners(ns, runner_ids, factory_id) do
    # One definition of "this namespace holds these runners advertising these
    # factories" — including the SDK language placement filters on.
    FleetStateHelper.seed(ns, Map.new(runner_ids, &{&1, [factory_id]}))
    Enum.each(runner_ids, &register_control_unit/1)
  end

  # `runner_registered?/1` gates on the CU registration, so a synthetic runner
  # needs one before placement will hand out its id. Unlinked: on_exit kills
  # it, and a kill must not propagate into ExUnit.
  defp register_control_unit(runner_id) do
    owner = self()

    {:ok, cu} =
      Task.start(fn ->
        {:ok, _} = Registry.register(Identificator, "#{runner_id}_cu", nil)
        send(owner, {:cu_registered, runner_id})
        NodeTestHelper.park()
      end)

    receive do
      {:cu_registered, ^runner_id} -> :ok
    after
      1_000 -> raise "control unit for #{runner_id} did not register"
    end

    ExUnit.Callbacks.on_exit(fn -> Process.exit(cu, :kill) end)
    :ok
  end
end
