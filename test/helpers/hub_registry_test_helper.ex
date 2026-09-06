defmodule FlezhaTest.Helper.HubRegistryTestHelper do
  @moduledoc """
  Seed a real ProcessHub and inject synthetic registry entries so hub-facing
  tests can exercise registry reads and hub lifecycle without a live SDK or
  runner process.
  """
  alias Flezha.Identificator
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

    def start_link({_ns, _operative}), do: GenServer.start_link(__MODULE__, :ok)
    def start_link({_ns, _runner, _operative}), do: GenServer.start_link(__MODULE__, :ok)

    @impl true
    def init(:ok), do: {:ok, %{}}

    # Stands in for an Overwatch, which absorbs the binding re-assert nudge.
    @impl true
    def handle_info(_msg, state), do: {:noreply, state}

    # Overwatch verbs (`:get_overwatch_state`, quota reads) probed by the stop
    # path must not kill the stub — answer as an unreadable operative instead.
    @impl true
    def handle_call(_msg, _from, state), do: {:reply, {:error, :stub}, state}
  end

  # `start_args/1`'s two-element clause yields a nil recorded runner, and
  # `unplaceable_runner/5` RAISES on nil — which kills the whole start request.
  # Naming a runner keeps an unplaceable row on the log-and-continue path.
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
  Inject a synthetic registry entry with an explicit `node_pids` list, so a
  multi-node operative is reproduced without a real peer node + runner.
  """
  def inject_operative(ns, operative_id, node_pids) do
    inject_operative(ns, operative_id, node_pids, %{
      id: operative_id,
      start:
        {StubChild, :start_link,
         [{ns, @recorded_runner, %{id: operative_id, factory_id: "market"}}]}
    })
  end

  @doc """
  Inject a synthetic registry entry carrying an explicit `child_spec`, for tests
  that need a start-args shape other than the default one.
  """
  def inject_operative(ns, operative_id, node_pids, child_spec) do
    hub_id = Identificator.hub(ns)

    :ok = ProcessRegistry.bulk_insert(hub_id, %{operative_id => {child_spec, node_pids, %{}}})
  end

  @doc """
  Inject a registered operative bound to a live local pid — the common case,
  and the shape a durably-suspended operative keeps (only its runner binding is
  cleared, its Overwatch pid stays in the registry).
  """
  def inject_operative(ns, operative_id),
    do:
      inject_operative(ns, operative_id, [
        {node(), spawn(&NodeTestHelper.park/0)}
      ])

  @doc """
  Start `operative_id` durably in `namespace`, through the same declared-set
  precommit every real start rides — so the hub's declared set holds it,
  whether or not anything is running it. The recorded runner keeps the
  `child_data_alter` hook on its log-and-continue path.
  """
  def start_durable(namespace, operative_id, factory_id \\ "market") do
    spec = %{
      id: operative_id,
      start:
        {StubChild, :start_link,
         [{namespace, @recorded_runner, %{id: operative_id, factory_id: factory_id}}]},
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
  def seed_runner(ns, runner_id, factory_id) do
    # One definition of "this namespace holds these runners advertising these
    # factories" — including the SDK language placement filters on.
    FleetStateHelper.seed(ns, %{runner_id => [factory_id]})
    register_control_unit(runner_id)
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
