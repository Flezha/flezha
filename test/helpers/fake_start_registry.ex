defmodule FlezhaTest.Helper.FakeStartRegistry do
  @moduledoc """
  A stand-in for the durable operative registry, for suites that drive the real
  start and stop paths without a hub.

  `ProcessHub.start_child`'s `:ets.insert_new` **is** the atomic id claim the
  real registry performs, so a concurrent burst exercises the production race
  resolution rather than a serialized double — which is why every start-path
  suite shares this one harness instead of stubbing the verdicts it wants.

  `install/1` stubs the seams the start/stop paths read (`HubManager`,
  `ProcessHub`, `FleetManager`, `Quotas`) against one ETS table and returns it.
  Every started builder is sent to the calling process as `{:started, builder}`;
  `started/0` drains them.
  """

  import Mimic

  alias Flezha.AuthZ.Quotas
  alias Flezha.Partition
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.HubManager

  defmodule Instance do
    @moduledoc """
    The stand-in operative: it answers what the start, stop and read paths call
    on an Overwatch, and records every ingested payload.

    It follows the process that installed the registry rather than linking to
    whoever started it, because a start may resolve inside a `Task` and a real
    operative outlives its starter.
    """
    use GenServer

    def start(owner, builder, registry),
      do: GenServer.start(__MODULE__, {owner, builder, registry})

    @doc "Payloads this instance was asked to ingest, oldest first."
    def ingested(pid), do: GenServer.call(pid, :ingested)

    @impl true
    def init({owner, builder, registry}) do
      Process.monitor(owner)
      {:ok, %{builder: builder, registry: registry, ingested: []}}
    end

    @impl true
    def handle_call(:registry_runner_set, _from, state), do: {:reply, {:ok, "runner-1"}, state}

    def handle_call(:get_overwatch_state, _from, state),
      do:
        {:reply,
         %{
           builder: state.builder,
           id: state.builder.id,
           # Plugins attached after the start live here, not in builder.opts —
           # the shape a rescale must read to re-point them.
           extensions: :persistent_term.get({:fake_live_extensions, state.builder.id}, nil)
         }, state}

    def handle_call(:ingested, _from, state), do: {:reply, Enum.reverse(state.ingested), state}

    def handle_call({:ingest, message, _metadata}, _from, state),
      do: {:reply, :ok, %{state | ingested: [message | state.ingested]}}

    def handle_call(_msg, _from, state), do: {:reply, {:error, :unsupported}, state}

    @impl true
    def handle_info({:DOWN, _ref, :process, _pid, _reason}, state), do: {:stop, :normal, state}
  end

  @doc """
  Install the stubs and return the ETS table backing them.

  Options:

    * `:runners` — the namespace's active runners (default three)
    * `:seed` — builders to pre-register as already-running operatives
  """
  @spec install(keyword()) :: :ets.tid()
  def install(opts \\ []) do
    registry = :ets.new(:fake_start_registry, [:public, :set])
    owner = self()
    parent = self()
    runners = Keyword.get(opts, :runners, ["runner-1", "runner-2", "runner-3"])

    for builder <- Keyword.get(opts, :seed, []), do: insert(registry, owner, builder)

    stub(HubManager, :hub_alive?, fn _ -> true end)
    stub(HubManager, :get_pid, fn _ns, id -> pid_of(registry, id) end)
    stub(HubManager, :operative_rows, fn _ns -> rows(registry) end)

    stub(HubManager, :partition_members, fn _ns, group ->
      registry
      |> rows()
      |> Enum.filter(&(&1.partition != nil and &1.partition.group == group))
      |> Enum.map(&Map.take(&1, [:id, :partition, :node]))
      |> Enum.sort_by(& &1.partition.index)
    end)

    stub(ProcessHub, :child_lookup, fn _hub, id ->
      case pid_of(registry, id) do
        {:ok, pid} -> {id, [{node(), pid}]}
        _ -> nil
      end
    end)

    stub(ProcessHub, :start_child, fn _hub, child_spec, _opts ->
      {_mod, _fun, [{_ns, builder}]} = child_spec.start
      send(parent, {:started, builder})

      case insert_new(registry, owner, builder) do
        {:ok, pid} ->
          %ProcessHub.StartResult{started: [{child_spec.id, [{node(), pid}]}], errors: []}

        :taken ->
          %ProcessHub.StartResult{
            started: [],
            errors: [{child_spec.id, {:error, "operative id already in use"}}]
          }
      end
    end)

    # The hub stop: drop each row BEFORE terminating its instance — the
    # remove-before-terminate ordering the real stop path gives — and answer
    # the StopResult `stop_via_hub` reads.
    stub(ProcessHub, :stop_children, fn _hub, ids, _opts ->
      stopped = Enum.map(ids, fn id -> stop_and_forget(registry, id) end)

      %ProcessHub.StopResult{status: :ok, stopped: stopped, errors: []}
    end)

    stub(ProcessHub.Future, :await, fn result -> result end)
    stub(FleetManager, :runners_for_factory, fn _ns, _factory -> Enum.take(runners, 1) end)
    stub(FleetManager, :active_runners, fn _ns -> runners end)
    stub(Quotas, :check_allocation, fn _ns, _key, _n -> :ok end)
    stub(Quotas, :record_allocation, fn _ns, _key, _n -> :ok end)
    stub(Quotas, :record_deallocation, fn _ns, _key, _n -> :ok end)

    registry
  end

  @doc ~S"""
  Give one member a live extension table — the plugins a `flz plugin enable`
  attached after the start, which never reach `builder.opts[:plugins]`.
  """
  def put_live_extensions(member_id, table) do
    :persistent_term.put({:fake_live_extensions, member_id}, table)

    ExUnit.Callbacks.on_exit(fn ->
      :persistent_term.erase({:fake_live_extensions, member_id})
    end)
  end

  @doc "The builders started since the last drain, in start order."
  @spec started() :: [struct()]
  def started(acc \\ []) do
    receive do
      {:started, builder} -> started([builder | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  @doc "Register `builder` as already running — the shape a squatter takes."
  @spec insert(:ets.tid(), pid(), struct()) :: pid()
  def insert(registry, owner, builder) do
    {:ok, pid} = Instance.start(owner, builder, registry)
    :ets.insert(registry, {builder.id, pid, builder})
    pid
  end

  @doc "The live pid registered under `operative_id`."
  @spec pid_of(:ets.tid(), binary()) :: {:ok, pid()} | {:error, binary()}
  def pid_of(registry, operative_id) do
    case :ets.lookup(registry, operative_id) do
      [{^operative_id, pid, _builder}] -> {:ok, pid}
      [] -> {:error, "no pid found"}
    end
  end

  @doc "The registry rows, in the shape `HubManager.operative_rows/1` answers."
  @spec rows(:ets.tid()) :: [map()]
  def rows(registry) do
    registry
    |> :ets.tab2list()
    |> Enum.map(fn {id, _pid, builder} ->
      %{
        id: id,
        health: :healthy,
        runner: "runner-1",
        factory: builder.factory_id,
        partition: Partition.identity(builder.opts),
        node: node()
      }
    end)
  end

  # Stop the process a child id names and drop its registry row, reporting the
  # `{id, nodes}` pair a `StopResult` carries.
  defp stop_and_forget(registry, id) do
    pid = pid_of(registry, id)
    :ets.delete(registry, id)
    with {:ok, pid} <- pid, do: GenServer.stop(pid, :normal, 1_000)
    {id, [node()]}
  end

  defp insert_new(registry, owner, builder) do
    {:ok, pid} = Instance.start(owner, builder, registry)

    if :ets.insert_new(registry, {builder.id, pid, builder}) do
      {:ok, pid}
    else
      GenServer.stop(pid, :normal)
      :taken
    end
  end
end
