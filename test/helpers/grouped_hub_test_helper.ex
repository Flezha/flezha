defmodule FlezhaTest.Helper.GroupedHubTestHelper do
  @moduledoc """
  A bare ProcessHub distributing with `Flezha.Cluster.GroupedHashing`, and a
  child that carries a term across a HotSwap handover.

  The strategy names no namespace, runner or factory, so its own cluster
  scenarios are driven against a hub that holds nothing of Flezha's — only
  groups, children and nodes. Both halves live under `test/helpers/` because a
  peer BEAM can only load what is compiled to disk.
  """

  alias Flezha.Cluster.GroupedHashing
  alias Flezha.Layout.Table, as: Layout
  alias Flezha.Service.HubManager

  defmodule Child do
    @moduledoc "A child whose whole state is one term, handed over on a migration."

    use GenServer
    use ProcessHub.Strategy.Migration.HotSwap, handover: true

    # `group` rides in the spec, as a relay's factory does, for `spec_group/1`.
    def start_link(value, _group), do: GenServer.start_link(__MODULE__, value)

    @doc "The term this instance holds, asked of it wherever it runs."
    def held(pid), do: GenServer.call(pid, :held)

    @impl true
    def init({:notify, pid, value}) do
      send(pid, {:child_started, value, node()})
      {:ok, value}
    end

    def init(value), do: {:ok, value}

    @impl true
    def handle_call(:held, _from, value), do: {:reply, value, value}

    @impl true
    def prepare_handover_state(value), do: value

    @impl true
    def alter_handover_state(_value, handover), do: handover
  end

  @doc """
  Start a hub distributing by group on this node, under the hub supervisor so
  it outlives the call that asked for it.

  Its id is deliberately off the `<namespace>_hub` convention, so every
  relay-oriented walker in the controller ignores it. `:sync_interval` sets the
  PubSub sync tick, which also opens reconcile rounds; `:reconcile_interval`
  sets `:reconcile_interval_ms`, which is also the first round's hook budget.
  """
  @spec start_hub(atom(), keyword()) :: {:ok, pid()} | {:error, term()}
  def start_hub(hub_id, opts \\ []) do
    # `reconcile: true` turns on the orphan reconcile round, which is what a
    # declared child of a group nobody serves waits for.
    recovery =
      if Keyword.get(opts, :reconcile, false),
        do: [
          reconcile_grace_ms: 1_000,
          reconcile_interval_ms: Keyword.get(opts, :reconcile_interval, 1_000),
          cluster_settle_ms: 500
        ],
        else: false

    spec =
      ProcessHub.child_spec(%ProcessHub{
        hub_id: hub_id,
        distribution_strategy: %GroupedHashing{spec_group: {__MODULE__, :spec_group}},
        migration_strategy: %ProcessHub.Strategy.Migration.HotSwap{handover: true},
        redundancy_strategy: %ProcessHub.Strategy.Redundancy.Singularity{},
        synchronization_strategy:
          struct(ProcessHub.Strategy.Synchronization.PubSub, Keyword.take(opts, [:sync_interval])),
        partition_tolerance_strategy: %ProcessHub.Strategy.PartitionTolerance.Divergence{},
        registry_backend: registry_backend(hub_id, recovery),
        auto_recovery: recovery
      })

    case Supervisor.start_child(HubManager, spec) do
      {:error, {:already_started, pid}} -> {:ok, pid}
      other -> other
    end
  end

  @doc "Stop the hub on this node, leaving the node itself running, and clear its files."
  @spec stop_hub(atom()) :: :ok
  def stop_hub(hub_id) do
    Supervisor.terminate_child(HubManager, hub_id)
    Supervisor.delete_child(HubManager, hub_id)
    File.rm_rf(Path.dirname(registry_path(hub_id)))
    :ok
  end

  # ProcessHub keeps a hub's declared list beside its registry file, and an
  # `:ets` registry keeps it in memory only, so a restarted test hub would lose
  # it. Naming the registry file under this project's hub root keeps both files
  # inside `storage/app/`, where `stop_hub/1` and `flz reset` can reach them.
  defp registry_backend(_hub_id, false), do: :ets
  defp registry_backend(hub_id, _recovery), do: {:durable_ets, [path: registry_path(hub_id)]}

  defp registry_path(hub_id) do
    Path.join([Layout.dir(:process_hub), Atom.to_string(hub_id), Layout.hub_registry_file()])
  end

  @doc """
  Kill `hub_id`'s whole tree on this node, as a crash takes it: no leave is
  announced, its children die, its files stay, and the hub supervisor starts it
  afresh. Answers the pid that was killed.
  """
  @spec kill_hub(atom()) :: pid()
  def kill_hub(hub_id) do
    pid = hub_pid(hub_id)
    # Killing only the top lets its children die after the hub supervisor has
    # already tried to restart the tree under their names. So the supervisor
    # waits while every process of the tree dies at once, as in a node crash.
    :ok = :sys.suspend(HubManager)
    tree = tree_pids(pid)
    downs = Enum.map(tree, &Process.monitor/1)
    Enum.each(tree, &Process.exit(&1, :kill))
    Enum.each(downs, fn ref -> receive do: ({:DOWN, ^ref, :process, _, _} -> :ok) end)
    :ok = :sys.resume(HubManager)
    pid
  end

  defp tree_pids(sup) do
    children =
      for {_id, child, type, _modules} <- Supervisor.which_children(sup),
          is_pid(child),
          do: if(type == :supervisor, do: tree_pids(child), else: [child])

    [sup | List.flatten(children)]
  end

  @doc "The pid of `hub_id`'s tree on this node, or `nil`."
  @spec hub_pid(atom()) :: pid() | nil
  def hub_pid(hub_id) do
    Enum.find_value(Supervisor.which_children(HubManager), fn
      {^hub_id, pid, _type, _modules} when is_pid(pid) -> pid
      _child -> nil
    end)
  end

  @doc "Declare `groups` served by this node on `hub_id`."
  @spec serve(atom(), [term()]) :: :ok | {:error, :not_grouped_hashing}
  def serve(hub_id, groups), do: GroupedHashing.serve_groups(hub_id, groups)

  @doc "Withdraw `groups` from this node's declaration on `hub_id`."
  @spec unserve(atom(), [term()]) :: :ok | {:error, :not_grouped_hashing}
  def unserve(hub_id, groups), do: GroupedHashing.unserve_groups(hub_id, groups)

  @doc """
  Start `child_id` of `group` holding `value`, from this node, and answer the
  node it started on — the start result's own word, which needs no wait for the
  registry row to reach this node.
  """
  @spec start_child(atom(), term(), term(), term(), keyword()) ::
          {:ok, node()} | {:error, term()}
  def start_child(hub_id, child_id, group, value, opts \\ []) do
    result =
      hub_id
      |> ProcessHub.start_child(
        %{id: child_id, start: {Child, :start_link, [value, group]}, restart: :transient},
        awaitable: true,
        metadata: %{group: group},
        disable_logging: true,
        durable: Keyword.get(opts, :durable, false)
      )
      |> ProcessHub.Future.await()

    case result do
      %ProcessHub.StartResult{started: started, errors: errors} ->
        case Enum.find(started || [], fn {id, _} -> id == child_id end) do
          {_id, [{node, _pid} | _]} -> {:ok, node}
          _ -> {:error, errors}
        end

      other ->
        other
    end
  end

  @doc "The group a child spec started by `start_child/5` carries."
  def spec_group(%{start: {Child, :start_link, [_value, group]}}), do: group
  def spec_group(_child_spec), do: nil

  @doc """
  Write `groups` into THIS node's table as `from`'s served set, by the same
  cast a peer's announcement arrives on — so one node can be made to believe
  something the peer has not said, which is what a lost or late announcement
  leaves behind.
  """
  @spec announce_locally(atom(), node(), [term()]) :: :ok
  def announce_locally(hub_id, from, groups) do
    GenServer.cast(hub_id, {:exec_cast, {GroupedHashing, :apply_announcement, [from, groups]}})
  end

  @doc "The nodes `child_id` is registered on, as this node's registry has it."
  @spec child_nodes(atom(), term()) :: [node()]
  def child_nodes(hub_id, child_id) do
    case ProcessHub.Service.ProcessRegistry.lookup(hub_id, child_id) do
      {_child_spec, node_pids} -> Keyword.keys(node_pids)
      _ -> []
    end
  end

  @doc "What `child_id` holds, asked wherever it runs, or `nil` if it runs nowhere."
  @spec child_held(atom(), term()) :: term()
  def child_held(hub_id, child_id) do
    case ProcessHub.Service.ProcessRegistry.lookup(hub_id, child_id) do
      {_child_spec, [{_node, pid} | _]} -> Child.held(pid)
      _ -> nil
    end
  end

  @doc "The child ids this hub holds in its declared set on this node."
  @spec declared(atom()) :: [term()]
  def declared(hub_id) do
    hub_id
    |> ProcessHub.Service.DeclaredChildren.declared_children()
    |> Map.fetch!(:children)
    |> Enum.map(& &1.id)
    |> Enum.sort()
  end

  @doc "The nodes serving `group`, as this node's group table has them."
  @spec group_nodes(atom(), term()) :: [node()]
  def group_nodes(hub_id, group), do: GroupedHashing.group_nodes(hub_id, group)

  @doc "The hub's member nodes on this node, sorted."
  @spec hub_nodes(atom()) :: [node()]
  def hub_nodes(hub_id) do
    if ProcessHub.is_alive?(hub_id),
      do: hub_id |> ProcessHub.nodes([:include_local]) |> Enum.sort(),
      else: []
  end
end
