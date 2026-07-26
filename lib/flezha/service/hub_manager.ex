defmodule Flezha.Service.HubManager do
  alias Flezha.Plugin
  alias Flezha.Service.Identificator
  alias ProcessHub.Service.ProcessRegistry
  alias ProcessHub.Constant.Hook
  alias ProcessHub.Service.HookManager
  alias Flezha.Service.OperativeManager
  alias Flezha.Service.RegistryUpdate

  require Logger

  use Supervisor

  alias Flezha.Convergence.{RecoveryGate, SafeCall, Snapshot}

  @default_await_timeout_ms 60_000
  # Bounds one whole start request: WorkerQueue kills it at this deadline and
  # `Stream.run/1` discards the exit, so an overrun silently drops the batch's
  # registry update. Every wait on the child-start path must fit inside it.
  @cross_node_request_timeout_ms 10_000
  # 20% of the request bound, leaving the rest for the operative starts the
  # bound was sized for. Overrunning it is not fatal — see `unplaceable_runner/5`.
  @runner_assign_wait_ms 2_000

  def start_link(_) do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  def stop_hub_manager() do
    Supervisor.stop(__MODULE__)
  end

  def start_hub(namespace) do
    hub_id = Identificator.hub(namespace)

    case Supervisor.start_child(__MODULE__, process_hub(namespace)) do
      {:ok, _pid} = ok ->
        on_hub_started(hub_id)
        ok

      {:ok, _pid, _info} = ok ->
        on_hub_started(hub_id)
        ok

      other ->
        other
    end
  end

  defp on_hub_started(hub_id) do
    wait_until_hub_alive(hub_id)
    _ = Flezha.Boot.RecoveryHooks.register(hub_id)
    # Seed the snapshot cache with the coordinator's current state. The
    # recovery telemetry fired during recovery overwrites this entry as
    # the lifecycle progresses; the seed only matters when a hub boots
    # straight into `:normal` (marker present + no replay) and no
    # telemetry event will follow.
    _ = Snapshot.observe_recovery(hub_id, ProcessHub.Service.Recovery.recovery_state(hub_id))
  end

  @doc """
  Last known recovery state for `hub_id`. Returns
  `{:ok, :recovery_pending | :recovering | :normal | :skipped | :timeout}`,
  or `{:error, :unknown_hub}` if the hub is not registered with this
  supervisor.
  """
  @spec recovery_state(atom()) :: {:ok, atom()} | {:error, :unknown_hub}
  def recovery_state(hub_id) when is_atom(hub_id) do
    cond do
      not ProcessHub.is_alive?(hub_id) ->
        {:error, :unknown_hub}

      true ->
        case Flezha.Convergence.Snapshot.recovery_state(hub_id) do
          :unknown -> {:ok, ProcessHub.Service.Recovery.recovery_state(hub_id)}
          state -> {:ok, state}
        end
    end
  end

  @doc """
  Block until every per-namespace hub reaches a terminal recovery state
  (`:normal | :skipped`) or `timeout_ms` (default 60s) elapses.
  Returns `:ok` on success or
  `{:error, {:recovery_timeout, [hub_id]}}` carrying the offending
  hub_ids that did not finish.
  """
  @spec await_recovery_complete(non_neg_integer()) ::
          :ok | {:error, {:recovery_timeout, [atom()]}}
  def await_recovery_complete(timeout_ms \\ @default_await_timeout_ms)
      when is_integer(timeout_ms) and timeout_ms >= 0 do
    RecoveryGate.await_recovery_complete(nil, timeout_ms)
  end

  @doc """
  Wait for the listed hubs to reach `:normal | :skipped`, with a custom
  timeout. Returns the same shape as `await_recovery_complete/1`.
  """
  @spec await_recovery_complete([atom()], non_neg_integer()) ::
          :ok | {:error, {:recovery_timeout, [atom()]}}
  def await_recovery_complete(hub_ids, timeout_ms)
      when is_list(hub_ids) and is_integer(timeout_ms) do
    RecoveryGate.await_recovery_complete(hub_ids, timeout_ms)
  end

  # Supervisor.start_child returns once the hub's top supervisor is up,
  # but ProcessHub registers its named coordinator GenServer asynchronously
  # afterwards. Anything calling into the named hub_id (register_handler,
  # register_hook_handlers, alter_child_data callbacks) before that
  # registration completes raises :noproc and cascades the FleetManager
  # crash. Spin briefly until ProcessHub.is_alive? confirms the name is
  # claimed.
  defp wait_until_hub_alive(hub_id, deadline_ms \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + deadline_ms
    do_wait_until_hub_alive(hub_id, deadline)
  end

  defp do_wait_until_hub_alive(hub_id, deadline) do
    cond do
      ProcessHub.is_alive?(hub_id) ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        :timeout

      true ->
        Process.sleep(10)
        do_wait_until_hub_alive(hub_id, deadline)
    end
  end

  def stop_hub(namespace) do
    hub_id = Identificator.hub(namespace)
    Supervisor.terminate_child(__MODULE__, hub_id)
    Supervisor.delete_child(__MODULE__, hub_id)

    # `stop_hub/1` is the explicit "tear this namespace down" path
    # (operator-initiated, or last-runner cleanup deciding the
    # namespace has gone away). Clean the persistent registry so a
    # later `start_hub/1` for the same namespace starts empty rather
    # than resurrecting children that were intentionally torn down.
    # Cold-boot recovery is unaffected — that path never calls
    # `stop_hub/1`.
    _ = wipe_registry_dir(hub_id)
    :ok
  end

  @doc """
  Reclaim a namespace hub, but only when it holds no registered operatives.
  Tearing down a hub that still holds durably-suspended operatives would kill
  their Overwatches and wipe the durable registry, stranding a parked fleet
  across a runner restart — so runner cleanup and cross-node migration reclaim
  through this guard. The explicit `stop_hub/1` teardown (operator
  `stop_namespace`) is intentionally unguarded.
  """
  def stop_hub_if_vacant(namespace) do
    unless registered_operatives?(namespace), do: stop_hub(namespace)
    :ok
  end

  defp wipe_registry_dir(hub_id) do
    root = Application.get_env(:flezha, :process_hub_registry_root, "app_data/process_hub")
    File.rm_rf(Path.join(root, Atom.to_string(hub_id)))
  end

  def hub_alive?(namespace) do
    ProcessHub.is_alive?(Identificator.hub(namespace))
  end

  def get_running_namespaces() do
    list_running_hub_ids()
    |> Enum.map(&extract_namespace_from_hub/1)
    |> Enum.reject(&is_nil/1)
  end

  @doc """
  All hub_ids currently registered with this supervisor that have a live
  coordinator. Filters non-hub children by the
  `Identificator.hub/1`-shaped atom suffix and gating on
  `ProcessHub.is_alive?/1`. Used by `RecoveryGate.await_recovery_complete/2`
  when the caller does not pass an explicit hub-id list.
  """
  @spec list_running_hub_ids() :: [atom()]
  def list_running_hub_ids() do
    Supervisor.which_children(__MODULE__)
    |> Enum.map(fn {id, _pid, _type, _modules} -> id end)
    |> Enum.filter(&hub_id?/1)
    |> Enum.filter(fn id -> ProcessHub.is_alive?(id) end)
  end

  defp hub_id?(id) when is_atom(id) do
    String.ends_with?(Atom.to_string(id), "_hub")
  end

  defp hub_id?(_), do: false

  defp extract_namespace_from_hub(hub_id) when is_atom(hub_id) do
    hub_id
    |> Atom.to_string()
    |> String.replace_suffix("_hub", "")
  end

  defp extract_namespace_from_hub(_), do: nil

  # The one guarded seam for reading a namespace's hub: run `fun` with the
  # resolved hub_id when the hub is alive, else return `on_dead`. Every hub
  # reader goes through this so none can forget the liveness check — the class
  # of bug where `get_pid/2` alone omitted it and raised on a torn-down hub.
  defp with_live_hub(namespace, on_dead, fun) do
    hub_id = Identificator.hub(namespace)
    if ProcessHub.is_alive?(hub_id), do: fun.(hub_id), else: on_dead
  end

  def get_registry(namespace) do
    with_live_hub(namespace, {:error, "hub not alive"}, fn hub_id ->
      {:ok, ProcessHub.registry_dump(hub_id)}
    end)
  end

  def get_hub_nodes(namespace, include_local \\ false) do
    opts = if include_local, do: [:include_local], else: []
    with_live_hub(namespace, [], fn hub_id -> ProcessHub.nodes(hub_id, opts) end)
  end

  def get_pid(namespace, operative_id) do
    with_live_hub(namespace, {:error, "hub not alive"}, fn hub_id ->
      case ProcessHub.get_pids(hub_id, operative_id) do
        [] ->
          {:error, "no pid found"}

        pids ->
          {:ok, List.first(pids)}
      end
    end)
  end

  @doc """
  Resolve a operative's runnable pid from a ProcessHub `node_pids` list
  (`[{node, pid}]`, with 0, 1, or many entries).

  Drops entries on nodes absent from the live cluster, prefers the local node's
  pid (verified alive), and otherwise returns a pid on another live node.
  Returns `nil` when no live pid remains.

  This centralises the multi-node-safe access that replaced the former
  single-element `[{_node, pid}]` matches in `get_all_operatives/1`,
  `runner_switch/3`, and `Flezha.Runner.FleetManager.runner_reroute/4` —
  any of which raised `FunctionClauseError` the moment a operative was registered on
  more than one node.
  """
  @spec live_pid([{node(), pid()}]) :: pid() | nil
  def live_pid(node_pids) when is_list(node_pids) do
    local = node()
    live_nodes = [local | Node.list()]
    on_live_node = Enum.filter(node_pids, fn {n, _pid} -> n in live_nodes end)

    local_alive =
      Enum.find_value(on_live_node, fn
        {^local, pid} -> if Process.alive?(pid), do: pid, else: nil
        _ -> nil
      end)

    cond do
      is_pid(local_alive) ->
        local_alive

      true ->
        case Enum.find(on_live_node, fn {n, _pid} -> n != local end) do
          {_n, pid} -> pid
          nil -> nil
        end
    end
  end

  def live_pid(_), do: nil

  def get_all_operatives(namespace) do
    with_live_hub(namespace, [], fn hub_id ->
      ProcessHub.process_list(hub_id, :global)
      |> Enum.flat_map(fn {operative_id, node_pids} ->
        case live_pid(node_pids) do
          nil -> []
          operative_pid -> [{operative_id, operative_pid}]
        end
      end)
    end)
  end

  @doc """
  The live node hosting a operative from its `node_pids` (`[{node, pid}]`): the
  local node when it holds the runnable pid, else another live cluster node,
  else `nil` (every entry is on a dead node — a FAILED placement). Sibling of
  `live_pid/1` for surfacing per-node placement in the CLI.
  """
  @spec live_node([{node(), pid()}]) :: node() | nil
  def live_node(node_pids) when is_list(node_pids) do
    local = node()
    live_nodes = [local | Node.list()]
    on_live_node = Enum.filter(node_pids, fn {n, _pid} -> n in live_nodes end)

    cond do
      Enum.any?(on_live_node, fn {n, pid} -> n == local and Process.alive?(pid) end) ->
        local

      true ->
        case Enum.find(on_live_node, fn {n, _pid} -> n != local end) do
          {n, _pid} -> n
          nil -> nil
        end
    end
  end

  def live_node(_), do: nil

  @doc """
  Per-operative view of a namespace's durable registry for operator surfaces:
  one row per registered operative (including FAILED ones whose only placement is
  a dead node, which `get_all_operatives/1` filters out), carrying the assigned
  runner, the declared factory (from the durable cspec start-args) and live node
  from the registry dump and the live health classified
  by the Convergence Loop (`Snapshot.operative_health/2`), so the CLI sees exactly
  what the loop sees. A registry row the loop has not yet observed reports
  `:unknown` until the next tick covers it.
  """
  @spec operative_rows(String.t()) :: [
          %{id: term(), health: atom(), runner: term(), factory: term(), node: term()}
        ]
  def operative_rows(namespace) do
    with_live_hub(namespace, [], fn hub_id ->
      hub_id
      |> ProcessRegistry.dump()
      |> Enum.map(fn {operative_id, entry} ->
        {cspec, node_pids, metadata} = normalize_registry_entry(entry)

        %{
          id: operative_id,
          health: Snapshot.operative_health(namespace, operative_id),
          runner: Map.get(metadata, :tag),
          factory: cspec_factory_id(cspec),
          node: live_node(node_pids)
        }
      end)
      |> Enum.sort_by(&inspect(&1.id))
    end)
  end

  # The declared factory from a registry entry's cspec start-args — the same
  # `{namespace, operative}` / `{namespace, runner, operative}` arg shapes
  # `alter_child_data/2` rewrites.
  defp cspec_factory_id(%{start: {_m, _f, [arg]}}) do
    case arg do
      {_namespace, %{} = operative} -> Map.get(operative, :factory_id)
      {_namespace, _runner, %{} = operative} -> Map.get(operative, :factory_id)
      _ -> nil
    end
  end

  defp cspec_factory_id(_), do: nil

  defp normalize_registry_entry({cspec, node_pids, metadata}), do: {cspec, node_pids, metadata}
  defp normalize_registry_entry({cspec, node_pids}), do: {cspec, node_pids, %{}}

  def register_handler(namespace, runner_id) do
    ProcessHub.register_hook_handlers(
      Identificator.hub(namespace),
      Hook.child_registered(),
      [
        %HookManager{
          id: runner_id <> "_hub_pid_insertion",
          m: Flezha.Service.HubManager,
          f: :hook_insert_pid,
          a: [runner_id, :_]
        }
      ]
    )
  end

  # Live stream-channel children (primary + secondary) for a runner; `[]`
  # if the supervisors are gone.
  defp stream_channels_for_runner(runner_id) do
    Supervisor.which_children(Identificator.stream_sup_primary(runner_id)) ++
      Supervisor.which_children(Identificator.stream_sup_secondary(runner_id))
  catch
    :exit, _ -> []
  end

  def unregister_handler(namespace, runner_id) do
    ProcessHub.cancel_hook_handlers(
      Identificator.hub(namespace),
      Hook.child_registered(),
      [runner_id <> "_hub_pid_insertion"]
    )
  end

  def runner_operatives(namespace, runner_id) do
    hub_id = Identificator.hub(namespace)

    ProcessHub.tag_query(hub_id, runner_id)
  end

  def runner_switch(namespace, old_runner_id, new_runner_id) do
    hub_id = Identificator.hub(namespace)

    # Updates the runner id in the registry. Only the operative_id is needed here,
    # so accept any node_pids arity (a multi-node operative must not crash this).
    runner_operatives(namespace, old_runner_id)
    |> Enum.each(fn {operative_id, _node_pids} ->
      operative_set_runner(namespace, operative_id, new_runner_id)
    end)

    # Propagate the change to all nodes in the namespace.
    hub_id
    |> ProcessHub.Coordinator.get_hub()
    |> ProcessHub.Service.Synchronizer.trigger_sync()
  end

  def operative_set_runner(namespace, operative_id, runner_id) do
    with_live_hub(namespace, {:error, "hub not alive"}, fn hub_id ->
      # propagate: true — `runner_operatives/2` is a local-table tag query, so a
      # peer handling this runner's disconnect misses the operative otherwise.
      # Returned, not discarded: a "No child found" is the whole signal that a
      # binding never reached the durable row.
      ProcessRegistry.update(
        hub_id,
        operative_id,
        RegistryUpdate.set_runner_tag(runner_id),
        propagate: true
      )
    end)
  end

  # The route is operative_id -> local Overwatch pid, so seeding it into a
  # non-owning runner's channels is inert: any runner's channel resolves the
  # same process, and only the owner ever emits frames for this operative.
  def hook_insert_pid(runner_id, %{child_id: operative_id} = hook_data) do
    curr_node = node()

    updated_pids =
      Map.get(hook_data, :node_pids, [])
      |> Enum.filter(fn {node, _pid} -> node === curr_node end)
      |> Enum.map(fn {_, pid} -> pid end)

    case updated_pids do
      [] ->
        nil

      _ ->
        # A wedged or not-yet-started StreamChannel must not crash the caller:
        # this runs inside the `child_registered` hook, where an exit aborts
        # the registry write that dispatched it.
        Enum.each(stream_channels_for_runner(runner_id), fn
          {_, pid, _, _} when is_pid(pid) ->
            _ = SafeCall.bounded(pid, {:operative_pid_update, operative_id, updated_pids}, 1_000)

          _ ->
            :ok
        end)
    end
  end

  @doc """
  Hub-wide `child_registered` handler: tells each local Overwatch that its
  durable row was just rewritten.

  A start request writes the row after its children are already running, so the
  `metadata.tag` it carries can name a runner an Overwatch has since rebound
  away from. Dispatching from inside the write orders the re-assert after it
  instead of racing it.
  """
  def hook_reassert_binding(_hub_id, %{child_id: operative_id} = hook_data) do
    local = node()

    Enum.each(Map.get(hook_data, :node_pids, []), fn
      {^local, pid} when is_pid(pid) -> send(pid, {:reassert_binding, operative_id})
      _ -> :ok
    end)
  end

  @doc "The owning runner recorded on `operative_id`'s durable row, or `nil`."
  def operative_runner_tag(namespace, operative_id) do
    with_live_hub(namespace, nil, fn hub_id ->
      case ProcessRegistry.lookup(hub_id, operative_id, with_metadata: true) do
        {_cspec, _node_pids, metadata} when is_map(metadata) -> Map.get(metadata, :tag)
        _ -> nil
      end
    end)
  end

  @doc """
  Convenience over `hook_insert_pid/2` for the common case: seed one operative's
  local `pid` into `runner_id`'s pre-spawned StreamChannels so data-plane frames
  route to it. Used by the convergence rebind, the reconnect binding swap, and
  the Overwatch's own bind.
  """
  def hook_insert_local_pid(runner_id, operative_id, pid) when is_pid(pid) do
    hook_insert_pid(runner_id, %{child_id: operative_id, node_pids: [{node(), pid}]})
  end

  @doc """
  Clear a operative's pid mapping from every stream channel of `runner_id`.
  Invoked directly by `Flezha.Runner.FleetManager.evict_runner/1` so a
  dead runner's stale mappings never linger; idempotent on an
  already-gone runner.
  """
  def hook_remove_pid(runner_id, %{child_id: operative_id}) do
    Enum.each(stream_channels_for_runner(runner_id), fn
      {_, pid, _, _} when is_pid(pid) ->
        _ = SafeCall.bounded(pid, {:operative_pid_remove, operative_id}, 1_000)

      _ ->
        :ok
    end)

    :telemetry.execute(
      [:flezha, :hub, :pid_removed],
      %{count: 1},
      %{runner_id: runner_id, operative_id: operative_id}
    )

    :ok
  end

  @doc """
  Read the current plugin list embedded in a operative's child-spec
  start args. Returns `{:ok, [{plugin_id, %Flezha.Plugin{}}]}` (entries
  are normalised to tuple form regardless of how they are stored).
  """
  @spec operative_plugins(String.t(), String.t()) ::
          {:ok, [{binary(), Plugin.t()}]} | {:error, term()}
  def operative_plugins(namespace, operative_id) do
    with_live_hub(namespace, {:error, "hub not alive"}, fn hub_id ->
      case ProcessRegistry.lookup(hub_id, operative_id) do
        nil ->
          {:error, "operative not found"}

        {%{start: {_m, _f, [arg]}}, _node_pids} ->
          {:ok, normalize_plugins(extract_plugins(arg))}

        {%{start: {_m, _f, [arg]}}, _node_pids, _metadata} ->
          {:ok, normalize_plugins(extract_plugins(arg))}
      end
    end)
  end

  @doc """
  Apply `update_fn` to the plugin list embedded in a child-spec's
  start-args and return the rewritten cspec. Used by
  `Flezha.Service.PluginManager` when it issues a registry-write-first
  cspec update through `ProcessHub.Service.ProcessRegistry.update/3`.
  """
  @spec rewrite_cspec_plugins(map(), ([{binary(), Plugin.t()}] -> [{binary(), Plugin.t()}])) ::
          map()
  def rewrite_cspec_plugins(%{start: {m, f, [arg]}} = cspec, update_fn) do
    new_arg =
      case arg do
        {namespace, operative} ->
          {namespace, rewrite_operative_plugins(operative, update_fn)}

        {namespace, runner_id, operative} ->
          {namespace, runner_id, rewrite_operative_plugins(operative, update_fn)}
      end

    Map.put(cspec, :start, {m, f, [new_arg]})
  end

  defp rewrite_operative_plugins(operative, update_fn) do
    current = normalize_plugins(extract_plugins({nil, operative}))
    new_plugins = update_fn.(current)
    new_opts = Keyword.put(operative.opts, :plugins, new_plugins)
    %{operative | opts: new_opts}
  end

  defp extract_plugins({_namespace, operative}),
    do: Keyword.get(operative.opts, :plugins, [])

  defp extract_plugins({_namespace, _runner_id, operative}),
    do: Keyword.get(operative.opts, :plugins, [])

  defp normalize_plugins(plugins) when is_list(plugins) do
    Enum.map(plugins, fn
      {id, %Plugin{} = plugin} -> {id, plugin}
      %Plugin{id: id} = plugin -> {id, plugin}
    end)
  end

  def alter_child_data(_hub_id, %{child_spec: cs} = child_data) do
    case start_args(cs) do
      {:ok, parsed} -> place_child(child_data, cs, parsed)
      :error -> unparsable_child(child_data, cs)
    end
  end

  defp start_args(%{start: {_m, _f, [{namespace, %{} = operative}]}}),
    do: {:ok, {namespace, nil, operative}}

  defp start_args(%{start: {_m, _f, [{namespace, recorded_runner, %{} = operative}]}}),
    do: {:ok, {namespace, recorded_runner, operative}}

  defp start_args(_), do: :error

  # Failing this one child in `start_child` beats raising, which would kill the
  # request before `update_registry` and strand every sibling row in the batch.
  defp unparsable_child(child_data, cs) do
    Logger.error(
      "[hub_manager] unrecognised start args for child #{inspect(Map.get(cs, :id))}; " <>
        "leaving it unaltered: #{inspect(Map.get(cs, :start))}"
    )

    child_data
  end

  defp place_child(child_data, %{start: {m, f, _args}}, {namespace, recorded_runner, operative}) do
    operative_id = Map.get(operative, :id)
    factory_id = Map.get(operative, :factory_id)

    # Recovery replay races the `FleetManager.runner_connect/3` that triggered
    # it: the connecting runner is not placeable until that handler returns, so
    # the first `nil` here is not final.
    assigned_runner =
      case OperativeManager.await_runner(
             namespace,
             factory_id,
             operative_id,
             batch_runner_wait_ms(),
             &OperativeManager.runner_registered?/1
           ) do
        {:ok, runner_id} ->
          runner_id

        {:error, reason} ->
          unplaceable_runner(recorded_runner, namespace, factory_id, operative_id, reason)
      end

    Logger.info(
      "[hub_manager] alter_child_data operative=#{inspect(operative_id)} " <>
        "factory=#{inspect(factory_id)} assigned_ce=#{inspect(assigned_runner)}"
    )

    new_args = [{namespace, assigned_runner, operative}]

    child_data
    |> put_in([:child_spec, :start], {m, f, new_args})
    |> put_in([:metadata, :tag], assigned_runner)
  end

  # A first start has no recorded runner and a caller waiting on the result, so
  # it still fails loudly.
  defp unplaceable_runner(nil, namespace, factory_id, _operative_id, reason) do
    raise "No active compute engines available in namespace #{namespace} " <>
            "for factory #{inspect(factory_id)} (#{reason})"
  end

  # A replayed cspec already names the runner it was bound to. Keeping it starts
  # the operative, so its registry row gets a live pid and both Overwatch's own
  # bounded wait and the convergence loop can still rebind it. Raising here
  # instead kills the whole start request before `update_registry`, stranding
  # every row in the batch on a dead pid where no reconciler can see it.
  defp unplaceable_runner(recorded_runner, namespace, _factory_id, operative_id, reason) do
    Logger.warning(
      "[hub_manager] no placeable runner for #{operative_id} in #{namespace} (#{reason}); " <>
        "starting on recorded runner #{inspect(recorded_runner)} pending rebind"
    )

    recorded_runner
  end

  @batch_runner_wait_key :flezha_alter_child_data_deadline

  # One start request alters every child in sequence and runs `update_registry`
  # only after the last one, so the wait is shared across the batch: a per-child
  # budget would multiply past `cross_node_request_timeout` and the request
  # would be killed with its registry update undone.
  #
  # The process IS the request: `StartChildrenRequest.start_children/3` is the
  # sole dispatch site, under `WorkerQueue`'s `Task.async_stream`, which gives a
  # fresh unpooled process per request that dies with it. That is what scopes
  # the deadline — no wall-clock guard, which could only ever hand a second
  # request the first one's spent budget.
  defp batch_runner_wait_ms do
    now = System.monotonic_time(:millisecond)

    deadline =
      Process.get(@batch_runner_wait_key) ||
        now + Application.get_env(:flezha, :hub_runner_assign_wait_ms, @runner_assign_wait_ms)

    Process.put(@batch_runner_wait_key, deadline)
    max(deadline - now, 0)
  end

  @impl true
  def init(_init_arg) do
    # Dynamic supervisor for the per-namespace hubs; recovery-state
    # caching moved to `Flezha.Convergence.Snapshot` (started earlier in
    # the application tree).
    Supervisor.init([], strategy: :one_for_one)
  end

  defp process_hub(namespace) do
    hub_id = Identificator.hub(namespace)

    ProcessHub.child_spec(%ProcessHub{
      hub_id: hub_id,
      synchronization_strategy: %ProcessHub.Strategy.Synchronization.PubSub{},
      redundancy_strategy: %ProcessHub.Strategy.Redundancy.Singularity{},
      partition_tolerance_strategy: %ProcessHub.Strategy.PartitionTolerance.Divergence{},
      distribution_strategy: %ProcessHub.Strategy.Distribution.ConsistentHashing{},
      migration_strategy: migration_strategy(),
      registry_backend: registry_backend_config(hub_id),
      auto_recovery: auto_recovery_config(hub_id),
      # Operative startup involves multiple GenServer hops and socket communication
      # to the compute engine (Rust), so the default 5s is too tight.
      cross_node_request_timeout: @cross_node_request_timeout_ms,
      hooks:
        Map.merge(
          %{
            Hook.child_data_alter() => [
              %HookManager{
                id: ProcessHub.Constant.Hook.child_data_alter(),
                m: __MODULE__,
                f: :alter_child_data,
                a: [hub_id, :_]
              }
            ],
            Hook.child_registered() => [
              %HookManager{
                id: :flezha_binding_reassert,
                m: __MODULE__,
                f: :hook_reassert_binding,
                a: [hub_id, :_]
              }
            ]
          },
          Snapshot.hook_handlers(hub_id)
        )
    })
  end

  @doc false
  def registry_backend_config(hub_id) do
    # When auto-restore is disabled (test mode + any operator who flips
    # the kill-switch), there is no point persisting the registry to
    # disk — the DETS file would only become test-isolation noise. ETS
    # gives us identical in-memory semantics with no on-disk residue.
    case Application.get_env(:flezha, :auto_restore_enabled, true) do
      false -> :ets
      _ -> {:dets, [path: registry_path(hub_id)]}
    end
  end

  @doc false
  def migration_strategy do
    %ProcessHub.Strategy.Migration.HotSwap{
      handover: true,
      consent_settings: consent_settings()
    }
  end

  # `max_defer_ms` nil = derive from the shutdown budget, so a deferring
  # operative can never outlive the graceful-shutdown window.
  @doc false
  def consent_settings do
    opts = Application.get_env(:flezha, :migration_consent, [])

    %ProcessHub.Strategy.Migration.MigrationConsent{
      consent_timeout: Keyword.get(opts, :consent_timeout_ms, 5_000),
      retry_interval: Keyword.get(opts, :retry_interval_ms, 2_000),
      max_defer_time:
        Keyword.get(opts, :max_defer_ms) || Flezha.Shutdown.Coordinator.configured_timeout_ms()
    }
  end

  defp registry_path(hub_id) do
    root = Application.get_env(:flezha, :process_hub_registry_root, "app_data/process_hub")
    dir = Path.join(root, Atom.to_string(hub_id))
    File.mkdir_p!(dir)
    Path.join(dir, "registry.dets")
  end

  @doc """
  True when auto-restore is enabled and the durable registry holds operatives
  this (re)started controller has not loaded back into live, bound
  processes yet — closing the post-restart "vacuously healthy" hole.
  Covers both an on-disk `registry.dets` for a hub that has not started
  (`unstarted_hub_has_registry?/0`) and a running hub whose replayed DETS
  rows outnumber the live-bound operatives (`running_hub_operatives_unbound?/0`).
  Used by `/healthz` to report `recovering` instead of `healthy` until the
  registry is fully reloaded and bound.
  """
  @spec durable_registry_pending?() :: boolean()
  def durable_registry_pending?() do
    # Kill-switch semantics: only an explicit `false` disables auto-restore.
    # Reading it as a strict boolean would crash the convergence Loop tick on
    # a non-boolean value; `!= false` keeps the default-on behaviour robust.
    Application.get_env(:flezha, :auto_restore_enabled, true) != false and
      (running_hub_operatives_unbound?() or unstarted_hub_has_registry?())
  end

  # Running hub whose durable registry declares more operatives than are
  # currently bound to a live pid — operatives replayed from DETS but not yet
  # reseated onto a reconnected runner.
  defp running_hub_operatives_unbound?() do
    Enum.any?(list_running_hub_ids(), fn hub_id ->
      namespace = extract_namespace_from_hub(hub_id)

      namespace != nil and durable_operative_count(hub_id) > length(get_all_operatives(namespace))
    end)
  end

  defp durable_operative_count(hub_id) do
    map_size(ProcessRegistry.dump(hub_id))
  catch
    :exit, _ -> 0
  end

  @doc """
  Operative ids present in the durable registries of running hubs — durable
  evidence of intent consumed by the convergence intent-mismatch check.
  """
  @spec durable_operative_ids() :: MapSet.t()
  def durable_operative_ids() do
    list_running_hub_ids()
    |> Enum.flat_map(&durable_operative_keys/1)
    |> MapSet.new()
  end

  defp durable_operative_keys(hub_id) do
    ProcessRegistry.dump(hub_id) |> Map.keys() |> Enum.filter(&is_binary/1)
  catch
    :exit, _ -> []
  end

  @doc """
  True when the namespace's hub still holds registered operatives (running or
  durably suspended). Runner cleanup / migration consult this so a hub is never
  torn down while it holds parked operatives that must stay addressable.
  """
  def registered_operatives?(namespace) do
    Identificator.hub(namespace) |> durable_operative_keys() != []
  end

  # On-disk registry.dets for a hub that has not started yet.
  defp unstarted_hub_has_registry?() do
    running = MapSet.new(list_running_hub_ids())
    root = Application.get_env(:flezha, :process_hub_registry_root, "app_data/process_hub")

    case File.ls(root) do
      {:ok, entries} ->
        Enum.any?(entries, fn entry ->
          hub_id = String.to_atom(entry)

          not MapSet.member?(running, hub_id) and
            File.regular?(Path.join([root, entry, "registry.dets"]))
        end)

      _ ->
        false
    end
  end

  defp auto_recovery_config(hub_id) do
    case Application.get_env(:flezha, :auto_restore_enabled, true) do
      false ->
        false

      _ ->
        [
          marker_path: marker_path(hub_id),
          recovery_timeout_ms:
            Application.get_env(:flezha, :process_hub_recovery_timeout_ms, 30_000)
        ]
    end
  end

  defp marker_path(hub_id) do
    case Application.get_env(:flezha, :process_hub_recovery_marker_path) do
      nil ->
        root = Application.get_env(:flezha, :process_hub_registry_root, "app_data/process_hub")
        Path.join([root, Atom.to_string(hub_id), "cluster.healthy"])

      override when is_binary(override) ->
        override
    end
  end

  @doc false
  # Public for the CLI `flz cluster status --recovery` surface. Resolves the
  # same marker path the hub spec uses, so an operator-visible check matches
  # the live state.
  @spec resolved_marker_path(atom()) :: binary()
  def resolved_marker_path(hub_id), do: marker_path(hub_id)
end
