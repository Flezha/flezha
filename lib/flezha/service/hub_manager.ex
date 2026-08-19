defmodule Flezha.Service.HubManager do
  alias Flezha.Plugin
  alias Flezha.Service.Identificator
  alias ProcessHub.Service.ProcessRegistry
  alias ProcessHub.Constant.Hook
  alias ProcessHub.Service.HookManager
  alias Flezha.Service.OperativeManager
  alias Flezha.Service.RegistryUpdate
  alias Flezha.State.DurabilityTier
  alias ProcessHub.Service.DeclaredChildren

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
  # Deliberately off the `<namespace>_hub` convention: `Identificator.hub_id?/1` filters it
  # out, so every operative-oriented walker (Convergence, RecoveryGate,
  # ShutdownCoordinator, the CLI, metrics) ignores it.
  @schedule_hub_id :flz_schedules
  # Default ceiling on one `drain_hub/2`. The graceful-shutdown path passes its
  # own per-hub slice of the shutdown budget instead.
  @drain_budget_ms 60_000
  # A force pass fires AT the drain deadline and its handover is async, so this
  # window is carved out of the drain timeout to let it land.
  @forced_handover_grace_ms 2_000

  def start_link(_) do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  def stop_hub_manager() do
    Supervisor.stop(__MODULE__)
  end

  def start_hub(namespace) do
    hub_id = Identificator.hub(namespace)

    case Supervisor.start_child(__MODULE__, process_hub(namespace)) do
      {:ok, pid} = ok ->
        on_hub_started(hub_id, pid, namespace)
        ok

      {:ok, pid, _info} = ok ->
        on_hub_started(hub_id, pid, namespace)
        ok

      other ->
        other
    end
  end

  defp on_hub_started(hub_id, pid, namespace) do
    wait_until_hub_alive(hub_id)
    # Advertise placement cluster-wide; membership dies with the hub pid,
    # so stop/crash/nodedown need no leave call.
    Flezha.Cluster.Directory.join_hub(pid, namespace)
    _ = Flezha.Boot.RecoveryHooks.register(hub_id)
    # Seed the snapshot cache with the coordinator's current state. The
    # recovery telemetry fired during recovery overwrites this entry as
    # the lifecycle progresses; the seed only matters when a hub boots
    # straight into `:normal` (auto-recovery off) and no telemetry event
    # will follow.
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

  @doc "Hub id of the cron-schedule hub."
  @spec schedule_hub_id() :: atom()
  def schedule_hub_id(), do: @schedule_hub_id

  @doc """
  Start the cron-schedule hub. Idempotent: an already-started hub answers
  `{:ok, pid}` too, so a restarted owner just re-asserts.
  """
  @spec start_schedule_hub() :: {:ok, pid()} | {:error, term()}
  def start_schedule_hub() do
    case Supervisor.start_child(__MODULE__, schedule_hub()) do
      {:ok, pid} -> schedule_hub_started(pid)
      {:ok, pid, _info} -> schedule_hub_started(pid)
      {:error, {:already_started, pid}} -> schedule_hub_started(pid)
      other -> other
    end
  end

  defp schedule_hub_started(pid) do
    wait_until_hub_alive(@schedule_hub_id)
    {:ok, pid}
  end

  @doc """
  Stop the cron-schedule hub. No registry wipe: its backend is `:ets`, so it
  leaves nothing on disk.
  """
  @spec stop_schedule_hub() :: :ok
  def stop_schedule_hub() do
    Supervisor.terminate_child(__MODULE__, @schedule_hub_id)
    Supervisor.delete_child(__MODULE__, @schedule_hub_id)
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

  @doc """
  Migrate every locally-hosted operative of `namespace` to a peer controller
  through the migration-consent gate (live HotSwap handover), then wait for the
  local children to actually be gone.

  The wait is load-bearing: `Migration.drain/2` returns once the deferred list
  is empty, but the migration start requests are async and HotSwap stops a
  local child only AFTER the remote start succeeds. Locally-empty is the
  completion signal — acting on `drain/2`'s return alone kills mid-flight
  handovers. `:budget_ms` bounds the whole call; `:handover_grace_ms` is carved
  off it so the deadline force pass can land before the caller moves on.

  The one drain seam: graceful shutdown and the last-runner migration both
  reach ProcessHub through here.
  """
  @spec drain_hub(binary(), keyword()) :: {:ok, map()} | {:error, term()} | :deadline
  def drain_hub(namespace, opts \\ []) do
    budget = Keyword.get(opts, :budget_ms, @drain_budget_ms)
    grace = Keyword.get(opts, :handover_grace_ms, @forced_handover_grace_ms)
    deadline = System.monotonic_time(:millisecond) + budget

    with_live_hub(namespace, {:error, :no_target_nodes}, fn hub_id ->
      result = ProcessHub.Service.Migration.drain(hub_id, timeout: max(budget - grace, 1_000))
      with {:ok, _} <- result, do: await_local_children_gone(hub_id, deadline)
      result
    end)
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp await_local_children_gone(hub_id, deadline) do
    cond do
      ProcessHub.process_list(hub_id, :local) == [] ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        :deadline

      true ->
        Process.sleep(100)
        await_local_children_gone(hub_id, deadline)
    end
  catch
    _kind, _reason -> :ok
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
    |> Enum.map(&Identificator.namespace_from_hub/1)
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
    |> Enum.filter(&Identificator.hub_id?/1)
    |> Enum.filter(fn id -> ProcessHub.is_alive?(id) end)
  end

  @doc """
  Run `fun` with `hub_id` when that hub is alive, else return `on_dead`.

  The one guarded seam for reading ANY hub — the namespace hubs via
  `with_live_hub/3` below, the schedule hub via `Flezha.Schedule.Hub`. Every
  hub reader goes through it so none can forget the liveness check: the class
  of bug where `get_pid/2` alone omitted it and raised on a torn-down hub.
  """
  @spec with_live_hub_id(atom(), term(), (atom() -> term())) :: term()
  def with_live_hub_id(hub_id, on_dead, fun) when is_atom(hub_id) do
    if ProcessHub.is_alive?(hub_id), do: fun.(hub_id), else: on_dead
  end

  defp with_live_hub(namespace, on_dead, fun) do
    with_live_hub_id(Identificator.hub(namespace), on_dead, fun)
  end

  def get_registry(namespace) do
    with_live_hub(namespace, {:error, "hub not alive"}, fn hub_id ->
      {:ok, live_registry_dump(hub_id)}
    end)
  end

  # Every caller here means "what is currently running with a live binding":
  # unbound rows are replay residue, not runtime. Declared intent is a
  # different question — `declared_operatives/1` answers that from the hub's
  # declared set.
  defp live_registry_dump(hub_id) do
    ProcessRegistry.dump_all(hub_id, include_unbound: false, include_stopped: false)
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
  single-element `[{_node, pid}]` matches in `get_all_operatives/1` and
  `Flezha.Runner.FleetManager.runner_reroute/4` — either of which raised
  `FunctionClauseError` the moment a operative was registered on more than
  one node.
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
      |> live_registry_dump()
      |> Enum.map(fn {operative_id, entry} ->
        {cspec, node_pids, metadata} = normalize_registry_entry(entry)

        builder = cspec_operative(cspec)

        %{
          id: operative_id,
          health: Snapshot.operative_health(namespace, operative_id),
          runner: Map.get(metadata, :tag),
          factory: builder && Map.get(builder, :factory_id),
          partition: Flezha.Partition.identity(cspec_opts(cspec)),
          node: live_node(node_pids)
        }
      end)
      |> Enum.sort_by(&inspect(&1.id))
    end)
  end

  @doc """
  Every operative's declared plugins, as `{operative_id, [%Flezha.Plugin{}]}`.

  Read from the same durable registry dump `operative_rows/1` walks, so asking
  "who depends on X" costs one registry read rather than an Overwatch
  round-trip per operative. Declared, not live: a plugin the cspec records but
  whose host has not re-subscribed it yet still appears, which is what a caller
  looking for dependents wants.
  """
  @spec operative_plugins(binary() | atom()) :: [{binary(), [Flezha.Plugin.t()]}]
  def operative_plugins(namespace) do
    with_live_hub(namespace, [], fn hub_id ->
      hub_id
      |> live_registry_dump()
      |> Enum.map(fn {operative_id, entry} ->
        {cspec, _node_pids, _metadata} = normalize_registry_entry(entry)
        {operative_id, Keyword.get(cspec_opts(cspec), :plugins, [])}
      end)
      |> Enum.reject(fn {_id, plugins} -> plugins == [] end)
    end)
  end

  # The declared operative from a registry entry's cspec start-args — the same
  # `{namespace, operative}` / `{namespace, runner, operative}` arg shapes
  # `alter_child_data/2` rewrites. The durable record of everything a start
  # declared, which is why partition identity lives there.
  defp cspec_operative(%{start: {_m, _f, [arg]}}) do
    case arg do
      {_namespace, %{} = operative} -> operative
      {_namespace, _runner, %{} = operative} -> operative
      _ -> nil
    end
  end

  defp cspec_operative(_), do: nil

  # The builder opts a registry entry's cspec start-args carry, or `[]` — where
  # a partition member's identity is durable.
  defp cspec_opts(cspec) do
    case cspec_operative(cspec) do
      nil -> []
      operative -> Map.get(operative, :opts, [])
    end
  end

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

  def operative_set_runner(namespace, operative_id, runner_id) do
    with_live_hub(namespace, {:error, "hub not alive"}, fn hub_id ->
      # propagate: true — `runner_operatives/2` is a local-table tag query, so a
      # peer handling this runner's disconnect misses the operative otherwise.
      # Returned, not discarded: a "No child found" is the whole signal that a
      # binding never reached the durable row.
      result =
        ProcessRegistry.update(
          hub_id,
          operative_id,
          RegistryUpdate.set_runner_tag(runner_id),
          propagate: true
        )

      # The cspec just changed: shipping state without it would give a
      # recovering controller perfect Entities for operatives it does not know
      # exist.
      DurabilityTier.ship_cspec(hub_id)
      result
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
  def hook_reassert_binding(hub_id, %{child_id: operative_id} = hook_data) do
    local = node()
    DurabilityTier.ship_cspec(hub_id)

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
    # Dynamic supervisor for every hub: the per-namespace ones plus the
    # schedule hub. Recovery-state caching moved to
    # `Flezha.Convergence.Snapshot` (started earlier in the application tree).
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
      auto_recovery: auto_recovery_config(),
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

  # Transient uniqueness arbiter for cron schedules: `Singularity` gives one
  # schedule process cluster-wide per name. Durability belongs to
  # `Flezha.Schedule.Store`, so the registry is `:ets` and `auto_recovery` is
  # off — nothing lands under `:process_hub_registry_root`, which keeps
  # `unstarted_hub_has_registry?/0` (and thus `/healthz`) clean.
  #
  # No `child_data_alter` hook: `place_child/3` would raise looking for a
  # factory a schedule does not have. No `child_registered` hook either —
  # schedules have no runner binding to reassert.
  #
  # `ColdSwap` over the namespace hubs' `HotSwap`: HotSwap keeps the old
  # process alive until the new one registers, which for a cron timer means
  # two armed timers for one schedule. Terminate-then-start makes "at most one
  # armed timer cluster-wide" structural; the resulting gap is what the job's
  # misfire-grace look-back on init repairs.
  defp schedule_hub() do
    ProcessHub.child_spec(%ProcessHub{
      hub_id: @schedule_hub_id,
      synchronization_strategy: %ProcessHub.Strategy.Synchronization.PubSub{},
      redundancy_strategy: %ProcessHub.Strategy.Redundancy.Singularity{},
      partition_tolerance_strategy: %ProcessHub.Strategy.PartitionTolerance.Divergence{},
      distribution_strategy: %ProcessHub.Strategy.Distribution.ConsistentHashing{},
      migration_strategy: %ProcessHub.Strategy.Migration.ColdSwap{},
      registry_backend: :ets,
      auto_recovery: false,
      hooks: %{}
    })
  end

  @doc false
  def registry_backend_config(hub_id) do
    # When auto-restore is disabled (test mode + any operator who flips
    # the kill-switch), there is no point persisting the registry to
    # disk — the DETS file would only become test-isolation noise. ETS
    # gives us identical in-memory semantics with no on-disk residue.
    if durable_starts?() do
      # `:durable_ets` serves reads from ETS and mirrors writes to disk
      # synchronously, so the reconcile's per-round registry reads stay
      # in-memory while the file remains the record a cold cluster
      # recovers from. Same on-disk format as `:dets`.
      {:durable_ets, [path: registry_path(hub_id)]}
    else
      :ets
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
    durable_starts?() and
      (running_hub_operatives_unbound?() or unstarted_hub_has_registry?())
  end

  # Running hub whose durable registry declares more operatives than are
  # currently bound to a live pid — operatives replayed from DETS but not yet
  # reseated onto a reconnected runner.
  defp running_hub_operatives_unbound?() do
    Enum.any?(list_running_hub_ids(), fn hub_id ->
      namespace = Identificator.namespace_from_hub(hub_id)

      namespace != nil and durable_operative_count(hub_id) > length(get_all_operatives(namespace))
    end)
  end

  defp durable_operative_count(hub_id) do
    map_size(live_registry_dump(hub_id))
  catch
    :exit, _ -> 0
  end

  @doc """
  Whether operative starts declare durable intent — `durable: true` through
  the hub's declared set. THE auto-restore kill-switch predicate: the start
  authority, the hub's `auto_recovery` config, the registry backend, and the
  pending-restore health read all flip on it together — a hub without
  auto-recovery refuses durable starts, so they cannot diverge.

  Only an explicit `false` disables. Reading it as a strict boolean would
  crash callers on a non-boolean value; `!= false` keeps the default-on
  behaviour robust.
  """
  @spec durable_starts?() :: boolean()
  def durable_starts?() do
    Application.get_env(:flezha, :auto_restore_enabled, true) != false
  end

  @doc """
  The namespace's declared operatives, read from the hub's declared set:
  intent, not runtime. An entry exists here for exactly as long as the
  operative should exist, whether or not anything currently runs it — a wiped
  runtime registry does not empty this list, and a deliberate stop removes its
  entry for good. Parsed through the same cspec start-args shapes the registry
  readers use; an entry whose args no current clause understands still counts
  as declared, with an unknown factory.
  """
  @spec declared_operatives(binary() | atom()) ::
          [%{id: binary(), factory_id: binary() | nil}]
  def declared_operatives(namespace) do
    with_live_hub(namespace, [], fn hub_id ->
      %{children: children} = DeclaredChildren.declared_children(hub_id)

      Enum.map(children, fn child_spec ->
        operative = cspec_operative(child_spec)
        %{id: child_spec.id, factory_id: operative && Map.get(operative, :factory_id)}
      end)
    end)
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
    live_registry_dump(hub_id) |> Map.keys() |> Enum.filter(&is_binary/1)
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

  @doc false
  def auto_recovery_config do
    # Orphan reconcile: each round converges the cluster toward the hub's
    # declared set — starting declared children observed running nowhere,
    # stopping running durable children whose entry was removed. The grace is
    # also what holds the hub in `:recovering`, so it gates `RecoveryGate` and
    # `BootLoader.await_namespace_hubs` — it is set long enough for runners to
    # finish their handshake, since an orphan start needs one to place against.
    if durable_starts?() do
      [
        reconcile_grace_ms: Application.get_env(:flezha, :process_hub_reconcile_grace_ms, 45_000),
        reconcile_interval_ms:
          Application.get_env(:flezha, :process_hub_reconcile_interval_ms, 30_000),
        remote_manifest: remote_manifest_config()
      ]
    else
      false
    end
  end

  # The declared set's off-cluster copy rides the durability tier the operator
  # already configured — one remote-storage path, every tier backend
  # (`:postgres`, `:s3`, `:local_path`, custom) works unchanged. No tier, no
  # remote copy: `nil` keeps the feature local-only, exactly as before the
  # tier existed.
  defp remote_manifest_config do
    if DurabilityTier.enabled?() do
      {Flezha.State.DurabilityTier.DeclaredManifest, []}
    else
      nil
    end
  end
end
