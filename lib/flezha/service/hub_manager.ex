defmodule Flezha.Service.HubManager do
  alias Flezha.Plugin
  alias Flezha.Service.Identificator
  alias ProcessHub.Service.ProcessRegistry
  alias ProcessHub.Constant.Hook
  alias ProcessHub.Service.HookManager
  alias Flezha.Service.RelayManager

  require Logger

  use Supervisor

  alias Flezha.Convergence.{Probe, SafeCall, Snapshot}

  @default_await_timeout_ms 60_000

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
    _ = Snapshot.observe_recovery(hub_id, ProcessHub.recovery_state(hub_id))
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
        case Probe.recovery_state(hub_id) do
          :unknown -> {:ok, ProcessHub.recovery_state(hub_id)}
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
    Probe.await_recovery_complete(nil, timeout_ms)
  end

  @doc """
  Wait for the listed hubs to reach `:normal | :skipped`, with a custom
  timeout. Returns the same shape as `await_recovery_complete/1`.
  """
  @spec await_recovery_complete([atom()], non_neg_integer()) ::
          :ok | {:error, {:recovery_timeout, [atom()]}}
  def await_recovery_complete(hub_ids, timeout_ms)
      when is_list(hub_ids) and is_integer(timeout_ms) do
    Probe.await_recovery_complete(hub_ids, timeout_ms)
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
    # (operator-initiated, or last-executor cleanup deciding the
    # namespace has gone away). Clean the persistent registry so a
    # later `start_hub/1` for the same namespace starts empty rather
    # than resurrecting children that were intentionally torn down.
    # Cold-boot recovery is unaffected — that path never calls
    # `stop_hub/1`.
    _ = wipe_registry_dir(hub_id)
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
  `ProcessHub.is_alive?/1`. Used by `Probe.await_recovery_complete/2`
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

  def get_registry(namespace) do
    hub_id = Identificator.hub(namespace)

    case ProcessHub.is_alive?(hub_id) do
      true ->
        {:ok, ProcessHub.registry_dump(hub_id)}

      false ->
        {:error, "hub not alive"}
    end
  end

  def get_hub_nodes(namespace, include_local \\ false) do
    opts =
      case include_local do
        true ->
          [:include_local]

        false ->
          []
      end

    hub_id = Identificator.hub(namespace)

    if ProcessHub.is_alive?(hub_id) do
      ProcessHub.nodes(hub_id, opts)
    else
      []
    end
  end

  def get_pid(namespace, relay_id) do
    hub_id = Identificator.hub(namespace)

    case ProcessHub.get_pids(hub_id, relay_id) do
      [] ->
        {:error, "no pid found"}

      pids ->
        {:ok, List.first(pids)}
    end
  end

  @doc """
  Resolve a relay's runnable pid from a ProcessHub `node_pids` list
  (`[{node, pid}]`, with 0, 1, or many entries).

  Drops entries on nodes absent from the live cluster, prefers the local node's
  pid (verified alive), and otherwise returns a pid on another live node.
  Returns `nil` when no live pid remains.

  This centralises the multi-node-safe access that replaced the former
  single-element `[{_node, pid}]` matches in `get_all_relays/1`,
  `executor_switch/3`, and `Flezha.Executor.FleetManager.executor_reroute/4` —
  any of which raised `FunctionClauseError` the moment a relay was registered on
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

  def get_all_relays(namespace) do
    hub_id = Identificator.hub(namespace)

    if ProcessHub.is_alive?(hub_id) do
      ProcessHub.process_list(hub_id, :global)
      |> Enum.flat_map(fn {relay_id, node_pids} ->
        case live_pid(node_pids) do
          nil -> []
          relay_pid -> [{relay_id, relay_pid}]
        end
      end)
    else
      []
    end
  end

  @doc """
  The live node hosting a relay from its `node_pids` (`[{node, pid}]`): the
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
  Per-relay view of a namespace's durable registry for operator surfaces:
  one row per registered relay (including FAILED ones whose only placement is
  a dead node, which `get_all_relays/1` filters out), carrying the reconciler's
  persisted health, the assigned executor, and the live node. Reads the same
  registry dump the reconciler does, so the CLI sees exactly what the loop sees.
  """
  @spec relay_rows(String.t()) :: [%{id: term(), health: atom(), executor: term(), node: term()}]
  def relay_rows(namespace) do
    hub_id = Identificator.hub(namespace)

    if ProcessHub.is_alive?(hub_id) do
      hub_id
      |> ProcessRegistry.dump()
      |> Enum.map(fn {relay_id, entry} ->
        {_cspec, node_pids, metadata} = normalize_registry_entry(entry)

        %{
          id: relay_id,
          health: Map.get(metadata, :health, :unknown),
          executor: Map.get(metadata, :tag),
          node: live_node(node_pids)
        }
      end)
      |> Enum.sort_by(&inspect(&1.id))
    else
      []
    end
  end

  defp normalize_registry_entry({cspec, node_pids, metadata}), do: {cspec, node_pids, metadata}
  defp normalize_registry_entry({cspec, node_pids}), do: {cspec, node_pids, %{}}

  def register_handler(namespace, executor_id) do
    ProcessHub.register_hook_handlers(
      Identificator.hub(namespace),
      Hook.child_registered(),
      [
        %HookManager{
          id: executor_id <> "_hub_pid_insertion",
          m: Flezha.Service.HubManager,
          f: :hook_insert_pid,
          a: [executor_id, :_]
        }
      ]
    )

    ProcessHub.register_hook_handlers(
      Identificator.hub(namespace),
      Hook.child_registered(),
      [
        %HookManager{
          id: executor_id <> "_hub_pid_removal",
          m: Flezha.Service.HubManager,
          f: :hook_remove_pid,
          a: [executor_id, :_]
        }
      ]
    )
  end

  def unregister_handler(namespace, executor_id) do
    ProcessHub.cancel_hook_handlers(
      Identificator.hub(namespace),
      Hook.child_registered(),
      [executor_id <> "_hub_pid_insertion"]
    )
  end

  def executor_relays(namespace, executor_id) do
    hub_id = Identificator.hub(namespace)

    ProcessHub.tag_query(hub_id, executor_id)
  end

  def executor_switch(namespace, old_executor_id, new_executor_id) do
    hub_id = Identificator.hub(namespace)

    # Updates the executor id in the registry. Only the relay_id is needed here,
    # so accept any node_pids arity (a multi-node relay must not crash this).
    executor_relays(namespace, old_executor_id)
    |> Enum.each(fn {relay_id, _node_pids} ->
      relay_set_executor(namespace, relay_id, new_executor_id)
    end)

    # Propagate the change to all nodes in the namespace.
    hub_id
    |> ProcessHub.Coordinator.get_hub()
    |> ProcessHub.Service.Synchronizer.trigger_sync()
  end

  def relay_set_executor(namespace, relay_id, executor_id) do
    hub_id = Identificator.hub(namespace)

    case ProcessHub.is_alive?(hub_id) do
      true ->
        ProcessRegistry.update(hub_id, relay_id, fn cspec, node_pids, metadata ->
          {cspec, node_pids, Map.put(metadata, :tag, executor_id)}
        end)

        :ok

      false ->
        {:error, "hub not alive"}
    end
  end

  def hook_insert_pid(executor_id, %{child_id: relay_id} = hook_data) do
    node_pids = Map.get(hook_data, :node_pids, [])
    metadata = Map.get(hook_data, :metadata, %{})
    assigned_ce = Map.get(metadata, :tag)

    # Phase 2 fix (consume-recovery-mode-and-self-heal §2.8): only the
    # executor whose session id matches the registry entry's
    # `metadata.tag` (a.k.a `assigned_ce`) should receive this relay's
    # frames. The pre-change behaviour pushed the update to EVERY
    # registered executor's stream channels — `e1` AND `e2` both got
    # the pid list, and `e2`'s SDK dropped each frame at
    # `fast.rs:249` (`relay instance ... not found`), causing the
    # measurable per-pod data-plane drain that EVIDENCE1 §2 captured.
    cond do
      is_binary(assigned_ce) and assigned_ce != executor_id ->
        # This insertion is for a different executor; this handler is
        # registered for `executor_id` only.
        nil

      true ->
        curr_node = node()

        updated_pids =
          Enum.filter(node_pids, fn {node, _pid} -> node === curr_node end)
          |> Enum.map(fn {_, pid} -> pid end)

        case updated_pids do
          [] ->
            nil

          _ ->
            stream_channels =
              Supervisor.which_children(Identificator.stream_sup_primary(executor_id)) ++
                Supervisor.which_children(Identificator.stream_sup_secondary(executor_id))

            # A wedged StreamChannel must not crash the caller.
            Enum.each(stream_channels, fn
              {_, pid, _, _} when is_pid(pid) ->
                _ = SafeCall.bounded(pid, {:relay_pid_update, relay_id, updated_pids}, 1_000)

              _ ->
                :ok
            end)
        end
    end
  end

  def hook_remove_pid(_executor_id, %{child_id: _relay_id}) do
    # TODO: Implement.
  end

  @doc """
  Read the current plugin list embedded in a relay's child-spec
  start args. Returns `{:ok, [{plugin_id, %Flezha.Plugin{}}]}` (entries
  are normalised to tuple form regardless of how they are stored).
  """
  @spec relay_plugins(String.t(), String.t()) ::
          {:ok, [{binary(), Plugin.t()}]} | {:error, term()}
  def relay_plugins(namespace, relay_id) do
    hub_id = Identificator.hub(namespace)

    case ProcessHub.is_alive?(hub_id) do
      false ->
        {:error, "hub not alive"}

      true ->
        case ProcessRegistry.lookup(hub_id, relay_id) do
          nil ->
            {:error, "relay not found"}

          {%{start: {_m, _f, [arg]}}, _node_pids} ->
            {:ok, normalize_plugins(extract_plugins(arg))}

          {%{start: {_m, _f, [arg]}}, _node_pids, _metadata} ->
            {:ok, normalize_plugins(extract_plugins(arg))}
        end
    end
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
        {namespace, relay} ->
          {namespace, rewrite_relay_plugins(relay, update_fn)}

        {namespace, executor_id, relay} ->
          {namespace, executor_id, rewrite_relay_plugins(relay, update_fn)}
      end

    Map.put(cspec, :start, {m, f, [new_arg]})
  end

  defp rewrite_relay_plugins(relay, update_fn) do
    current = normalize_plugins(extract_plugins({nil, relay}))
    new_plugins = update_fn.(current)
    new_opts = Keyword.put(relay.opts, :plugins, new_plugins)
    %{relay | opts: new_opts}
  end

  defp extract_plugins({_namespace, relay}),
    do: Keyword.get(relay.opts, :plugins, [])

  defp extract_plugins({_namespace, _executor_id, relay}),
    do: Keyword.get(relay.opts, :plugins, [])

  defp normalize_plugins(plugins) when is_list(plugins) do
    Enum.map(plugins, fn
      {id, %Plugin{} = plugin} -> {id, plugin}
      %Plugin{id: id} = plugin -> {id, plugin}
    end)
  end

  def alter_child_data(_hub_id, %{child_spec: cs} = child_data) do
    %{start: {m, f, [arg]}} = cs

    {namespace, relay} =
      case arg do
        {namespace, relay} ->
          {namespace, relay}

        {namespace, _existing_executor, relay} ->
          {namespace, relay}
      end

    factory_id = Map.get(relay, :factory_id)
    assigned_executor = RelayManager.assign_executor(namespace, factory_id)

    Logger.info(
      "[hub_manager] alter_child_data relay=#{inspect(Map.get(relay, :id))} " <>
        "factory=#{inspect(factory_id)} assigned_ce=#{inspect(assigned_executor)}"
    )

    if is_nil(assigned_executor) do
      raise "No active compute engines available in namespace #{namespace} for factory #{inspect(factory_id)}"
    end

    new_args = [{namespace, assigned_executor, relay}]

    child_data
    |> put_in([:child_spec, :start], {m, f, new_args})
    |> put_in([:metadata, :tag], assigned_executor)
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
      migration_strategy: %ProcessHub.Strategy.Migration.HotSwap{
        handover: true
      },
      registry_backend: registry_backend_config(hub_id),
      auto_recovery: auto_recovery_config(),
      recovery_marker: recovery_marker_config(hub_id),
      # Relay startup involves multiple GenServer hops and socket communication
      # to the compute engine (Rust), so the default 5s is too tight.
      cross_node_request_timeout: 10_000,
      hooks: %{
        Hook.child_data_alter() => [
          %HookManager{
            id: ProcessHub.Constant.Hook.child_data_alter(),
            m: __MODULE__,
            f: :alter_child_data,
            a: [hub_id, :_]
          }
        ]
      }
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

  defp registry_path(hub_id) do
    root = Application.get_env(:flezha, :process_hub_registry_root, "app_data/process_hub")
    dir = Path.join(root, Atom.to_string(hub_id))
    File.mkdir_p!(dir)
    Path.join(dir, "registry.dets")
  end

  defp auto_recovery_config() do
    case Application.get_env(:flezha, :auto_restore_enabled, true) do
      false ->
        false

      _ ->
        [
          recovery_window_ms: Application.get_env(:flezha, :recovery_window_ms, 10_000),
          replay_timeout_ms: Application.get_env(:flezha, :boot_replay_timeout_ms, 60_000),
          recovery_timeout_ms:
            Application.get_env(:flezha, :process_hub_recovery_timeout_ms, 30_000)
        ]
    end
  end

  defp recovery_marker_config(hub_id) do
    case Application.get_env(:flezha, :auto_restore_enabled, true) do
      false ->
        # When auto_recovery is off the upstream library ignores recovery_marker.
        # Pass nil so its default `enabled?: false` resolution applies.
        nil

      _ ->
        %{enabled?: true, path: marker_path(hub_id)}
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
  # Public for the CLI `flz cluster status --recovery` surface (introduced
  # by `consume-recovery-mode-and-self-heal`). Resolves the same marker path
  # the hub spec uses, so an operator-visible check matches the live state.
  @spec resolved_marker_path(atom()) :: binary()
  def resolved_marker_path(hub_id), do: marker_path(hub_id)
end
