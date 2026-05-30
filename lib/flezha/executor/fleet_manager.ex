defmodule Flezha.Executor.FleetManager do
  alias Flezha.Cluster.Orchestrator
  alias Flezha.Executor.ExecutorSupervisor
  alias Flezha.Executor.FleetSupervisor
  alias Flezha.Service.Identificator
  alias Flezha.Service.HubManager
  alias Flezha.Service.EventManager
  alias Flezha.Shutdown.Coordinator, as: ShutdownCoordinator
  alias Flezha.Transport.Native
  alias Flezha.Convergence.SafeCall

  require Logger

  use GenServer

  def start_link(_) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  @doc """
  Unified connection handler for all transports.

  Called by Transport.Server when a new SDK connects.
  `stream_ref` is a NIF StreamHandle resource.
  `namespace` is extracted from the connect_control_channel frame.
  `init_payload` is the decoded JSON from the initial frame.
  """
  def list_factories(namespace) do
    GenServer.call(__MODULE__, {:list_factories, namespace})
  end

  def connect_handler(stream_ref, namespace, init_payload, transport) do
    if ShutdownCoordinator.shutting_down?() do
      Logger.info("[shutdown] rejecting executor connect attempt for namespace=#{namespace}")
      {:error, :shutting_down}
    else
      factories = parse_factories(init_payload)
      previous_executor_id = parse_previous_executor_id(init_payload)

      case GenServer.call(
             __MODULE__,
             {:executor_connect, stream_ref, namespace, factories, transport,
              previous_executor_id},
             executor_connect_timeout()
           ) do
        {:error, reason} ->
          Logger.error("Failed to connect executor: #{inspect(reason)}")
          {:error, reason}

        {:ok, {_namespace, executor_id}} ->
          {:ok, executor_id}
      end
    end
  end

  def active_executors(namespace) do
    GenServer.call(__MODULE__, {:active_executors, namespace})
  end

  @doc """
  Atomically evict a dead/orphaned executor binding: clear every relay's
  stale stream mapping (`HubManager.hook_remove_pid/2`), stop its executor
  supervisor, drop its factory refs + relay-insertion hook, and remove it
  from `executor_namespaces`. Its relays become misbound and rebind onto
  a live executor on the next convergence tick. Idempotent — an unknown
  id returns `:ok`. Driven by `Flezha.Convergence.Heal.execute/1` (row 16)
  and by `connect_handler/4` when an SDK presents a `previous_executor_id`.
  """
  @spec evict_executor(binary()) :: :ok
  def evict_executor(executor_id) when is_binary(executor_id) do
    GenServer.call(__MODULE__, {:evict_executor, executor_id})
  end

  @doc """
  Returns active compute engines in `namespace` that advertise `factory_id`.

  Used by `RelayManager.assign_executor/2` so we never route a
  relay to a node that doesn't own its factory — without this filter
  every input frame for a misrouted relay hits a dead registry entry
  on the foreign node and generates a `enqueue: relay X not found`
  warning on every input.
  """
  def executors_for_factory(namespace, factory_id) do
    GenServer.call(__MODULE__, {:executors_for_factory, namespace, factory_id})
  end

  def disconnect_executor(namespace, executor_id) do
    GenServer.call(__MODULE__, {:disconnect_executor, namespace, executor_id})
  end

  @doc """
  Clear the `stream_ref` on this executor's `ControlChannel` so the existing
  rebind machinery (see `executor-controller-connection-healing`) picks up
  the next data stream the SDK opens. Called by
  `Flezha.Relay.Overwatch` after an end-to-end ping fails, indicating the
  controller-side `ControlChannel` is alive but its underlying transport
  handle is stale.

  Returns `:ok` on success, `{:error, :no_control_channel}` if the channel
  GenServer is not registered for `executor_id`.
  """
  @spec reset_control_channel(binary()) :: :ok | {:error, term()}
  def reset_control_channel(executor_id) when is_binary(executor_id) do
    Flezha.Executor.ControlChannel.reset_stream_ref(executor_id)
  end

  @doc """
  Re-issue `ConnectSocket` to `executor_id` so the SDK re-opens its data
  sockets; the `StreamAcceptor` binds the fresh streams to the unbound
  `StreamChannel`s (see `executor-controller-connection-healing`). Used by the
  reconciler to heal a relay whose data stream dropped (`stream_ref: nil`).

  Returns `:ok`, or `{:error, :no_control_unit}` when the executor's control
  unit is not registered.
  """
  @spec rebind_data_streams(binary()) :: :ok | {:error, term()}
  def rebind_data_streams(executor_id) when is_binary(executor_id) do
    control_unit = Identificator.control_unit(executor_id)

    case GenServer.whereis(control_unit) do
      nil -> {:error, :no_control_unit}
      _ -> GenServer.call(control_unit, :command_sockets_connect)
    end
  end

  def stop_namespace(namespace) do
    GenServer.call(__MODULE__, {:stop_namespace, namespace})
  end

  @impl true
  def init(_) do
    # Block boot until every per-namespace hub finishes its
    # marker-gated recovery cycle. With no hubs yet (the common case at
    # init time — hubs are created lazily by `executor_connect`), this
    # returns immediately. The gate matters on a restart where the
    # supervisor brings up a previously-running hub before FleetManager
    # is asked to serve registry-touching calls.
    case HubManager.await_recovery_complete(boot_recovery_timeout()) do
      :ok ->
        :ok

      {:error, {:recovery_timeout, hubs}} ->
        Logger.warning(
          "[fleet_manager] proceeding past recovery timeout; unfinished hubs=#{inspect(hubs)}"
        )
    end

    schedule_executor_cleanup()

    {:ok, %{executor_namespaces: %{}, namespace_factories: %{}}}
  end

  defp boot_recovery_timeout() do
    Application.get_env(:flezha, :boot_replay_timeout_ms, 60_000)
  end

  @impl true
  def handle_call({:disconnect_executor, namespace, executor_id}, _from, state) do
    case get_in(state.executor_namespaces, [namespace, executor_id]) do
      nil ->
        {:reply, {:error, "executor not found"}, state}

      _ ->
        Logger.info("Disconnecting executor #{executor_id} from namespace #{namespace}")

        GenServer.call(Identificator.control_unit(executor_id), :disconnect_executor)

        {:reply, :ok, state}
    end
  end

  @impl true
  def handle_call({:executor_disconnected, namespace, executor_id}, _from, state) do
    state = handle_executor_disconnect(state, namespace, executor_id)

    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:active_executors, namespace}, _from, state) do
    executor_ids = get_executor_ids(state, namespace, :active)

    {:reply, executor_ids, state}
  end

  # Evict each id in `executor_ids` that the periodic liveness sweep
  # already proved unreachable (cu dead, or end-to-end ping failed). The
  # ping happens outside the GenServer (see `liveness_sweep/1`); this call
  # only applies the verdict so the FleetManager is never blocked on a
  # round-trip. `do_evict_executor` stops the executor supervisor — taking
  # the zombie ControlUnit/ControlChannel down with it — and drops the id
  # from the active set, closing the silent "relay bound to a zombie that
  # produces nothing" failure class.
  @impl true
  def handle_call({:evict_unreachable, executor_ids}, _from, state) do
    state =
      Enum.reduce(executor_ids, state, fn executor_id, acc ->
        :telemetry.execute(
          [:flezha, :executor, :evicted_unreachable],
          %{count: 1},
          %{executor_id: executor_id}
        )

        do_evict_executor(acc, executor_id)
      end)

    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:executors_for_factory, namespace, factory_id}, _from, state) do
    active_ids = MapSet.new(get_executor_ids(state, namespace, :active))

    ce_ids =
      state.namespace_factories
      |> Map.get(namespace, %{})
      |> Map.get(factory_id, %{ce_ids: MapSet.new()})
      |> Map.get(:ce_ids)
      |> MapSet.intersection(active_ids)
      |> MapSet.to_list()

    {:reply, ce_ids, state}
  end

  @impl true
  def handle_call({:stop_namespace, namespace}, _from, state) do
    {reply, executor_namespaces} =
      case Map.get(state.executor_namespaces, namespace, nil) do
        nil ->
          {:namespace_not_found, state.executor_namespaces}

        _ ->
          executor_ids = get_executor_ids(state, namespace, :active)

          Enum.each(executor_ids, fn executor_id ->
            FleetSupervisor.stop_executor_sup(executor_id)
          end)

          # Drop every plugin registered for this namespace so a follow-up
          # test re-using the same namespace does not collide on
          # `{:error, "plugin already registered"}`. The runtime relay
          # processes are about to disappear with the namespace's hub —
          # the registry is the only thing that survives without explicit
          # cleanup.
          _ = Flezha.Service.PluginManager.disable_namespace_plugins(namespace)

          HubManager.stop_hub(namespace)

          {:ok, Map.delete(state.executor_namespaces, namespace)}
      end

    state = Map.put(state, :executor_namespaces, executor_namespaces)
    state = Map.update!(state, :namespace_factories, &Map.delete(&1, namespace))
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:list_factories, namespace}, _from, state) do
    factories =
      state.namespace_factories
      |> Map.get(namespace, %{})
      |> Enum.map(fn {factory_id, info} ->
        %{id: factory_id, description: info.description}
      end)
      |> Enum.sort_by(& &1.id)

    {:reply, {:ok, factories}, state}
  end

  @impl true
  def handle_call(
        {:executor_connect, stream_ref, namespace, factories, transport, previous_executor_id},
        _from,
        state
      ) do
    # When the SDK presents its persisted id from a prior process, evict
    # that stale binding BEFORE the new bind so the old executor's relays
    # and stream mappings never linger as a corpse the takeover rebinds
    # onto (proposal §2 "FleetManager never evicts dead executors").
    state = maybe_evict_previous(state, previous_executor_id)

    # executor_connect/3 currently always returns {:ok, ...} — no error
    # path exists at the controller level yet. If one is ever added, re-introduce
    # the {:error, reason} branch here.
    {:ok, namespace, executor_id} =
      executor_connect(stream_ref, namespace, transport)

    timestamp = DateTime.utc_now() |> DateTime.to_unix(:millisecond)

    state =
      case Map.get(state.executor_namespaces, namespace, nil) do
        nil -> put_in(state, [:executor_namespaces, namespace], %{})
        _ -> state
      end

    state =
      put_in(
        state,
        [:executor_namespaces, namespace, executor_id],
        {:active, timestamp}
      )

    state = store_factories(state, namespace, executor_id, factories)

    # Fresh-process executor takeover (relays tagged to a now-orphaned
    # executor are reseated onto this new id) is driven by the
    # `Flezha.Convergence.Loop`, which subscribes to this event and runs
    # an immediate tick.
    EventManager.dispatch(
      :executor_connected_event,
      {namespace, executor_id},
      %{members: :local}
    )

    {:reply, {:ok, {namespace, executor_id}}, state}
  end

  @impl true
  def handle_call({:evict_executor, executor_id}, _from, state) do
    {:reply, :ok, do_evict_executor(state, executor_id)}
  end

  @impl true
  def handle_call(:executor_cleanup, _from, state) do
    {:reply, :ok, executor_cleanup(state)}
  end

  @impl true
  def handle_info(:schedule_executor_cleanup, state) do
    schedule_executor_cleanup()

    state = executor_cleanup(state)

    # Sweep the active set for zombies: an executor whose container was
    # SIGKILL'd but whose controller-side ControlUnit GenServer is still
    # alive (so it still looks "active") and whose `{:flz_closed}` disconnect
    # signal never arrived. The ping round-trips happen in a detached task so
    # the FleetManager is never blocked; unreachable ids are evicted via a
    # follow-up `{:evict_unreachable, ...}` call.
    liveness_sweep(state)

    {:noreply, state}
  end

  @impl true
  def handle_info(any, state) do
    Logger.info("Received unexpected message at node manager: #{inspect(any)}")

    {:noreply, state}
  end

  defp schedule_executor_cleanup() do
    Process.send_after(
      self(),
      :schedule_executor_cleanup,
      get_executor_keepalive() + 10_000
    )
  end

  defp get_executor_keepalive() do
    keepalive = Application.get_env(:flezha, :executor_keepalive, "30000")

    cond do
      is_binary(keepalive) -> String.to_integer(keepalive)
      is_integer(keepalive) -> keepalive
    end
  end

  defp executor_cleanup(state) do
    now = DateTime.utc_now() |> DateTime.to_unix(:millisecond)
    keep_alive = get_executor_keepalive()

    namespaces = Map.keys(state.executor_namespaces)

    removal =
      Enum.map(namespaces, fn namespace ->
        in_active_executors = get_executor_ids(state, namespace, :inactive)

        removal_executors =
          Enum.map(in_active_executors, fn executor_id ->
            {_status, timestamp} = state.executor_namespaces[namespace][executor_id]

            if timestamp + keep_alive <= now do
              HubManager.get_registry(namespace)
              HubManager.stop_hub(namespace)
              executor_id
            else
              nil
            end
          end)

        {namespace, removal_executors}
      end)

    executor_namespaces =
      Enum.reduce(removal, state.executor_namespaces, fn {namespace, removal_executors}, acc ->
        namespace_executors =
          Enum.reduce(removal_executors, acc[namespace], fn executor_id, acc ->
            if executor_id == nil do
              acc
            else
              Map.delete(acc, executor_id)
            end
          end)

        Map.put(acc, namespace, namespace_executors)
      end)

    state = %{state | executor_namespaces: executor_namespaces}

    # Clean factory refs for removed CEs
    Enum.reduce(removal, state, fn {namespace, removal_executors}, acc ->
      Enum.reduce(removal_executors, acc, fn
        nil, acc -> acc
        ce_id, acc -> remove_ce_factories(acc, namespace, ce_id)
      end)
    end)
  end

  # Spawn a detached task that pings every `:active` executor end-to-end and
  # reports the unreachable ones back for eviction. Detached because a ping is
  # a bounded GenServer.call into the cu (default 2 s) per executor — running
  # them inline would block the FleetManager mailbox for seconds under churn.
  # A genuinely live executor replies to its ping and is left untouched, so a
  # healthy node is never evicted.
  defp liveness_sweep(state) do
    active =
      state.executor_namespaces
      |> Map.values()
      |> Enum.flat_map(&get_active_ids/1)

    if active != [] do
      fleet = self()

      _ =
        Task.start(fn ->
          unreachable = Enum.reject(active, &executor_reachable?/1)

          if unreachable != [] do
            _ = SafeCall.bounded(fleet, {:evict_unreachable, unreachable}, 5_000)
          end
        end)
    end

    :ok
  end

  defp get_active_ids(executors) do
    executors
    |> Enum.filter(fn {_id, {status, _ts}} -> status == :active end)
    |> Enum.map(fn {id, _} -> id end)
  end

  @doc """
  True only when `executor_id`'s ControlUnit GenServer is registered AND a
  fresh end-to-end `Ping` round-trips to its SDK. A registered-but-wedged
  control unit (the zombie left behind by a SIGKILL'd container whose
  `{:flz_closed}` disconnect never arrived) fails the ping, so callers in the
  relay-assignment path can skip it. Used both by the FleetManager liveness
  sweep and by `RelayManager.assign_executor/2` as defense in depth.
  """
  @spec executor_reachable?(binary()) :: boolean()
  def executor_reachable?(executor_id) when is_binary(executor_id) do
    case Flezha.Executor.ControlUnit.ping_executor(executor_id) do
      {:ok, :pong} -> true
      _ -> false
    end
  end

  defp get_executor_ids(state, namespace, status) do
    state
    |> Map.get(:executor_namespaces, %{})
    |> Map.get(namespace, %{})
    |> Enum.filter(fn {_, {s, _}} -> s == status end)
    |> Enum.map(fn {executor_id, {_, _}} -> executor_id end)
  end

  defp maybe_evict_previous(state, previous_id) when is_binary(previous_id) and previous_id != "" do
    do_evict_executor(state, previous_id)
  end

  defp maybe_evict_previous(state, _), do: state

  defp do_evict_executor(state, executor_id) do
    case find_executor_namespace(state, executor_id) do
      nil ->
        state

      namespace ->
        if HubManager.hub_alive?(namespace) do
          HubManager.executor_relays(namespace, executor_id)
          |> Enum.each(fn {relay_id, _node_pids} ->
            HubManager.hook_remove_pid(executor_id, %{child_id: relay_id})
          end)

          HubManager.unregister_handler(namespace, executor_id)
        end

        FleetSupervisor.stop_executor_sup(executor_id)

        :telemetry.execute(
          [:flezha, :executor, :evicted],
          %{count: 1},
          %{namespace: namespace, executor_id: executor_id}
        )

        state
        |> remove_ce_factories(namespace, executor_id)
        |> update_in([:executor_namespaces, namespace], &Map.delete(&1 || %{}, executor_id))
    end
  end

  defp find_executor_namespace(state, executor_id) do
    Enum.find_value(state.executor_namespaces, fn {namespace, executors} ->
      if Map.has_key?(executors, executor_id), do: namespace, else: nil
    end)
  end

  defp handle_executor_disconnect(state, namespace, executor_id) do
    if not Map.has_key?(state.executor_namespaces, namespace) do
      state
    else
      do_handle_executor_disconnect(state, namespace, executor_id)
    end
  end

  defp do_handle_executor_disconnect(state, namespace, executor_id) do
    FleetSupervisor.stop_executor_sup(executor_id)

    state = remove_ce_factories(state, namespace, executor_id)

    if HubManager.hub_alive?(namespace) do
      HubManager.unregister_handler(namespace, executor_id)
    end

    executor_ids =
      state
      |> get_executor_ids(namespace, :active)
      |> Enum.filter(fn id -> id != executor_id end)

    other_nodes = HubManager.get_hub_nodes(namespace)

    namespace_executors =
      cond do
        !Enum.empty?(executor_ids) ->
          executor_reroute(state, namespace, executor_id, executor_ids)

        Enum.empty?(executor_ids) && !Enum.empty?(other_nodes) ->
          hub_migrate(state, namespace, executor_id)

        Enum.empty?(executor_ids) && Enum.empty?(other_nodes) ->
          executor_preserve(state, namespace, executor_id)
      end

    EventManager.dispatch(
      :executor_disconnected_event,
      {namespace, executor_id},
      %{members: :local}
    )

    put_in(state, [:executor_namespaces, namespace], namespace_executors)
  end

  # Drop the dead executor from the active set unconditionally. Each branch
  # of the disconnect handler also performs best-effort relay-migration side
  # effects against ProcessHub / StreamChannels that can raise `:noproc` while
  # the hub or a freshly reconnected peer is mid-restart under rapid churn. If
  # one of those raises, the executor's `:active` entry must still be removed —
  # otherwise it lingers as a corpse that `active_executors/1` keeps reporting
  # and relays get assigned to a dead node (unbounded ghost accumulation).
  defp drop_executor(state, namespace, executor_id) do
    Map.delete(state.executor_namespaces[namespace] || %{}, executor_id)
  end

  defp hub_migrate(_state, namespace, _executor_id) do
    HubManager.stop_hub(namespace)

    %{}
  end

  defp executor_preserve(state, namespace, executor_id) do
    executor = {:inactive, DateTime.utc_now() |> DateTime.to_unix(:millisecond)}

    put_in(state.executor_namespaces[namespace], [executor_id], executor)
  end

  defp executor_reroute(state, namespace, old_executor, available_executors) do
    new_executor = Enum.random(available_executors)

    # Best-effort relay migration. These calls reach into ProcessHub and the
    # new executor's StreamChannels, which can raise `:noproc` while the hub or
    # the freshly reconnected peer is mid-restart. A failure here must not abort
    # the dead executor's removal from the active set below — its relays heal on
    # the next convergence tick regardless.
    try do
      reroute_relays(namespace, old_executor, new_executor)
    rescue
      error ->
        Logger.warning(
          "[FleetManager] executor_reroute side effects failed for " <>
            "#{old_executor} -> #{new_executor}: #{inspect(error)}; " <>
            "evicting anyway, relays heal on next convergence tick"
        )
    catch
      :exit, reason ->
        Logger.warning(
          "[FleetManager] executor_reroute side effects exited for " <>
            "#{old_executor} -> #{new_executor}: #{inspect(reason)}; " <>
            "evicting anyway, relays heal on next convergence tick"
        )
    end

    drop_executor(state, namespace, old_executor)
  end

  defp reroute_relays(namespace, old_executor, new_executor) do
    HubManager.executor_relays(namespace, old_executor)
    |> Enum.each(fn {relay_id, node_pids} ->
      # Multi-node-safe: resolve the live/local pid (dropping dead-node
      # entries) before calling, so a relay registered on more than one node
      # neither crashes this path nor blocks on a dead pid.
      case HubManager.live_pid(node_pids) do
        nil ->
          Logger.debug(
            "[FleetManager] executor_reroute: no live pid for relay #{relay_id}; " <>
              "it will migrate on its next bind/rebind"
          )

        relay_pid ->
          # Bounded timeout (< 5s default of the outer Transport.Server /
          # ControlChannel call into FleetManager) so a relay that is
          # itself calling back into FleetManager cannot deadlock the
          # disconnect path. The relay will be migrated on its next
          # bind/rebind attempt.
          _ = SafeCall.bounded(relay_pid, {:executor_migrate, new_executor}, 2_000)
      end
    end)

    HubManager.executor_switch(namespace, old_executor, new_executor)

    HubManager.unregister_handler(namespace, old_executor)
    HubManager.register_handler(namespace, new_executor)

    overwatchers =
      HubManager.executor_relays(namespace, new_executor)
      |> Enum.map(fn {relay_id, node_pids} ->
        {relay_id, Enum.map(node_pids, &elem(&1, 1))}
      end)

    stream_channels =
      Supervisor.which_children(Identificator.stream_sup_primary(new_executor)) ++
        Supervisor.which_children(Identificator.stream_sup_secondary(new_executor))

    # Defensive: a slow StreamChannel must not crash the caller.
    Enum.each(stream_channels, fn
      {_, pid, _, _} when is_pid(pid) ->
        _ = SafeCall.bounded(pid, {:relay_bulk_insert, overwatchers}, 1_000)

      _ ->
        :ok
    end)
  end

  # executor_connect/3 may block inside Orchestrator.await_cluster_ready/0
  # (bounded by :cluster_formation_timeout_ms, default 15s) while the cluster
  # forms after a restart. The connect call must outlast that window plus the
  # rest of the connect work, or the caller's default 5s timeout fires and
  # strands every executor that connects during formation.
  defp executor_connect_timeout do
    Application.get_env(:flezha, :cluster_formation_timeout_ms, 15_000) + 5_000
  end

  defp executor_connect(stream_ref, namespace, transport) do
    executor_id = Identificator.unique_id(namespace)
    control_unit = Identificator.control_unit(executor_id)

    if !HubManager.hub_alive?(namespace) do
      # Gate on-demand hub creation on cluster formation: an executor connecting
      # during the boot window must not create a singleton hub before the node
      # has joined the cluster. Bounded by the degrade-to-solo timeout, so a
      # genuinely single-node deployment still proceeds.
      _ = Orchestrator.await_cluster_ready()
      HubManager.start_hub(namespace)
    end

    HubManager.register_handler(namespace, executor_id)

    FleetSupervisor.start_executor_sup(namespace, executor_id, stream_ref, transport)

    # Transfer ownership of the stream to the ControlChannel process
    control_channel_pid =
      ExecutorSupervisor.find_control_channel(executor_id)

    Native.controlling_process(stream_ref, control_channel_pid)
    Native.set_active(stream_ref)

    # Send setup commands
    GenServer.call(control_unit, :command_assign_executor_id)
    GenServer.call(control_unit, :command_sockets_connect)

    {:ok, namespace, executor_id}
  end

  # The SDK's persisted id from a prior process (Phase D writes it); used
  # to evict the stale binding on reconnect. Absent on first connect.
  defp parse_previous_executor_id(init_payload) do
    case Map.get(init_payload, "previous_executor_id") do
      id when is_binary(id) and id != "" -> id
      _ -> nil
    end
  end

  defp parse_factories(init_payload) do
    case Map.get(init_payload, "factories") do
      nil ->
        # Backward compat: old SDKs only send factory_ids
        (Map.get(init_payload, "factory_ids") || [])
        |> Enum.map(fn id -> %{"id" => id, "description" => nil} end)

      factories when is_list(factories) ->
        factories
    end
  end

  defp store_factories(state, namespace, executor_id, factories) do
    ns_factories = Map.get(state.namespace_factories, namespace, %{})

    ns_factories =
      Enum.reduce(factories, ns_factories, fn factory, acc ->
        factory_id = factory["id"]
        existing = Map.get(acc, factory_id, %{description: nil, ce_ids: MapSet.new()})

        description = factory["description"] || existing.description

        Map.put(acc, factory_id, %{
          description: description,
          ce_ids: MapSet.put(existing.ce_ids, executor_id)
        })
      end)

    put_in(state, [:namespace_factories, namespace], ns_factories)
  end

  defp remove_ce_factories(state, namespace, executor_id) do
    ns_factories = Map.get(state.namespace_factories, namespace, %{})

    ns_factories =
      ns_factories
      |> Enum.map(fn {factory_id, info} ->
        {factory_id, %{info | ce_ids: MapSet.delete(info.ce_ids, executor_id)}}
      end)
      |> Enum.reject(fn {_factory_id, info} -> MapSet.size(info.ce_ids) == 0 end)
      |> Map.new()

    put_in(state, [:namespace_factories, namespace], ns_factories)
  end
end
