defmodule Flezha.Runner.FleetManager do
  alias Flezha.Cluster.Orchestrator
  alias Flezha.Runner.RunnerSupervisor
  alias Flezha.Runner.FleetSupervisor
  alias Flezha.Runner.StreamSup
  alias Flezha.Service.Identificator
  alias Flezha.Service.HubManager
  alias Flezha.Service.EventManager
  alias Flezha.Shutdown.Coordinator, as: ShutdownCoordinator
  alias Flezha.Transport.Native
  alias Flezha.Convergence.SafeCall

  require Logger

  use GenServer

  # Takeover fence marks (see `mark_session_superseded/1`). Entries outlive
  # eviction only long enough to cover in-flight mailbox residue; pruned on
  # the periodic cleanup tick.
  @superseded_table :flezha_superseded_sessions
  @superseded_ttl_ms 600_000

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
      Logger.info("[shutdown] rejecting runner connect attempt for namespace=#{namespace}")
      {:error, :shutting_down}
    else
      factories = parse_factories(init_payload)
      stable_id = parse_stable_id(init_payload)

      case GenServer.call(
             __MODULE__,
             {:runner_connect, stream_ref, namespace, factories, transport, stable_id},
             runner_connect_timeout()
           ) do
        {:error, reason} ->
          Logger.error("Failed to connect runner: #{inspect(reason)}")
          {:error, reason}

        {:ok, {_namespace, session_token}} ->
          {:ok, session_token}
      end
    end
  end

  def active_runners(namespace) do
    GenServer.call(__MODULE__, {:active_runners, namespace})
  end

  @doc """
  Atomically evict a dead/orphaned runner binding: clear every operative's
  stale stream mapping (`HubManager.hook_remove_pid/2`), stop its runner
  supervisor, drop its factory refs + operative-insertion hook, and remove it
  from `runner_namespaces`. Its operatives become misbound and rebind onto
  a live runner on the next convergence tick. Idempotent — an unknown
  id returns `:ok`. Driven by `Flezha.Convergence.Heal.execute/1` and by the
  reconnect binding-swap in `runner_connect` (to close the prior session).
  """
  @spec evict_runner(binary()) :: :ok
  def evict_runner(runner_id) when is_binary(runner_id) do
    GenServer.call(__MODULE__, {:evict_runner, runner_id})
  end

  @doc """
  Mark a session token superseded by a newest-wins takeover. The mark is the
  WAL-side half of the write-path fence: `Flezha.State.WalWriter` refuses a
  realign (or any append) from a marked session, closing the in-flight window
  the channel-side flip cannot cover — a wedged `StreamChannel` that flushes
  its queued frames only after the binding swap.
  """
  @spec mark_session_superseded(binary()) :: :ok
  def mark_session_superseded(session_token) when is_binary(session_token) do
    ensure_superseded_table()
    :ets.insert(@superseded_table, {session_token, :erlang.monotonic_time(:millisecond)})
    :ok
  end

  @doc "Whether a takeover superseded `session_token` (see `mark_session_superseded/1`)."
  @spec superseded_session?(binary()) :: boolean()
  def superseded_session?(session_token) do
    :ets.whereis(@superseded_table) != :undefined and
      :ets.member(@superseded_table, session_token)
  end

  @doc """
  Returns active compute engines in `namespace` that advertise `factory_id`.

  Used by `OperativeManager.assign_runner/3` so we never route a
  operative to a node that doesn't own its factory — without this filter
  every input frame for a misrouted operative hits a dead registry entry
  on the foreign node and generates a `enqueue: operative X not found`
  warning on every input.
  """
  def runners_for_factory(namespace, factory_id) do
    GenServer.call(__MODULE__, {:runners_for_factory, namespace, factory_id})
  end

  def disconnect_runner(namespace, runner_id) do
    GenServer.call(__MODULE__, {:disconnect_runner, namespace, runner_id})
  end

  @doc """
  Clear the `stream_ref` on this runner's `ControlChannel` so the existing
  rebind machinery (see `executor-controller-connection-healing`) picks up
  the next data stream the SDK opens. Called by
  `Flezha.Operative.Overwatch` after an end-to-end ping fails, indicating the
  controller-side `ControlChannel` is alive but its underlying transport
  handle is stale.

  Returns `:ok` on success, `{:error, :no_control_channel}` if the channel
  GenServer is not registered for `runner_id`.
  """
  @spec reset_control_channel(binary()) :: :ok | {:error, term()}
  def reset_control_channel(runner_id) when is_binary(runner_id) do
    Flezha.Runner.ControlChannel.reset_stream_ref(runner_id)
  end

  @doc """
  Re-issue `ConnectSocket` to `runner_id` so the SDK re-opens its data
  sockets; the `StreamAcceptor` binds the fresh streams to the unbound
  `StreamChannel`s (see `executor-controller-connection-healing`). Used by the
  reconciler to heal a operative whose data stream dropped (`stream_ref: nil`).

  Returns `:ok`, or `{:error, :no_control_unit}` when the runner's control
  unit is not registered.
  """
  @spec rebind_data_streams(binary()) :: :ok | {:error, term()}
  def rebind_data_streams(runner_id) when is_binary(runner_id) do
    control_unit = Identificator.control_unit(runner_id)

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
    # init time — hubs are created lazily by `runner_connect`), this
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

    schedule_runner_cleanup()

    # Own the fence table from this permanent process so marks survive the
    # transient readers (WalWriters) that consult them.
    ensure_superseded_table()

    {:ok, %{runner_namespaces: %{}, namespace_factories: %{}, stable_sessions: %{}}}
  end

  defp boot_recovery_timeout() do
    Application.get_env(:flezha, :boot_replay_timeout_ms, 60_000)
  end

  @impl true
  def handle_call({:disconnect_runner, namespace, runner_id}, _from, state) do
    case get_in(state.runner_namespaces, [namespace, runner_id]) do
      nil ->
        {:reply, {:error, "runner not found"}, state}

      _ ->
        Logger.info("Disconnecting runner #{runner_id} from namespace #{namespace}")

        GenServer.call(Identificator.control_unit(runner_id), :disconnect_runner)

        {:reply, :ok, state}
    end
  end

  @impl true
  def handle_call({:runner_disconnected, namespace, runner_id}, _from, state) do
    state = handle_runner_disconnect(state, namespace, runner_id)

    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:active_runners, namespace}, _from, state) do
    runner_ids = get_runner_ids(state, namespace, :active)

    {:reply, runner_ids, state}
  end

  @impl true
  def handle_call({:runners_for_factory, namespace, factory_id}, _from, state) do
    {:reply, MapSet.to_list(placeable_runners(state, namespace, factory_id)), state}
  end

  @impl true
  def handle_call({:stop_namespace, namespace}, _from, state) do
    {reply, runner_namespaces} =
      case Map.get(state.runner_namespaces, namespace, nil) do
        nil ->
          {:namespace_not_found, state.runner_namespaces}

        _ ->
          runner_ids = get_runner_ids(state, namespace, :active)

          Enum.each(runner_ids, fn runner_id ->
            FleetSupervisor.stop_runner_sup(runner_id)
          end)

          # Drop every plugin registered for this namespace so a follow-up
          # test re-using the same namespace does not collide on
          # `{:error, "plugin already registered"}`. The runtime operative
          # processes are about to disappear with the namespace's hub —
          # the registry is the only thing that survives without explicit
          # cleanup.
          _ = Flezha.Service.PluginManager.disable_namespace_plugins(namespace)

          HubManager.stop_hub(namespace)

          {:ok, Map.delete(state.runner_namespaces, namespace)}
      end

    state = Map.put(state, :runner_namespaces, runner_namespaces)
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
        {:runner_connect, stream_ref, namespace, factories, transport, stable_id},
        _from,
        state
      ) do
    # runner_connect/3 mints a fresh per-connection SESSION TOKEN (the
    # Registry/StreamChannel naming + routing key) and brings up its supervision
    # tree. It always returns {:ok, ...} — no controller-level error path exists.
    # If one is added, re-introduce the {:error, reason} branch.
    {:ok, namespace, session_token} =
      runner_connect(stream_ref, namespace, transport)

    timestamp = DateTime.utc_now() |> DateTime.to_unix(:millisecond)

    state =
      case Map.get(state.runner_namespaces, namespace, nil) do
        nil -> put_in(state, [:runner_namespaces, namespace], %{})
        _ -> state
      end

    state =
      put_in(
        state,
        [:runner_namespaces, namespace, session_token],
        {:active, timestamp}
      )

    state = store_factories(state, namespace, session_token, factories)

    # Resolve the runner's durable STABLE IDENTITY: on a reconnect (or a
    # newest-wins takeover) re-point the prior session's operative bindings onto
    # this fresh session token and close the old session. A first connect just
    # records the mapping; a degraded runner (no stable id) connects fresh.
    state = resolve_stable_identity(state, namespace, stable_id, session_token)

    # The convergence Loop subscribes to this and runs an immediate tick as a
    # safety net; the binding swap above is the synchronous primary path.
    EventManager.dispatch(
      :runner_connected_event,
      {namespace, session_token},
      %{members: :local}
    )

    {:reply, {:ok, {namespace, session_token}}, state}
  end

  @impl true
  def handle_call({:evict_runner, runner_id}, _from, state) do
    # Safe no-op on a healthy target: never evict a runner that is
    # currently `:active` (connection open). The convergence Diff runs off
    # a snapshot that can lag a reconnect by a tick; acting on that stale
    # view is exactly the false-eviction → cascade-rebind regression this
    # change exists to remove. The authoritative live set is the gate.
    # (The reconnect binding-swap closes a prior session by id directly, not
    # through this guarded path.)
    if runner_active?(state, runner_id) do
      {:reply, :ok, state}
    else
      {:reply, :ok, do_evict_runner(state, runner_id)}
    end
  end

  @impl true
  def handle_call(:runner_cleanup, _from, state) do
    {:reply, :ok, runner_cleanup(state)}
  end

  # Full-set replacement of one runner's declared factory list, cast from
  # `ControlChannel` on an SDK `UpdateFactories` frame (post-start
  # define/remove). Composes the existing removal + store paths, so redelivery
  # is idempotent and other runners' factory refs are untouched. An empty set
  # is legal: the runner stays connected with zero placeable factories.
  @impl true
  def handle_cast({:sync_factories, namespace, runner_id, factories}, state) do
    dropped = dropped_factory_ids(state, namespace, runner_id, factories)

    state =
      state
      |> remove_ce_factories(namespace, runner_id)
      |> store_factories(namespace, runner_id, factories)

    warn_dropped_factories(state, namespace, runner_id, dropped)

    {:noreply, state}
  end

  @impl true
  def handle_info(:schedule_runner_cleanup, state) do
    schedule_runner_cleanup()

    {:noreply, runner_cleanup(state)}
  end

  @impl true
  def handle_info(any, state) do
    Logger.info("Received unexpected message at node manager: #{inspect(any)}")

    {:noreply, state}
  end

  defp schedule_runner_cleanup() do
    Process.send_after(
      self(),
      :schedule_runner_cleanup,
      get_runner_keepalive() + 10_000
    )
  end

  defp get_runner_keepalive() do
    keepalive = Application.get_env(:flezha, :runner_keepalive, "30000")

    cond do
      is_binary(keepalive) -> String.to_integer(keepalive)
      is_integer(keepalive) -> keepalive
    end
  end

  defp runner_cleanup(state) do
    prune_superseded_sessions()

    now = DateTime.utc_now() |> DateTime.to_unix(:millisecond)
    keep_alive = get_runner_keepalive()

    namespaces = Map.keys(state.runner_namespaces)

    removal =
      Enum.map(namespaces, fn namespace ->
        in_active_runners = get_runner_ids(state, namespace, :inactive)

        removal_runners =
          Enum.map(in_active_runners, fn runner_id ->
            {_status, timestamp} = state.runner_namespaces[namespace][runner_id]

            if timestamp + keep_alive <= now do
              HubManager.get_registry(namespace)
              HubManager.stop_hub(namespace)
              runner_id
            else
              nil
            end
          end)

        {namespace, removal_runners}
      end)

    runner_namespaces =
      Enum.reduce(removal, state.runner_namespaces, fn {namespace, removal_runners}, acc ->
        namespace_runners =
          Enum.reduce(removal_runners, acc[namespace], fn runner_id, acc ->
            if runner_id == nil do
              acc
            else
              Map.delete(acc, runner_id)
            end
          end)

        Map.put(acc, namespace, namespace_runners)
      end)

    state = %{state | runner_namespaces: runner_namespaces}

    # Clean factory refs for removed CEs
    Enum.reduce(removal, state, fn {namespace, removal_runners}, acc ->
      Enum.reduce(removal_runners, acc, fn
        nil, acc -> acc
        ce_id, acc -> remove_ce_factories(acc, namespace, ce_id)
      end)
    end)
  end

  defp get_runner_ids(state, namespace, status) do
    state
    |> Map.get(:runner_namespaces, %{})
    |> Map.get(namespace, %{})
    |> Enum.filter(fn {_, {s, _}} -> s == status end)
    |> Enum.map(fn {runner_id, {_, _}} -> runner_id end)
  end

  # A runner is healthy iff some namespace holds it with `:active` status.
  defp runner_active?(state, runner_id) do
    state
    |> Map.get(:runner_namespaces, %{})
    |> Enum.any?(fn {_ns, runners} ->
      match?({:active, _}, Map.get(runners, runner_id))
    end)
  end

  # Reconnect/takeover binding swap. The SDK presents the same persisted stable
  # id on every connect; the controller keys operatives on the per-connection
  # session token, so a reconnect re-points the prior session's bindings onto
  # the new token (flip the Registry tag + seed the new StreamChannels — the
  # same primitives the convergence Heal uses) and then closes the old session
  # (newest-wins, `executor-identity-and-session` D5). A first connect just
  # records the mapping; a degraded runner with no stable id connects fresh.
  defp resolve_stable_identity(state, _namespace, nil, _session_token), do: state

  defp resolve_stable_identity(state, namespace, stable_id, session_token) do
    state =
      case Map.get(state.stable_sessions, stable_id) do
        old_session when is_binary(old_session) and old_session != session_token ->
          # Unbind-old BEFORE bind-new: once the fence returns, nothing the
          # superseded session forwards can land in any Segment WAL, so no
          # window exists in which both sessions hold a writable binding.
          fence_superseded_session(old_session)
          swap_bindings(namespace, old_session, session_token)
          do_evict_runner(state, old_session)

        _ ->
          state
      end

    put_in(state, [:stable_sessions, stable_id], session_token)
  end

  # The write-path fence, in refusal order: mark the session superseded (the
  # WalWriter-side gate), then synchronously flip each of its StreamChannels
  # off the Segment write path. Bounded calls — a wedged channel must not
  # block the connect; its late-flushed frames are refused by the mark instead.
  defp fence_superseded_session(old_session) do
    mark_session_superseded(old_session)

    Enum.each(stream_channel_pids(old_session), fn pid ->
      _ = SafeCall.bounded(pid, :fence_write_path, 1_000)
    end)
  end

  # Resolve the session's fixed channel-name set (the same one `StreamSup`
  # spawns); a dead/unregistered channel forwards nothing, so it is skipped.
  defp stream_channel_pids(session_token) do
    for type <- [:primary, :secondary],
        name <- StreamSup.stream_ids(session_token, type),
        pid = GenServer.whereis(name),
        is_pid(pid),
        do: pid
  end

  defp ensure_superseded_table() do
    case :ets.whereis(@superseded_table) do
      :undefined ->
        try do
          :ets.new(@superseded_table, [:set, :public, :named_table, read_concurrency: true])
        rescue
          ArgumentError -> :ok
        end

        :ok

      _ ->
        :ok
    end
  end

  defp prune_superseded_sessions() do
    if :ets.whereis(@superseded_table) != :undefined do
      cutoff = :erlang.monotonic_time(:millisecond) - @superseded_ttl_ms
      :ets.select_delete(@superseded_table, [{{:_, :"$1"}, [{:<, :"$1", cutoff}], [true]}])
    end

    :ok
  end

  # Re-tag every operative still bound to `old_session` onto `new_session` and
  # seed the new session's pre-spawned StreamChannels so in-flight frames land
  # without waiting for a convergence tick. `do_evict_runner/2` then tears down
  # the old (now operative-less) session.
  defp swap_bindings(namespace, old_session, new_session) do
    if HubManager.hub_alive?(namespace) do
      HubManager.runner_operatives(namespace, old_session)
      |> Enum.each(fn {operative_id, node_pids} ->
        HubManager.operative_set_runner(namespace, operative_id, new_session)

        case HubManager.live_pid(node_pids) do
          nil -> :ok
          pid -> HubManager.hook_insert_local_pid(new_session, operative_id, pid)
        end
      end)
    end
  end

  defp do_evict_runner(state, runner_id) do
    case find_runner_namespace(state, runner_id) do
      nil ->
        state

      namespace ->
        if HubManager.hub_alive?(namespace) do
          HubManager.runner_operatives(namespace, runner_id)
          |> Enum.each(fn {operative_id, _node_pids} ->
            HubManager.hook_remove_pid(runner_id, %{child_id: operative_id})
          end)

          HubManager.unregister_handler(namespace, runner_id)
        end

        FleetSupervisor.stop_runner_sup(runner_id)

        :telemetry.execute(
          [:flezha, :runner, :evicted],
          %{count: 1},
          %{namespace: namespace, runner_id: runner_id}
        )

        state
        |> remove_ce_factories(namespace, runner_id)
        |> update_in([:runner_namespaces, namespace], &Map.delete(&1 || %{}, runner_id))
    end
  end

  defp find_runner_namespace(state, runner_id) do
    Enum.find_value(state.runner_namespaces, fn {namespace, runners} ->
      if Map.has_key?(runners, runner_id), do: namespace, else: nil
    end)
  end

  defp handle_runner_disconnect(state, namespace, runner_id) do
    if not Map.has_key?(state.runner_namespaces, namespace) do
      state
    else
      do_handle_runner_disconnect(state, namespace, runner_id)
    end
  end

  defp do_handle_runner_disconnect(state, namespace, runner_id) do
    FleetSupervisor.stop_runner_sup(runner_id)

    state = remove_ce_factories(state, namespace, runner_id)

    if HubManager.hub_alive?(namespace) do
      HubManager.unregister_handler(namespace, runner_id)
    end

    runner_ids =
      state
      |> get_runner_ids(namespace, :active)
      |> Enum.filter(fn id -> id != runner_id end)

    other_nodes = HubManager.get_hub_nodes(namespace)

    namespace_runners =
      cond do
        !Enum.empty?(runner_ids) ->
          runner_reroute(state, namespace, runner_id, runner_ids)

        Enum.empty?(runner_ids) && !Enum.empty?(other_nodes) ->
          hub_migrate(state, namespace, runner_id)

        Enum.empty?(runner_ids) && Enum.empty?(other_nodes) ->
          runner_preserve(state, namespace, runner_id)
      end

    EventManager.dispatch(
      :runner_disconnected_event,
      {namespace, runner_id},
      %{members: :local}
    )

    put_in(state, [:runner_namespaces, namespace], namespace_runners)
  end

  # Drop the dead runner from the active set unconditionally. Each branch
  # of the disconnect handler also performs best-effort operative-migration side
  # effects against ProcessHub / StreamChannels that can raise `:noproc` while
  # the hub or a freshly reconnected peer is mid-restart under rapid churn. If
  # one of those raises, the runner's `:active` entry must still be removed —
  # otherwise it lingers as a corpse that `active_runners/1` keeps reporting
  # and operatives get assigned to a dead node (unbounded ghost accumulation).
  defp drop_runner(state, namespace, runner_id) do
    Map.delete(state.runner_namespaces[namespace] || %{}, runner_id)
  end

  defp hub_migrate(_state, namespace, _runner_id) do
    HubManager.stop_hub(namespace)

    %{}
  end

  defp runner_preserve(state, namespace, runner_id) do
    runner = {:inactive, DateTime.utc_now() |> DateTime.to_unix(:millisecond)}

    put_in(state.runner_namespaces[namespace], [runner_id], runner)
  end

  defp runner_reroute(state, namespace, old_runner, available_runners) do
    new_runner = Enum.random(available_runners)

    # Best-effort operative migration. These calls reach into ProcessHub and the
    # new runner's StreamChannels, which can raise `:noproc` while the hub or
    # the freshly reconnected peer is mid-restart. A failure here must not abort
    # the dead runner's removal from the active set below — its operatives heal on
    # the next convergence tick regardless.
    try do
      reroute_operatives(namespace, old_runner, new_runner)
    rescue
      error ->
        Logger.warning(
          "[FleetManager] runner_reroute side effects failed for " <>
            "#{old_runner} -> #{new_runner}: #{inspect(error)}; " <>
            "evicting anyway, operatives heal on next convergence tick"
        )
    catch
      :exit, reason ->
        Logger.warning(
          "[FleetManager] runner_reroute side effects exited for " <>
            "#{old_runner} -> #{new_runner}: #{inspect(reason)}; " <>
            "evicting anyway, operatives heal on next convergence tick"
        )
    end

    drop_runner(state, namespace, old_runner)
  end

  defp reroute_operatives(namespace, old_runner, new_runner) do
    HubManager.runner_operatives(namespace, old_runner)
    |> Enum.each(fn {operative_id, node_pids} ->
      # Multi-node-safe: resolve the live/local pid (dropping dead-node
      # entries) before calling, so a operative registered on more than one node
      # neither crashes this path nor blocks on a dead pid.
      case HubManager.live_pid(node_pids) do
        nil ->
          Logger.debug(
            "[FleetManager] runner_reroute: no live pid for operative #{operative_id}; " <>
              "it will migrate on its next bind/rebind"
          )

        operative_pid ->
          # Bounded timeout (< 5s default of the outer Transport.Server /
          # ControlChannel call into FleetManager) so a operative that is
          # itself calling back into FleetManager cannot deadlock the
          # disconnect path. The operative will be migrated on its next
          # bind/rebind attempt.
          _ = SafeCall.bounded(operative_pid, {:runner_migrate, new_runner}, 2_000)
      end
    end)

    HubManager.runner_switch(namespace, old_runner, new_runner)

    HubManager.unregister_handler(namespace, old_runner)
    HubManager.register_handler(namespace, new_runner)

    overwatchers =
      HubManager.runner_operatives(namespace, new_runner)
      |> Enum.map(fn {operative_id, node_pids} ->
        {operative_id, Enum.map(node_pids, &elem(&1, 1))}
      end)

    stream_channels =
      Supervisor.which_children(Identificator.stream_sup_primary(new_runner)) ++
        Supervisor.which_children(Identificator.stream_sup_secondary(new_runner))

    # Defensive: a slow StreamChannel must not crash the caller.
    Enum.each(stream_channels, fn
      {_, pid, _, _} when is_pid(pid) ->
        _ = SafeCall.bounded(pid, {:operative_bulk_insert, overwatchers}, 1_000)

      _ ->
        :ok
    end)
  end

  # runner_connect/3 may block inside Orchestrator.await_cluster_ready/0
  # (bounded by :cluster_formation_timeout_ms, default 15s) while the cluster
  # forms after a restart. The connect call must outlast that window plus the
  # rest of the connect work, or the caller's default 5s timeout fires and
  # strands every runner that connects during formation.
  defp runner_connect_timeout do
    Application.get_env(:flezha, :cluster_formation_timeout_ms, 15_000) + 5_000
  end

  defp runner_connect(stream_ref, namespace, transport) do
    runner_id = Identificator.unique_id(namespace)
    control_unit = Identificator.control_unit(runner_id)

    if !HubManager.hub_alive?(namespace) do
      # Gate on-demand hub creation on cluster formation: a runner connecting
      # during the boot window must not create a singleton hub before the node
      # has joined the cluster. Bounded by the degrade-to-solo timeout, so a
      # genuinely single-node deployment still proceeds.
      _ = Orchestrator.await_cluster_ready()
      HubManager.start_hub(namespace)
    end

    HubManager.register_handler(namespace, runner_id)

    FleetSupervisor.start_runner_sup(namespace, runner_id, stream_ref, transport)

    # Transfer ownership of the stream to the ControlChannel process
    control_channel_pid =
      RunnerSupervisor.find_control_channel(runner_id)

    Native.controlling_process(stream_ref, control_channel_pid)
    Native.set_active(stream_ref)

    # Send setup commands
    GenServer.call(control_unit, :command_assign_runner_id)
    GenServer.call(control_unit, :command_sockets_connect)

    {:ok, namespace, runner_id}
  end

  # The SDK's self-owned, durable stable identity, presented on every connect.
  # `nil` only when absent/empty (a degraded runner with no writable
  # persistence path) — such a runner connects fresh with no binding
  # continuity, per the identity-degradation contract.
  defp parse_stable_id(init_payload) do
    case Map.get(init_payload, "stable_id") do
      id when is_binary(id) and id != "" -> id
      _ -> nil
    end
  end

  defp parse_factories(init_payload) do
    Map.get(init_payload, "factories") || []
  end

  defp store_factories(state, namespace, runner_id, factories) do
    ns_factories = Map.get(state.namespace_factories, namespace, %{})

    ns_factories =
      Enum.reduce(factories, ns_factories, fn factory, acc ->
        factory_id = factory["id"]
        existing = Map.get(acc, factory_id, %{description: nil, ce_ids: MapSet.new()})

        description = factory["description"] || existing.description

        Map.put(acc, factory_id, %{
          description: description,
          ce_ids: MapSet.put(existing.ce_ids, runner_id)
        })
      end)

    put_in(state, [:namespace_factories, namespace], ns_factories)
  end

  defp remove_ce_factories(state, namespace, runner_id) do
    ns_factories = Map.get(state.namespace_factories, namespace, %{})

    ns_factories =
      ns_factories
      |> Enum.map(fn {factory_id, info} ->
        {factory_id, %{info | ce_ids: MapSet.delete(info.ce_ids, runner_id)}}
      end)
      |> Enum.reject(fn {_factory_id, info} -> MapSet.size(info.ce_ids) == 0 end)
      |> Map.new()

    put_in(state, [:namespace_factories, namespace], ns_factories)
  end

  # Factory ids this runner advertised before a sync that are absent from the
  # incoming full set.
  defp dropped_factory_ids(state, namespace, runner_id, factories) do
    new_ids = MapSet.new(factories, & &1["id"])

    state.namespace_factories
    |> Map.get(namespace, %{})
    |> Enum.filter(fn {factory_id, info} ->
      MapSet.member?(info.ce_ids, runner_id) and not MapSet.member?(new_ids, factory_id)
    end)
    |> Enum.map(fn {factory_id, _info} -> factory_id end)
  end

  # Tripwire for the suspended-instance removal hole: a dropped factory with
  # controller-known operatives still bound to this runner — or suspended
  # (runner tag nil) with no other active runner advertising the factory —
  # escaped the SDK-side live-instance check, and its rehydration will fail
  # placement. Warn only; a blocking policy is deferred.
  defp warn_dropped_factories(state, namespace, runner_id, dropped) do
    Enum.each(dropped, fn factory_id ->
      case affected_operatives(state, namespace, runner_id, factory_id) do
        [] ->
          :ok

        affected ->
          Logger.warning(
            "[fleet_manager] runner #{runner_id} on #{namespace} dropped factory " <>
              "#{factory_id} that still has controller-known operatives " <>
              "#{inspect(affected)}; their (re)placement will fail until a " <>
              "runner re-defines the factory"
          )
      end
    end)
  end

  defp affected_operatives(state, namespace, runner_id, factory_id) do
    still_placeable = MapSet.size(placeable_runners(state, namespace, factory_id)) > 0

    namespace
    |> HubManager.operative_rows()
    |> Enum.filter(fn row ->
      row.factory == factory_id and
        (row.runner == runner_id or (is_nil(row.runner) and not still_placeable))
    end)
    |> Enum.map(& &1.id)
  end

  # ACTIVE runners currently advertising `factory_id` — the placement filter
  # (`runners_for_factory`) and the dropped-factory tripwire share this view.
  defp placeable_runners(state, namespace, factory_id) do
    active_ids = MapSet.new(get_runner_ids(state, namespace, :active))

    state.namespace_factories
    |> Map.get(namespace, %{})
    |> Map.get(factory_id, %{ce_ids: MapSet.new()})
    |> Map.get(:ce_ids)
    |> MapSet.intersection(active_ids)
  end
end
