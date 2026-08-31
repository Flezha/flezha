defmodule Flezha.Runner.FleetManager do
  alias Flezha.AuthZ.QuotaCounters
  alias Flezha.AuthZ.Quotas
  alias Flezha.AuthZ.Scope
  alias Flezha.Cluster.Directory
  alias Flezha.Cluster.Orchestrator
  alias Flezha.Runner.ControlChannel
  alias Flezha.Runner.ControlUnit
  alias Flezha.Runner.RunnerSupervisor
  alias Flezha.Runner.FleetSupervisor
  alias Flezha.Runner.StreamSup
  alias Flezha.Identificator
  alias Flezha.Service.HubManager
  alias Flezha.Service.EventManager
  alias Flezha.Service.PluginManager
  alias Flezha.Shutdown.Coordinator, as: ShutdownCoordinator
  alias Flezha.Convergence.Heal
  alias Flezha.Convergence.SafeCall
  alias Flezha.Operative.ClassRegistry

  require Logger

  use GenServer

  # Takeover fence marks (see `mark_session_superseded/1`). Entries outlive
  # eviction only long enough to cover in-flight mailbox residue; pruned on
  # the periodic cleanup tick.
  @superseded_table :flezha_superseded_sessions
  @superseded_ttl_ms 600_000
  # Runners the controller is itself closing, so the close their sockets report
  # is diagnosed as expected rather than as an incident. Same mechanism as the
  # fence above — a TTL-expiring mark on an identity string, pruned on the
  # cleanup tick — and deliberately a separate table: the fence decides
  # correctness, this decides only what gets reported.
  @retiring_table :flezha_retiring_runners
  @lookup_timeout_ms 5_000

  # An advertisement carrying no language. Never places: the runner most likely
  # to be untagged is the one most likely to be mismatched.
  @sdk_lang_unspecified 0

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
    GenServer.call(__MODULE__, {:list_factories, namespace}, manager_call_timeout())
  end

  def connect_handler(stream_ref, namespace, init_payload, transport) do
    with :ok <- check_not_shutting_down(namespace),
         :ok <- admit_runner(namespace, transport) do
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

  defp check_not_shutting_down(namespace) do
    if ShutdownCoordinator.shutting_down?() do
      Logger.info("[shutdown] rejecting runner connect attempt for namespace=#{namespace}")
      {:error, :shutting_down}
    else
      :ok
    end
  end

  # The runner principal is minted from the authenticated connection's own
  # namespace handshake (the transport authenticated the peer; the namespace
  # is what the connection is FOR), never from anything the payload claims.
  # Cross-namespace action is impossible by construction: the bound namespace
  # IS the handshake namespace. Also the per-namespace `max_runners` quota
  # point — both refusals are observable through the authz denial event /
  # the quota telemetry below.
  defp admit_runner(namespace, transport) do
    runner_ctx = runner_caller_context(namespace)

    with :ok <-
           Flezha.AuthZ.authorize(
             runner_ctx,
             :"runner.connect",
             Scope.build(namespace)
           ),
         :ok <- Quotas.check_allocation(namespace, :max_runners) do
      :ok
    else
      {:error, {:quota_exceeded, _kind, %{current: current, limit: limit}} = reason} ->
        Logger.warning(
          "[FleetManager] runner connect refused for namespace=#{namespace}: " <>
            "max_runners quota reached (#{current}/#{limit})"
        )

        :telemetry.execute(
          [:flezha, :runner, :quota_refused],
          %{count: 1},
          %{namespace: namespace, transport: transport, current: current, limit: limit}
        )

        {:error, reason}

      {:error, reason} ->
        Logger.warning(
          "[FleetManager] runner connect refused for namespace=#{namespace}: #{inspect(reason)}"
        )

        {:error, reason}
    end
  end

  @doc """
  The caller context service calls attributed to a runner connection carry:
  a `runner`-role principal bound to the connection's handshake namespace.
  """
  @spec runner_caller_context(binary()) :: Flezha.AuthZ.caller_context()
  def runner_caller_context(namespace) do
    %{
      source: :sdk,
      principal: "runner@" <> namespace,
      namespace: namespace,
      role: :runner
    }
  end

  # Pollers pass a short timeout so a FleetManager busy inside `runner_connect/3`
  # costs them one attempt rather than their whole deadline.
  def active_runners(namespace, timeout \\ @lookup_timeout_ms) do
    GenServer.call(__MODULE__, {:active_runners, namespace}, timeout)
  end

  @doc """
  Atomically evict a dead/orphaned runner binding: clear every operative's
  stale stream mapping (`HubManager.hook_remove_pid/2`), stop its runner
  supervisor, drop its factory refs + operative-insertion hook, and remove it
  from `runner_namespaces`. Its operatives become misbound and rebind onto
  a live runner on the next convergence tick. Idempotent — an unknown
  id returns `:ok`. Driven by `Flezha.Convergence.Heal.execute/1` and by the
  reconnect binding-swap in `runner_connect` (to close the prior session).

  `proved_wedged: true` waives the "never evict a live runner" guard. The
  guard exists to protect a HEALTHY runner from a stale snapshot; a runner the
  data-plane classifier caught still connected and still heartbeating while
  processing nothing is not healthy, and its connection being open is the
  reason nothing else can dislodge it.
  """
  @spec evict_runner(binary(), keyword()) :: :ok
  def evict_runner(runner_id, opts \\ []) when is_binary(runner_id) do
    GenServer.call(
      __MODULE__,
      {:evict_runner, runner_id, Keyword.get(opts, :proved_wedged, false)}
    )
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
    ensure_marks_table(@superseded_table)
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
  Record that the controller is itself closing `runner_id`, so the close its
  sockets report is diagnosed as expected rather than as an incident.

  Read from the `{:flz_closed, …}` clauses, which is why it is a bare ETS read
  and not a call: a channel asking this GenServer whether its own close was
  expected would be asking the process that is busy tearing it down.

  The mark expires. `runner_id` is a durable stable identity that survives
  reconnects, so a mark that outlived its disconnect would silence the
  diagnosis of a later, genuine close of the same runner.
  """
  @spec mark_runner_retiring(binary()) :: :ok
  def mark_runner_retiring(runner_id) when is_binary(runner_id) do
    ensure_marks_table(@retiring_table)
    :ets.insert(@retiring_table, {runner_id, :erlang.monotonic_time(:millisecond)})
    :ok
  end

  @doc "Whether the controller asked for `runner_id`'s close, within the mark's TTL."
  @spec runner_retiring?(binary()) :: boolean()
  def runner_retiring?(runner_id) when is_binary(runner_id) do
    with true <- :ets.whereis(@retiring_table) != :undefined,
         [{_, marked_at}] <- :ets.lookup(@retiring_table, runner_id) do
      :erlang.monotonic_time(:millisecond) - marked_at < retiring_ttl_ms()
    else
      _ -> false
    end
  end

  @doc "Drop `runner_id`'s retiring mark; the disconnect it covered is done."
  @spec clear_runner_retiring(binary()) :: :ok
  def clear_runner_retiring(runner_id) when is_binary(runner_id) do
    if :ets.whereis(@retiring_table) != :undefined,
      do: :ets.delete(@retiring_table, runner_id)

    :ok
  end

  defp retiring_ttl_ms(),
    do: Application.get_env(:flezha, :runner_retiring_mark_ttl_ms, 30_000)

  @doc """
  Returns active compute engines in `namespace` that advertise `factory_id`.

  Used by `OperativeManager.assign_runner/3` so we never route a
  operative to a node that doesn't own its factory — without this filter
  every input frame for a misrouted operative hits a dead registry entry
  on the foreign node and generates a `enqueue: operative X not found`
  warning on every input.
  """
  def runners_for_factory(namespace, factory_id, timeout \\ @lookup_timeout_ms) do
    GenServer.call(__MODULE__, {:runners_for_factory, namespace, factory_id}, timeout)
  end

  @doc """
  Why `factory_id` has no placement candidate in `namespace`.

  `:none` when some runner can take it; `:no_such_factory` when no runner
  advertises the id at all; `{:language, established}` when runners advertise
  it in another language than the one the factory is established at — a
  factory that exists and is simply unplaceable here, which must not be
  reported as a factory that does not exist.
  """
  @spec placement_refusal(String.t(), String.t(), timeout()) ::
          :none
          | :no_such_factory
          | :ambiguous_language
          | :no_language_declared
          | {:language, byte()}
  def placement_refusal(namespace, factory_id, timeout \\ @lookup_timeout_ms) do
    GenServer.call(__MODULE__, {:placement_refusal, namespace, factory_id}, timeout)
  end

  @doc """
  Runner ids across every cluster node holding active runners in `namespace`,
  resolved through the cluster directory. Serves `Flezha.list_runners/1`.
  An unreachable peer contributes nothing rather than failing the read.
  """
  @spec global_runners(String.t()) :: {:ok, [binary()]}
  def global_runners(namespace) do
    runners =
      [node() | Directory.runner_nodes(namespace)]
      |> Enum.uniq()
      |> Enum.flat_map(fn n ->
        try do
          :erpc.call(n, __MODULE__, :active_runners, [namespace], @lookup_timeout_ms)
        catch
          _, _ -> []
        end
      end)
      |> Enum.uniq()

    {:ok, runners}
  end

  def disconnect_runner(namespace, runner_id) do
    GenServer.call(__MODULE__, {:disconnect_runner, namespace, runner_id}, manager_call_timeout())
  end

  @doc """
  Clear the `stream_ref` on this runner's `ControlChannel` so the existing
  rebind machinery (see `runner-controller-connection-healing`) picks up
  the next data stream the SDK opens. Called by
  `Flezha.Operative.Overwatch` after an end-to-end ping fails, indicating the
  controller-side `ControlChannel` is alive but its underlying transport
  handle is stale.

  Returns `:ok` on success, `{:error, :no_control_channel}` if the channel
  GenServer is not registered for `runner_id`.
  """
  @spec reset_control_channel(binary()) :: :ok | {:error, term()}
  def reset_control_channel(runner_id) when is_binary(runner_id) do
    ControlChannel.reset_stream_ref(runner_id)
  end

  @doc """
  Re-issue `ConnectSocket` to `runner_id` so the SDK re-opens its data
  sockets; the `StreamAcceptor` binds the fresh streams to the unbound
  `StreamChannel`s (see `runner-controller-connection-healing`). Used by the
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
    GenServer.call(__MODULE__, {:stop_namespace, namespace}, manager_call_timeout())
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
    ensure_marks_table(@superseded_table)

    {:ok,
     %{
       runner_namespaces: %{},
       namespace_factories: %{},
       stable_sessions: %{},
       directory_groups: MapSet.new()
     }}
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

        # Marked BEFORE the close is issued: the mark has to be committed by
        # the time `{:flz_closed, …}` reaches the channels, or they diagnose
        # a teardown we asked for as one we did not.
        mark_runner_retiring(runner_id)
        ControlUnit.disconnect(runner_id)

        {:reply, :ok, state}
    end
  end

  @impl true
  def handle_call({:runner_disconnected, namespace, runner_id}, _from, state) do
    state = handle_runner_disconnect(state, namespace, runner_id)

    {:reply, :ok, sync_directory(state)}
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
  def handle_call({:placement_refusal, namespace, factory_id}, _from, state) do
    {info, advertising} = advertising_runners(state, namespace, factory_id)

    # The message must read the same derivation as the decision, or the two
    # drift exactly where the record is missing and the operator is told a
    # factory is established at a language nobody ever established.
    refusal =
      cond do
        not Enum.empty?(placeable_runners(state, namespace, factory_id)) ->
          :none

        Enum.empty?(advertising) ->
          :no_such_factory

        true ->
          case established_lang(namespace, factory_id, info, advertising) do
            {:ok, lang} -> {:language, lang}
            :ambiguous -> :ambiguous_language
            :none -> :no_language_declared
          end
      end

    {:reply, refusal, state}
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
            mark_runner_retiring(runner_id)
            FleetSupervisor.stop_runner_sup(runner_id)
          end)

          # Drop every plugin registered for this namespace so a follow-up
          # test re-using the same namespace does not collide on
          # `{:error, "plugin already registered"}`. The runtime operative
          # processes are about to disappear with the namespace's hub —
          # the registry is the only thing that survives without explicit
          # cleanup.
          _ = PluginManager.disable_namespace_plugins(namespace)

          HubManager.stop_hub(namespace)

          {:ok, Map.delete(state.runner_namespaces, namespace)}
      end

    state = Map.put(state, :runner_namespaces, runner_namespaces)
    state = Map.update!(state, :namespace_factories, &Map.delete(&1, namespace))
    {:reply, reply, sync_directory(state)}
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
    # tree. That tree can fail to come up for reasons that belong to this runner
    # alone — an acceptor whose socket the kernel refuses, for one — and this
    # manager serves every other runner on the node, so the connect is refused
    # rather than allowed to take the manager (and the transport server linked
    # to it) down.
    case catch_runner_connect(stream_ref, namespace, transport) do
      {:error, reason} ->
        {:reply, {:error, reason}, state}

      {:ok, namespace, session_token} ->
        finish_runner_connect(state, namespace, session_token, factories, stable_id)
    end
  end

  @impl true
  def handle_call({:evict_runner, runner_id, proved_wedged}, _from, state) do
    # Safe no-op on a healthy target: never evict a runner that is
    # currently `:active` (connection open). The convergence Diff runs off
    # a snapshot that can lag a reconnect by a tick; acting on that stale
    # view is exactly the false-eviction → cascade-rebind regression this
    # change exists to remove. The authoritative live set is the gate.
    # (The reconnect binding-swap closes a prior session by id directly, not
    # through this guarded path.)
    if not proved_wedged and runner_active?(state, runner_id) do
      {:reply, :ok, state}
    else
      {:reply, :ok, state |> do_evict_runner(runner_id) |> sync_directory()}
    end
  end

  @impl true
  def handle_call(:runner_cleanup, _from, state) do
    {:reply, :ok, state |> runner_cleanup() |> sync_directory()}
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
      |> prune_empty_factories(namespace)

    warn_dropped_factories(state, namespace, runner_id, dropped)

    {:noreply, sync_directory(state)}
  end

  @impl true
  def handle_info(:schedule_runner_cleanup, state) do
    schedule_runner_cleanup()

    {:noreply, state |> runner_cleanup() |> sync_directory()}
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
    prune_marks()

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
              # Reap the dead runner ref regardless; the hub is reclaimed only
              # when it holds no (suspended) operatives.
              HubManager.stop_hub_if_vacant(namespace)
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
      removal_runners
      |> Enum.reduce(acc, fn
        nil, acc -> acc
        ce_id, acc -> remove_ce_factories(acc, namespace, ce_id)
      end)
      |> prune_empty_factories(namespace)
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
  # (newest-wins, `runner-identity-and-session` D5). A first connect just
  # records the mapping; a degraded runner with no stable id connects fresh.
  defp resolve_stable_identity(state, _namespace, nil, _session_token), do: state

  defp resolve_stable_identity(state, namespace, stable_id, session_token) do
    state =
      case Map.get(state.stable_sessions, stable_id) do
        old_session when is_binary(old_session) and old_session != session_token ->
          # Unbind-old BEFORE bind-new: once the fence returns, nothing the
          # superseded session forwards can land in any Entity WAL, so no
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
  # off the Entity write path. Bounded calls — a wedged channel must not
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

  defp ensure_marks_table(table) do
    case :ets.whereis(table) do
      :undefined ->
        try do
          :ets.new(table, [:set, :public, :named_table, read_concurrency: true])
        rescue
          ArgumentError -> :ok
        end

        :ok

      _ ->
        :ok
    end
  end

  defp prune_marks() do
    for {table, ttl} <- [
          {@superseded_table, @superseded_ttl_ms},
          {@retiring_table, retiring_ttl_ms()}
        ],
        :ets.whereis(table) != :undefined do
      cutoff = :erlang.monotonic_time(:millisecond) - ttl
      :ets.select_delete(table, [{{:_, :"$1"}, [{:<, :"$1", cutoff}], [true]}])
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
        |> prune_empty_factories(namespace)
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
    clear_runner_retiring(runner_id)
    FleetSupervisor.stop_runner_sup(runner_id)

    state =
      state
      |> remove_ce_factories(namespace, runner_id)
      |> prune_empty_factories(namespace)

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
          runner_reroute(state, namespace, runner_id)

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
  # and operatives get assigned to a dead node (stream ghost accumulation).
  defp drop_runner(state, namespace, runner_id) do
    Map.delete(state.runner_namespaces[namespace] || %{}, runner_id)
  end

  # This controller has no runner left for the namespace but peers do, so its
  # operatives cannot run here — they must move to a controller that can host
  # them. The drain migrates each one through the consent gate with live
  # handover; the destination pulls the durable files it cannot otherwise reach
  # (`Flezha.State.WalTransfer`), since `state_root` is node-local.
  #
  # Off-process: the drain blocks to its deadline and this runs inside the
  # FleetManager call that the disconnecting runner is waiting on.
  #
  # Deliberately a SHORT budget, unlike graceful shutdown's. A drain takes this
  # node out of the namespace's distribution for its whole duration, and this
  # path is opportunistic: a runner may reconnect here seconds later, and the
  # namespace may even be torn down and restarted underneath. Whatever does not
  # migrate inside the window simply stays, visible and misbound, for the
  # convergence loop — so a long budget buys nothing and widens the window in
  # which a returning runner finds its own node still drained.
  @migrate_drain_budget_ms 5_000

  defp hub_migrate(_state, namespace, _runner_id) do
    spawn(fn ->
      _ = HubManager.drain_hub(namespace, budget_ms: @migrate_drain_budget_ms)
      HubManager.stop_hub_if_vacant(namespace)
    end)

    %{}
  end

  defp runner_preserve(state, namespace, runner_id) do
    runner = {:inactive, DateTime.utc_now() |> DateTime.to_unix(:millisecond)}

    put_in(state.runner_namespaces[namespace], [runner_id], runner)
  end

  defp runner_reroute(state, namespace, old_runner) do
    # Collected BEFORE the drop, while the tag query still names the dead
    # runner's operatives. The hub (and its ETS table) can be down or
    # mid-restart during churn — that must never abort the dead runner's
    # removal from the active set below; the operatives heal on the next
    # convergence tick instead.
    operatives =
      try do
        HubManager.runner_operatives(namespace, old_runner)
      rescue
        _ -> []
      catch
        :exit, _ -> []
      end

    # Off-process: each rebind resolves placement back through this
    # FleetManager (`assign_runner/3`), which would deadlock inside this
    # handler. The spawned work's calls queue behind the current one, so they
    # observe the post-drop active set — the dead runner is no longer a
    # placement candidate by the time anything is placed.
    spawn(fn -> reroute_operatives(namespace, old_runner, operatives) end)

    drop_runner(state, namespace, old_runner)
  end

  # Per-operative confirmed rebinds through the ONE migrate-commit contract
  # (`Heal.rebind/2`): factory-filtered HRW placement per operative, registry
  # flip and stream-channel seeding only after the operative confirms the
  # migrate, WAL resync into the new binding. An unconfirmed operative keeps
  # its binding and heals on the next convergence tick.
  defp reroute_operatives(namespace, old_runner, operatives) do
    Enum.each(operatives, fn {operative_id, node_pids} ->
      # Multi-node-safe: resolve the live/local pid (dropping dead-node
      # entries) first, so a operative registered on more than one node
      # neither crashes this path nor blocks on a dead pid.
      case HubManager.live_pid(node_pids) do
        nil ->
          Logger.debug(
            "[FleetManager] runner_reroute: no live pid for operative #{operative_id}; " <>
              "it will migrate on its next bind/rebind"
          )

        _operative_pid ->
          reroute_one(namespace, old_runner, operative_id)
      end
    end)
  end

  # Isolated per operative: one raising rebind (ProcessHub or a StreamChannel
  # mid-restart under churn) must not strand its siblings.
  defp reroute_one(namespace, old_runner, operative_id) do
    case Heal.rebind(namespace, operative_id) do
      :ok -> :ok
      :error -> log_reroute_miss(operative_id, old_runner, "did not confirm")
    end
  rescue
    error -> log_reroute_miss(operative_id, old_runner, "raised #{inspect(error)}")
  catch
    :exit, reason -> log_reroute_miss(operative_id, old_runner, "exited #{inspect(reason)}")
  end

  defp log_reroute_miss(operative_id, old_runner, what) do
    Logger.warning(
      "[FleetManager] reroute of #{operative_id} off #{old_runner} #{what}; " <>
        "binding left unchanged, heals on next convergence tick"
    )
  end

  # runner_connect/3 may block inside Orchestrator.await_cluster_ready/0
  # (bounded by :cluster_formation_timeout_ms, default 15s) while the cluster
  # forms after a restart. The connect call must outlast that window plus the
  # rest of the connect work, or the caller's default 5s timeout fires and
  # strands every runner that connects during formation.
  # Every call lands on this one serialized process, and a connect is allowed to
  # take `runner_connect_timeout/0` — so a caller waiting the `GenServer.call`
  # default of 5 s times out whenever it arrives while a connect is in flight.
  # That is not contention to ride out: the manager is permitted to be busy for
  # longer than the caller was willing to wait. Every caller gets the same
  # budget as the work the manager may be doing.
  defp manager_call_timeout, do: runner_connect_timeout()

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

    # Transfer ownership of the stream to the ControlChannel process. A runner
    # whose tree did not come up is refused rather than allowed to crash this
    # manager, which serves every other runner on the node.
    control_channel_pid =
      case RunnerSupervisor.find_control_channel(runner_id) do
        {:ok, pid} ->
          pid

        {:error, reason} ->
          Logger.error(
            "runner #{runner_id} on #{namespace} has no control channel " <>
              "(#{inspect(reason)}); refusing the connect"
          )

          throw({:runner_connect_failed, reason})
      end

    Flezha.Transport.Stream.controlling_process(stream_ref, control_channel_pid)
    Flezha.Transport.Stream.set_active(stream_ref)

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

  # Re-advertise this node's capability into the cluster directory from the
  # state just committed. Desired-state diff, not refcounts: a group stays
  # joined while ANY runner on this node still backs it, and a crashed
  # FleetManager's memberships are cleaned by `:pg`'s monitors.
  defp sync_directory(state) do
    sync_runner_counters(state)

    %{
      state
      | directory_groups:
          Directory.sync_membership(desired_directory_groups(state), state.directory_groups)
    }
  end

  # Every session mutation ends in `sync_directory/1`, so the per-namespace
  # `:runners` quota counter is DERIVED from the live session map here rather
  # than incremented/decremented at each of the several removal seams — the
  # counter cannot drift from the truth it is quota-checked against. A
  # briefly-down counter store costs one sync, never the FleetManager.
  defp sync_runner_counters(state) do
    if Flezha.AuthZ.enabled?() do
      Enum.each(state.runner_namespaces, fn {namespace, runners} ->
        active = Enum.count(runners, fn {_id, {status, _ts}} -> status == :active end)
        QuotaCounters.set(namespace, :runners, active)
      end)
    end

    :ok
  catch
    _, _ -> :ok
  end

  defp desired_directory_groups(state) do
    runner_groups =
      for {namespace, runners} <- state.runner_namespaces,
          Enum.any?(runners, fn {_id, {status, _ts}} -> status == :active end),
          do: Directory.runners_group(namespace)

    factory_groups =
      for {namespace, factories} <- state.namespace_factories,
          {factory_id, _info} <- factories,
          do: Directory.factory_group(namespace, factory_id)

    MapSet.new(runner_groups ++ factory_groups)
  end

  defp store_factories(state, namespace, runner_id, factories) do
    ns_factories = Map.get(state.namespace_factories, namespace, %{})

    ns_factories =
      Enum.reduce(factories, ns_factories, fn factory, acc ->
        factory_id = factory["id"]

        existing = factory_entry(Map.get(acc, factory_id))
        description = factory["description"] || existing.description
        advertised = advertised_lang(factory)

        # Stored even when it conflicts, so operator tooling can see the
        # runner that is advertising the wrong language; `placeable_runners/3`
        # is what refuses it.
        _ = establish_lang(namespace, runner_id, factory_id, advertised)

        Map.put(acc, factory_id, %{
          description: description,
          ce_ids: MapSet.put(existing.ce_ids, runner_id),
          langs: Map.put(existing.langs, runner_id, advertised)
        })
      end)

    put_in(state, [:namespace_factories, namespace], ns_factories)
  end

  defp remove_ce_factories(state, namespace, runner_id) do
    ns_factories = Map.get(state.namespace_factories, namespace, %{})

    ns_factories =
      ns_factories
      |> Map.new(fn {factory_id, info} ->
        info = factory_entry(info)

        {factory_id,
         %{
           info
           | ce_ids: MapSet.delete(info.ce_ids, runner_id),
             langs: Map.delete(info.langs, runner_id)
         }}
      end)

    put_in(state, [:namespace_factories, namespace], ns_factories)
  end

  # Drop the factories no runner advertises any more, releasing the language
  # each established. A pass of its own, run AFTER any store: a full-set resync
  # removes the runner's whole list before re-adding it, and releasing inside
  # the removal would discard a language the durable header had corrected.
  defp prune_empty_factories(state, namespace) do
    {empty, kept} =
      state.namespace_factories
      |> Map.get(namespace, %{})
      |> Map.split_with(fn {_factory_id, info} -> MapSet.size(info.ce_ids) == 0 end)

    Enum.each(Map.keys(empty), &ClassRegistry.delete_sdk_lang(namespace, &1))
    put_in(state, [:namespace_factories, namespace], kept)
  end

  # The one constructor for a stored factory entry. Total on an older entry: a
  # `KeyError` here would take the whole FleetManager down over a shape
  # question, and every placement lookup with it.
  defp factory_entry(nil), do: %{description: nil, ce_ids: MapSet.new(), langs: %{}}

  defp factory_entry(%{} = info) do
    info
    |> Map.put_new(:description, nil)
    |> Map.put_new(:ce_ids, MapSet.new())
    |> Map.put_new(:langs, %{})
  end

  # The language a factory entry advertises. Absent / `0` stays `0`: an
  # unspecified language is refused at the placement filter, never guessed.
  defp advertised_lang(factory) do
    case factory["sdk_lang"] do
      lang when is_integer(lang) and lang > 0 -> lang
      _ -> @sdk_lang_unspecified
    end
  end

  # An advertisement establishes the factory's language if nothing has yet, and
  # is reported when it disagrees. The one place a language is RECORDED — the
  # record is a durable sidecar, so writing it belongs on this path and never
  # in a query.
  #
  # The durable half of the resolution is read at the BIND
  # (`Flezha.Operative.Lifecycle`): a disk walk here would stall every
  # concurrent placement lookup behind the connect path.
  defp establish_lang(_ns, _runner_id, _factory_id, @sdk_lang_unspecified), do: :ok

  defp establish_lang(namespace, runner_id, factory_id, advertised) do
    case ClassRegistry.sdk_lang(namespace, factory_id) do
      nil ->
        ClassRegistry.put_sdk_lang(namespace, factory_id, advertised)

      established when established != advertised ->
        Logger.warning(
          "[fleet_manager] runner #{runner_id} on #{namespace} advertises factory " <>
            "#{factory_id} at sdk_lang #{advertised}, but the factory is established " <>
            "at #{established}; it will not be a placement candidate for it — durable " <>
            "State/Change bodies are per-SDK and are never decoded across languages"
        )

        :telemetry.execute(
          [:flezha, :runner, :factory_language_conflict],
          %{count: 1},
          %{
            namespace: namespace,
            runner: runner_id,
            factory: factory_id,
            established: established,
            advertised: advertised
          }
        )

      _agreed ->
        :ok
    end
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

  # ACTIVE runners advertising `factory_id` **in that factory's established SDK
  # language** — the one filter every placement path reads (initial start, the
  # convergence rebind, the reroute off a dead runner), so a future caller
  # inherits the guard rather than having to remember it.
  #
  # The homogeneous unit is the FACTORY, not the namespace: a restore ships one
  # SDK's bytes verbatim to whichever runner bound the Entity, and only one
  # factory id's implementations must never mix.
  defp placeable_runners(state, namespace, factory_id) do
    {info, candidates} = advertising_runners(state, namespace, factory_id)

    case established_lang(namespace, factory_id, info, candidates) do
      # An entry carrying no language never makes its runner a candidate, and
      # disagreeing advertisements with nothing recorded leave the controller
      # unable to say which language owns the state. Neither places: durable
      # State/Change bodies are per-SDK and are never decoded across languages.
      reason when reason in [:none, :ambiguous] ->
        MapSet.new()

      {:ok, established} ->
        MapSet.filter(
          candidates,
          &(Map.get(info.langs, &1, @sdk_lang_unspecified) == established)
        )
    end
  end

  # The stored entry and the ACTIVE runners advertising `factory_id` at any
  # language — what separates "the factory exists in another language" from "no
  # runner has this factory at all". `placeable_runners/3` is this set narrowed
  # to one language, so the two never disagree on who is even a candidate.
  defp advertising_runners(state, namespace, factory_id) do
    active_ids = MapSet.new(get_runner_ids(state, namespace, :active))

    info =
      state.namespace_factories
      |> Map.get(namespace, %{})
      |> Map.get(factory_id)
      |> factory_entry()

    {info, MapSet.intersection(info.ce_ids, active_ids)}
  end

  # A factory's established language: the record if there is one, else what its
  # own live advertisements say. Both halves of the record are losable (the ETS
  # table is recreated empty if its owner restarts, the sidecar goes with the
  # factory's state dir), and a missing record is no evidence of a conflict —
  # so it must not read as one. Derives, never records: this is a query.
  #
  # Unanimity required: disagreeing advertisements with nothing recorded mean
  # the controller cannot tell which language owns the factory.
  defp established_lang(namespace, factory_id, info, candidates) do
    case ClassRegistry.sdk_lang(namespace, factory_id) do
      nil ->
        case advertised_langs(info, candidates) do
          [] -> :none
          [lang] -> {:ok, lang}
          _conflicting -> :ambiguous
        end

      recorded ->
        {:ok, recorded}
    end
  end

  # The distinct non-zero languages the live candidates advertise. An
  # unspecified advertisement contributes nothing: it can neither establish a
  # factory nor make one ambiguous.
  defp advertised_langs(info, candidates) do
    candidates
    |> Enum.map(&Map.get(info.langs, &1, @sdk_lang_unspecified))
    |> Enum.reject(&(&1 == @sdk_lang_unspecified))
    |> Enum.uniq()
  end

  defp catch_runner_connect(stream_ref, namespace, transport) do
    runner_connect(stream_ref, namespace, transport)
  catch
    {:runner_connect_failed, reason} -> {:error, reason}
  end

  defp finish_runner_connect(state, namespace, session_token, factories, stable_id) do
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

    {:reply, {:ok, {namespace, session_token}}, sync_directory(state)}
  end
end
