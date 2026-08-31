defmodule Flezha.Runner.FleetManager.Session do
  @moduledoc """
  One runner session, from the handshake to the last operative rerouted off it.

  A session token is minted per connection; a `stable_id` that already has a
  live session is resolved newest-wins — the old token is fenced, its operative
  bindings swap onto the new one, and the old session is evicted.

  Invariant: a disconnect drops the runner from the active set unconditionally.
  What varies is only what becomes of the operatives it held — preserved,
  migrated or rerouted.
  """

  alias Flezha.AuthZ.QuotaCounters
  alias Flezha.AuthZ.Quotas
  alias Flezha.AuthZ.Scope
  alias Flezha.Cluster.Directory
  alias Flezha.Cluster.Orchestrator
  alias Flezha.Convergence.Heal
  alias Flezha.Identificator
  alias Flezha.Runner.FleetManager.Factories
  alias Flezha.Runner.FleetManager.Marks
  alias Flezha.Runner.FleetManager.Roster
  alias Flezha.Runner.FleetSupervisor
  alias Flezha.Runner.RunnerSupervisor
  alias Flezha.Runner.StreamPool
  alias Flezha.SafeCall
  alias Flezha.Service.EventManager
  alias Flezha.Service.HubManager
  alias Flezha.Shutdown.Coordinator, as: ShutdownCoordinator

  require Logger

  @migrate_drain_budget_ms 5_000

  def check_not_shutting_down(namespace) do
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
  def admit_runner(namespace, transport) do
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

  @spec runner_caller_context(binary()) :: Flezha.AuthZ.caller_context()
  def runner_caller_context(namespace) do
    %{
      source: :sdk,
      principal: "runner@" <> namespace,
      namespace: namespace,
      role: :runner
    }
  end

  # Reconnect/takeover binding swap. The SDK presents the same persisted stable
  # id on every connect; the controller keys operatives on the per-connection
  # session token, so a reconnect re-points the prior session's bindings onto
  # the new token (flip the Registry tag + seed the new StreamChannels — the
  # same primitives the convergence Heal uses) and then closes the old session
  # (newest-wins). A first connect just
  # records the mapping; a degraded runner with no stable id connects fresh.
  def resolve_stable_identity(state, _namespace, nil, _session_token), do: state

  def resolve_stable_identity(state, namespace, stable_id, session_token) do
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
  def fence_superseded_session(old_session) do
    Marks.mark_session_superseded(old_session)

    Enum.each(stream_channel_pids(old_session), fn pid ->
      _ = SafeCall.bounded(pid, :fence_write_path, 1_000)
    end)
  end

  # Resolve the session's fixed channel-name set (the same one `StreamSup`
  # spawns); a dead/unregistered channel forwards nothing, so it is skipped.
  def stream_channel_pids(session_token) do
    for type <- [:primary, :secondary],
        name <- StreamPool.stream_ids(session_token, type),
        pid = GenServer.whereis(name),
        is_pid(pid),
        do: pid
  end

  # Re-tag every operative still bound to `old_session` onto `new_session` and
  # seed the new session's pre-spawned StreamChannels so in-flight frames land
  # without waiting for a convergence tick. `do_evict_runner/2` then tears down
  # the old (now operative-less) session.
  def swap_bindings(namespace, old_session, new_session) do
    if HubManager.hub_alive?(namespace) do
      HubManager.runner_operatives(namespace, old_session)
      |> Enum.each(&swap_one_binding(namespace, new_session, &1))
    end
  end

  defp swap_one_binding(namespace, new_session, {operative_id, node_pids}) do
    HubManager.operative_set_runner(namespace, operative_id, new_session)

    case HubManager.live_pid(node_pids) do
      nil -> :ok
      pid -> HubManager.hook_insert_local_pid(new_session, operative_id, pid)
    end
  end

  def do_evict_runner(state, runner_id) do
    case Roster.find_runner_namespace(state, runner_id) do
      nil ->
        state

      namespace ->
        detach_runner_operatives(namespace, runner_id)

        stop_runner_sup(runner_id)

        :telemetry.execute(
          [:flezha, :runner, :evicted],
          %{count: 1},
          %{namespace: namespace, runner_id: runner_id}
        )

        state
        |> Factories.remove_ce_factories(namespace, runner_id)
        |> Factories.prune_empty_factories(namespace)
        |> update_in([:runner_namespaces, namespace], &Map.delete(&1 || %{}, runner_id))
    end
  end

  def handle_runner_disconnect(state, namespace, runner_id) do
    if Map.has_key?(state.runner_namespaces, namespace) do
      do_handle_runner_disconnect(state, namespace, runner_id)
    else
      state
    end
  end

  defp detach_runner_operatives(namespace, runner_id) do
    if HubManager.hub_alive?(namespace) do
      HubManager.runner_operatives(namespace, runner_id)
      |> Enum.each(fn {operative_id, _node_pids} ->
        HubManager.hook_remove_pid(runner_id, %{child_id: operative_id})
      end)

      HubManager.unregister_handler(namespace, runner_id)
    end
  end

  defp set_runner_counter({namespace, runners}) do
    active = Enum.count(runners, fn {_id, {status, _ts}} -> status == :active end)
    QuotaCounters.set(namespace, :runners, active)
  end

  def stop_runner_sup(runner_id) do
    Supervisor.terminate_child(FleetSupervisor, runner_id)
    Supervisor.delete_child(FleetSupervisor, runner_id)
  end

  def do_handle_runner_disconnect(state, namespace, runner_id) do
    Marks.clear_runner_retiring(runner_id)
    stop_runner_sup(runner_id)

    state =
      state
      |> Factories.remove_ce_factories(namespace, runner_id)
      |> Factories.prune_empty_factories(namespace)

    if HubManager.hub_alive?(namespace) do
      HubManager.unregister_handler(namespace, runner_id)
    end

    runner_ids =
      state
      |> Roster.get_runner_ids(namespace, :active)
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
  def drop_runner(state, namespace, runner_id) do
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

  def hub_migrate(_state, namespace, _runner_id) do
    spawn(fn ->
      _ = HubManager.drain_hub(namespace, budget_ms: @migrate_drain_budget_ms)
      HubManager.stop_hub_if_vacant(namespace)
    end)

    %{}
  end

  def runner_preserve(state, namespace, runner_id) do
    runner = {:inactive, DateTime.utc_now() |> DateTime.to_unix(:millisecond)}

    put_in(state.runner_namespaces[namespace], [runner_id], runner)
  end

  def runner_reroute(state, namespace, old_runner) do
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
    # observe the post-drop active set — the dead runner is absent from it
    # by the time anything is placed.
    spawn(fn -> reroute_operatives(namespace, old_runner, operatives) end)

    drop_runner(state, namespace, old_runner)
  end

  # Per-operative confirmed rebinds through the ONE migrate-commit contract
  # (`Heal.rebind/2`): factory-filtered HRW placement per operative, registry
  # flip and stream-channel seeding only after the operative confirms the
  # migrate, WAL resync into the new binding. An unconfirmed operative keeps
  # its binding and heals on the next convergence tick.
  def reroute_operatives(namespace, old_runner, operatives) do
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
  def reroute_one(namespace, old_runner, operative_id) do
    case Heal.rebind(namespace, operative_id) do
      :ok -> :ok
      :error -> log_reroute_miss(operative_id, old_runner, "did not confirm")
    end
  rescue
    error -> log_reroute_miss(operative_id, old_runner, "raised #{inspect(error)}")
  catch
    :exit, reason -> log_reroute_miss(operative_id, old_runner, "exited #{inspect(reason)}")
  end

  def log_reroute_miss(operative_id, old_runner, what) do
    Logger.warning(
      "[FleetManager] reroute of #{operative_id} off #{old_runner} #{what}; " <>
        "binding left unchanged, heals on next convergence tick"
    )
  end

  def runner_connect(stream_ref, namespace, transport) do
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

    Supervisor.start_child(FleetSupervisor, %{
      id: runner_id,
      start: {RunnerSupervisor, :start_link, [{namespace, runner_id, stream_ref, transport}]}
    })

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
  def parse_stable_id(init_payload) do
    case Map.get(init_payload, "stable_id") do
      id when is_binary(id) and id != "" -> id
      _ -> nil
    end
  end

  def parse_factories(init_payload) do
    Map.get(init_payload, "factories") || []
  end

  # Re-advertise this node's capability into the cluster directory from the
  # state just committed. Desired-state diff, not refcounts: a group stays
  # joined while ANY runner on this node still backs it, and a crashed
  # FleetManager's memberships are cleaned by `:pg`'s monitors.
  def sync_directory(state) do
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
  def sync_runner_counters(state) do
    if Flezha.AuthZ.enabled?(), do: Enum.each(state.runner_namespaces, &set_runner_counter/1)

    :ok
  catch
    _, _ -> :ok
  end

  def desired_directory_groups(state) do
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

  def catch_runner_connect(stream_ref, namespace, transport) do
    runner_connect(stream_ref, namespace, transport)
  catch
    {:runner_connect_failed, reason} -> {:error, reason}
  end

  def finish_runner_connect(state, namespace, session_token, factories, stable_id) do
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

    state = Factories.store_factories(state, namespace, session_token, factories)

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
