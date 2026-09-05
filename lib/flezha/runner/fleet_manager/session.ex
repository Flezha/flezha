defmodule Flezha.Runner.FleetManager.Session do
  @moduledoc """
  One runner session, from the handshake to the disconnect that ends it.

  A session token is minted per connection; a `stable_id` that already has a
  live session is resolved newest-wins — the old token is fenced, its operative
  bindings swap onto the new one, and the old session is evicted.

  Invariant: a disconnect drops the runner from the active set unconditionally
  and is declared to the convergence loop, which decides what becomes of the
  operatives it held. The one thing this module keeps is the keepalive: the
  last runner of a namespace nobody else serves is held `:inactive` for its
  window rather than dropped.
  """

  alias Flezha.AuthZ.QuotaCounters
  alias Flezha.AuthZ.Quotas
  alias Flezha.AuthZ.Verb
  alias Flezha.Cluster.Directory
  alias Flezha.Cluster.Orchestrator
  alias Flezha.Config.Durations
  alias Flezha.Convergence.Facts
  alias Flezha.Identificator
  alias Flezha.Process.Cspec
  alias Flezha.Runner.FleetManager.Factories
  alias Flezha.Runner.FleetManager.Marks
  alias Flezha.Runner.FleetManager.Roster
  alias Flezha.Runner.FleetSupervisor
  alias Flezha.Runner.RunnerSupervisor
  alias Flezha.Runner.StreamPool
  alias Flezha.SafeCall
  alias Flezha.Service.EventManager
  alias Flezha.Service.HubManager
  alias Flezha.Shutdown

  require Logger

  @behaviour Flezha.Shutdown.Step

  # The `:namespace` walk's runners step: every active runner of the namespace
  # is marked retiring — so its channels read the close as one we asked for —
  # and its supervision tree stopped. The fleet manager drops the namespace from
  # its roster once the walk returns.
  @impl Flezha.Shutdown.Step
  def shutdown_steps do
    [%{scope: :namespace, order: 20, name: :runners, run: &runners_step/2}]
  end

  defp runners_step(%{runner_ids: runner_ids}, _budget) do
    Enum.each(runner_ids, fn runner_id ->
      Marks.mark_runner_retiring(runner_id)
      stop_runner_sup(runner_id)
    end)

    :ok
  end

  def check_not_shutting_down(namespace) do
    with {:error, :shutting_down} = refusal <- Shutdown.admit(:namespace, namespace) do
      Logger.info("[shutdown] rejecting runner connect attempt for namespace=#{namespace}")
      refusal
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

    case Verb.run(:runner_connect, runner_ctx, [namespace], fn _call ->
           Quotas.check_allocation(namespace, :max_runners)
         end) do
      :ok ->
        :ok

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
    StreamPool.call_channels(old_session, :fence_write_path)
  end

  # Re-tag every operative still bound to `old_session` onto `new_session` and
  # seed the new session's pre-spawned StreamChannels so in-flight frames land
  # without waiting for a convergence tick. `do_evict_runner/2` then tears down
  # the old (now operative-less) session.
  def swap_bindings(namespace, old_session, new_session) do
    HubManager.runner_operatives(namespace, old_session)
    |> Enum.each(&swap_one_binding(namespace, new_session, &1))
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
    HubManager.runner_operatives(namespace, runner_id)
    |> Enum.each(fn {operative_id, _node_pids} ->
      HubManager.hook_remove_pid(runner_id, %{child_id: operative_id})
    end)

    _ = HubManager.unregister_handler(namespace, runner_id)
    :ok
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

    _ = HubManager.unregister_handler(namespace, runner_id)

    other_runners? =
      state
      |> Roster.get_runner_ids(namespace, :active)
      |> Enum.any?(fn id -> id != runner_id end)

    other_nodes? = HubManager.get_hub_nodes(namespace) != []

    # The keepalive: the last runner of a namespace nobody else serves is held
    # `:inactive` for its window, so a reconnect swaps its bindings back rather
    # than re-placing them. Every other case simply drops it.
    namespace_runners =
      if other_runners? or other_nodes?,
        do: drop_runner(state, namespace, runner_id),
        else: runner_preserve(state, namespace, runner_id)

    # The loop chooses what becomes of the operatives this runner held: a
    # rebind onto a surviving runner, or a re-place onto a peer's fleet, each
    # under its own key. Nothing is rerouted here.
    Facts.declare({:runner_disconnected, {namespace, runner_id}, %{}})

    EventManager.dispatch(
      :runner_disconnected_event,
      {namespace, runner_id},
      %{members: :local}
    )

    put_in(state, [:runner_namespaces, namespace], namespace_runners)
  end

  # Drop the dead runner from the active set unconditionally: a runner that
  # lingers reads as placeable to `active_runners/1`, and operatives get
  # assigned to a dead node.
  def drop_runner(state, namespace, runner_id) do
    Map.delete(state.runner_namespaces[namespace] || %{}, runner_id)
  end

  def runner_preserve(state, namespace, runner_id) do
    runner = {:inactive, DateTime.utc_now() |> DateTime.to_unix(:millisecond)}

    put_in(state.runner_namespaces[namespace], [runner_id], runner)
  end

  @doc """
  Admit a connecting runner, or `throw` `{:runner_connect_failed, reason}`.

  Four pieces of state, in the order they must be taken: the namespace's hub,
  that hub's operative-insertion hook, this runner's supervision tree, and the
  stream itself. Each step refuses by throwing rather than by crashing —
  `FleetManager` serves every runner on the node, and its acceptor goes with
  it.
  """
  def runner_connect(stream_ref, namespace, transport) do
    runner_id = Identificator.unique_id(namespace)

    ensure_hub(namespace)
    register_insertion_hook(namespace, runner_id)
    control_channel_pid = start_runner_tree(namespace, runner_id, stream_ref, transport)
    hand_over_stream(stream_ref, control_channel_pid, Identificator.control_unit(runner_id))

    {:ok, namespace, runner_id}
  end

  # Gate on-demand hub creation on cluster formation: a runner connecting
  # during the boot window must not create a singleton hub before the node has
  # joined the cluster. Bounded by the degrade-to-solo timeout, so a genuinely
  # single-node deployment still proceeds.
  defp ensure_hub(namespace) do
    if !HubManager.hub_alive?(namespace) do
      _ = Orchestrator.await_cluster_ready()
      HubManager.start_hub(namespace)
    end
  end

  # A hub that is gone, or terminating under a drain, cannot take this runner's
  # operative-insertion hook. Refusing here is what keeps one namespace's
  # teardown from travelling out of this call as an exit.
  defp register_insertion_hook(namespace, runner_id) do
    case HubManager.register_handler(namespace, runner_id) do
      :ok ->
        :ok

      {:error, reason} ->
        refuse(
          runner_id,
          namespace,
          "could not register its operative-insertion hook (#{inspect(reason)})",
          {:hub_unavailable, namespace}
        )
    end
  end

  # A runner whose tree did not come up is refused rather than allowed to crash
  # this manager.
  defp start_runner_tree(namespace, runner_id, stream_ref, transport) do
    Supervisor.start_child(
      FleetSupervisor,
      Cspec.build(
        runner_id,
        {RunnerSupervisor, :start_link, [{namespace, runner_id, stream_ref, transport}]},
        :worker_permanent
      )
    )

    case RunnerSupervisor.find_control_channel(runner_id) do
      {:ok, pid} ->
        pid

      {:error, reason} ->
        refuse(runner_id, namespace, "has no control channel (#{inspect(reason)})", reason)
    end
  end

  # Ownership of the stream passes to the ControlChannel process, and only then
  # is the runner told who it is.
  defp hand_over_stream(stream_ref, control_channel_pid, control_unit) do
    Flezha.Transport.Stream.controlling_process(stream_ref, control_channel_pid)
    Flezha.Transport.Stream.set_active(stream_ref)

    # A control unit that cannot take the handover is a failed connect, not a
    # silent one: the refusal is thrown the way every other connect fault is.
    bound = Durations.get(:control_unit_reply_timeout_ms)
    on_error = &throw({:runner_connect_failed, {:control_unit_unavailable, &1}})

    SafeCall.bounded(control_unit, :command_assign_runner_id, bound, on_error: on_error)
    SafeCall.bounded(control_unit, :command_sockets_connect, bound, on_error: on_error)
  end

  defp refuse(runner_id, namespace, what, reason) do
    Logger.error("runner #{runner_id} on #{namespace} #{what}; refusing the connect")
    throw({:runner_connect_failed, reason})
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
