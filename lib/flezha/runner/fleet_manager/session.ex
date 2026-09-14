defmodule Flezha.Runner.FleetManager.Session do
  @moduledoc """
  One runner session, from the handshake to the disconnect that ends it.

  A session token is minted per connection; a `stable_id` that already has a
  live session is resolved newest-wins — the old token is fenced, each of its
  operatives' Overwatches is told the new one, and the old session is evicted.
  A `stable_id` held by a session admitted under another credential is refused
  instead (`check_identity_pin/2`).

  A session is recorded only once its runner has answered both handshake
  frames. Until then it is pending, and every way it can end (a refusal, the
  reply bound, a disconnect, a revoked credential, its namespace stopping)
  tears its tree down with no roster entry to undo.

  Invariant: a disconnect drops the runner from the active set unconditionally,
  whatever the roster holds, and is declared to the convergence loop, which
  decides what becomes of the operatives it held. The one thing this module
  keeps is the keepalive: the last runner of a namespace nobody else serves is
  held `:inactive` for its window rather than dropped.
  """

  alias Flezha.AuthZ.QuotaCounters
  alias Flezha.AuthZ.Quotas
  alias Flezha.AuthZ.RoleStore
  alias Flezha.AuthZ.Verb
  alias Flezha.Cluster.Directory
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
  alias Flezha.Service.EventManager
  alias Flezha.Service.HubManager
  alias Flezha.Shutdown

  require Logger

  use Flezha.Process.Timer, keys: [:handshake]

  @behaviour Flezha.Shutdown.Step

  # The frames that make a session, in the order the runner needs them:
  # `ConnectSocket` is named by the id `AssignRunnerId` gives it.
  @handshake [:command_assign_runner_id, :command_sockets_connect]

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

  # The runner principal is minted from the namespace the connection PROVED
  # (`Flezha.AuthZ.RunnerProof`), which is also what argument one carries, so
  # both sides of the row's namespace rule read the proven value and neither
  # reads the frame. Cross-namespace action is refused before this, at the
  # proof; what this row judges is that the identity holds `runner.connect` at
  # all. Also the per-namespace `max_runners` quota point — both refusals are
  # observable through the authz denial event / the quota telemetry below.
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

  @doc """
  The identity a connection proved, as a caller context.

  The namespace is the proven one and never the frame's. The roles are read from
  `RoleStore` on every call rather than stamped once, so `flz role bind` and
  `flz role unbind` take effect on the next call instead of the next reconnect;
  a principal with no binding, one bound to another namespace, or one holding
  only an operator's role is `runner`, which grants `runner.connect` and nothing
  else. Granting a namespace's application more than that is one command per
  role:

      flz role bind runner@team-orders spawner team-orders
      flz role bind runner@team-orders stopper team-orders
  """
  @spec runner_caller_context(binary()) :: Flezha.AuthZ.caller_context()
  def runner_caller_context(namespace) do
    principal = "runner@" <> namespace

    %{
      source: :sdk,
      principal: principal,
      namespace: namespace,
      roles: RoleStore.runner_roles(principal, namespace)
    }
  end

  # Reconnect/takeover binding swap. The SDK presents the same persisted stable
  # id on every connect, and an operative is owned by that identity while its
  # routing keys on the per-connection session token, so a reconnect tells each
  # of the prior session's operatives its new token and then closes the old
  # session (newest-wins). A first connect just records the mapping, with the
  # credential that admitted it; a degraded runner with no stable id connects
  # fresh.
  defp resolve_stable_identity(state, _session_token, %{stable_id: nil}), do: state

  defp resolve_stable_identity(state, session_token, connect) do
    %{namespace: namespace, stable_id: stable_id, credential: credential} = connect

    state =
      case Map.get(state.stable_sessions, stable_id) do
        {old_session, _credential} when old_session != session_token ->
          # Unbind-old BEFORE bind-new: once the fence returns, nothing the
          # superseded session forwards can land in any Entity WAL, so no
          # window exists in which both sessions hold a writable binding.
          fence_superseded_session(old_session)
          swap_bindings(namespace, old_session, session_token)
          do_evict_runner(state, old_session)

        _ ->
          state
      end

    put_in(state, [:stable_sessions, stable_id], {session_token, credential})
  end

  @doc """
  Refuse a connect whose stable identity is held under another credential.

  An operative is owned by its runner's stable identity, so presenting an
  identity is the one door to another runner's operatives. While the identity
  has a session in the roster (live, or kept inactive for its keepalive) or a
  handshake in flight, admitted under one credential, a connect presenting it
  under a different credential is refused rather than taken over. The pin
  lapses when the identity's last session leaves the roster. A connect that
  carries no per-runner credential (certificate-proven, or posture off) is
  neither pinned nor refused.
  """
  @spec check_identity_pin(map(), map()) :: :ok | {:error, {:identity_taken, binary()}}
  def check_identity_pin(state, %{stable_id: stable_id, credential: credential})
      when is_binary(stable_id) and is_binary(credential) do
    if held_elsewhere?(state, stable_id, credential) do
      Logger.warning(
        "[FleetManager] runner connect refused: stable identity #{stable_id} is held " <>
          "under another credential"
      )

      {:error, {:identity_taken, stable_id}}
    else
      :ok
    end
  end

  def check_identity_pin(_state, _connect), do: :ok

  defp held_elsewhere?(state, stable_id, credential) do
    pinned_live? =
      case Map.get(state.stable_sessions, stable_id) do
        {session, held} when is_binary(held) and held != credential ->
          Roster.find_runner_namespace(state, session) != nil

        _unpinned_or_same ->
          false
      end

    pinned_live? or
      Enum.any?(state.pending_handshakes, fn {_session, %{connect: connect}} ->
        connect.stable_id == stable_id and connect.credential not in [nil, credential]
      end)
  end

  # The write-path fence, in refusal order: mark the session superseded (the
  # WalWriter-side gate), then synchronously flip each of its StreamChannels
  # off the Entity write path. Bounded calls — a wedged channel must not
  # block the connect; its late-flushed frames are refused by the mark instead.
  def fence_superseded_session(old_session) do
    Marks.mark_session_superseded(old_session)
    StreamPool.call_channels(old_session, :fence_write_path)
  end

  # Tell every operative the registry tags with `old_session` that its runner is
  # `new_session` now. Each Overwatch re-binds and publishes its own tag and
  # route, so the Overwatch, the tag and the route have one writer; the
  # convergence loop's stale-tag and misbound heals cover an operative with no
  # live Overwatch to tell. `do_evict_runner/2` then tears down the old session.
  def swap_bindings(namespace, old_session, new_session) do
    namespace
    |> HubManager.runner_operatives(old_session)
    |> Enum.each(fn {_operative_id, node_pids} ->
      case HubManager.live_pid(node_pids) do
        nil -> :ok
        pid -> GenServer.cast(pid, {:session_replaced, old_session, new_session})
      end
    end)
  end

  def do_evict_runner(state, runner_id) do
    case Roster.find_runner_namespace(state, runner_id) do
      nil ->
        forget_credential(state, runner_id)

      namespace ->
        stop_runner_sup(runner_id)

        :telemetry.execute(
          [:flezha, :runner, :evicted],
          %{count: 1},
          %{namespace: namespace, runner_id: runner_id}
        )

        state
        |> forget_credential(runner_id)
        |> Factories.remove_ce_factories(namespace, runner_id)
        |> Factories.prune_empty_factories(namespace)
        |> update_in([:runner_namespaces, namespace], &Map.delete(&1 || %{}, runner_id))
    end
  end

  # The credential ledger tracks live sessions only, so it is written where a
  # session is minted and cleared wherever one ends — an eviction here, a
  # disconnect below.
  defp remember_credential(state, _session_token, nil), do: state

  defp remember_credential(state, session_token, credential),
    do: put_in(state, [:session_credentials, session_token], credential)

  defp forget_credential(state, session_token),
    do: update_in(state, [:session_credentials], &Map.delete(&1 || %{}, session_token))

  # The roster answers where a runner may be placed, not whether a session that
  # ended is dismantled: a runner it no longer keys can still have its whole
  # tree running. A session is standing while either one holds it, so a second
  # disconnect finds neither and changes nothing, and a session still in its
  # handshake was never recorded at all.
  def handle_runner_disconnect(state, namespace, runner_id) do
    cond do
      Map.has_key?(state.pending_handshakes, runner_id) ->
        end_handshake(state, runner_id, :disconnected)

      Roster.runner_active?(state, runner_id) or tree_standing?(runner_id) ->
        do_handle_runner_disconnect(state, namespace, runner_id)

      true ->
        state
    end
  end

  defp tree_standing?(runner_id), do: GenServer.whereis(Identificator.unit_sup(runner_id)) != nil

  defp set_runner_counter({namespace, runners}) do
    active = Enum.count(runners, fn {_id, {status, _ts}} -> status == :active end)
    QuotaCounters.set(namespace, :runners, active)
  end

  # Every way a session ends stops its tree here, which is where its identity
  # is forgotten.
  def stop_runner_sup(runner_id) do
    Supervisor.terminate_child(FleetSupervisor, runner_id)
    Supervisor.delete_child(FleetSupervisor, runner_id)
    Marks.forget_identity(runner_id)
  end

  defp do_handle_runner_disconnect(state, namespace, runner_id) do
    Marks.clear_runner_retiring(runner_id)
    stop_runner_sup(runner_id)

    state =
      state
      |> forget_credential(runner_id)
      |> Factories.remove_ce_factories(namespace, runner_id)
      |> Factories.prune_empty_factories(namespace)

    other_runners? =
      state
      |> Roster.get_runner_ids(namespace, :active)
      |> Enum.any?(fn id -> id != runner_id end)

    other_nodes? = HubManager.get_hub_nodes(namespace) != []

    # The keepalive: the last runner of a namespace nobody else serves is held
    # `:inactive` for its window, so a reconnect swaps its bindings back rather
    # than re-placing them. Every other case simply drops it.
    state =
      cond do
        not Map.has_key?(state.runner_namespaces, namespace) ->
          state

        other_runners? or other_nodes? ->
          put_roster(state, namespace, drop_runner(state, namespace, runner_id))

        true ->
          put_roster(state, namespace, runner_preserve(state, namespace, runner_id))
      end

    # The loop chooses what becomes of the operatives this runner held: a
    # rebind onto a surviving runner, or a re-place onto a peer's fleet, each
    # under its own key. Nothing is rerouted here.
    Facts.declare({:runner_disconnected, {namespace, runner_id}, %{}})

    EventManager.dispatch(
      :runner_disconnected_event,
      {namespace, runner_id},
      %{members: :local}
    )

    state
  end

  defp put_roster(state, namespace, runners),
    do: put_in(state, [:runner_namespaces, namespace], runners)

  # Drop the dead runner from the active set unconditionally: a runner that
  # lingers reads as placeable to `active_runners/1`, and operatives get
  # assigned to a dead node.
  def drop_runner(state, namespace, runner_id) do
    Map.delete(state.runner_namespaces[namespace] || %{}, runner_id)
  end

  def runner_preserve(state, namespace, runner_id) do
    runner = {:inactive, System.monotonic_time(:millisecond)}

    put_in(state.runner_namespaces[namespace], [runner_id], runner)
  end

  @doc """
  Start a connecting runner's session, or `throw` `{:runner_connect_failed, reason}`.

  Four pieces of state, in the order they must be taken: the namespace's hub,
  the session's stable identity (its channels read it as they start), this
  runner's supervision tree, and the stream itself. Each step refuses by
  throwing rather than by crashing — `FleetManager` serves every runner on the
  node, and its acceptor goes with it. The session is recorded only once the
  runner has answered its handshake (`await_handshake/4`).
  """
  def runner_connect(stream_ref, %{namespace: namespace, transport: transport} = connect) do
    runner_id = Identificator.unique_id(namespace)

    ensure_hub(namespace, runner_id)
    Marks.bind_identity(runner_id, connect.stable_id)
    control_channel_pid = start_runner_tree(namespace, runner_id, stream_ref, transport)
    Flezha.Transport.Stream.controlling_process(stream_ref, control_channel_pid)
    Flezha.Transport.Stream.set_active(stream_ref)

    {:ok, namespace, runner_id}
  end

  @doc """
  Ask the runner for its two handshake answers, and wait for neither.

  `FleetManager` serves every namespace on the node, and an in-node runner is
  blocked in its own `init/1` until the connect returns, so the answers come
  back as messages (`handshake_answered/4`) and the session is recorded only
  once both are in. One not in by `control_unit_reply_timeout_ms` ends the
  session as a refusal does.
  """
  def await_handshake(state, session_token, stream_ref, connect) do
    control_unit = Identificator.control_unit(session_token)

    requests =
      Enum.reduce(@handshake, state.handshake_requests, fn command, acc ->
        :gen_server.send_request(control_unit, command, {session_token, command}, acc)
      end)

    bound = Durations.get(:control_unit_reply_timeout_ms)
    pending = %{connect: connect, stream_ref: stream_ref, owed: @handshake, waiters: []}

    %{
      state
      | handshake_requests: requests,
        pending_handshakes: Map.put(state.pending_handshakes, session_token, pending)
    }
    |> arm_timer({:handshake, session_token}, bound, {:handshake_timeout, session_token})
  end

  @doc """
  One handshake answer, as `:gen_server.check_response/3` resolved it. The
  session completes on the last one owed and ends on the first refusal.
  """
  def handshake_answered(state, session_token, command, {:reply, {:ok, _raw}}) do
    case Map.get(state.pending_handshakes, session_token) do
      %{owed: [^command]} = pending ->
        {_, state} = pop_in(state, [:pending_handshakes, session_token])

        state
        |> cancel_timer({:handshake, session_token})
        |> finish_runner_connect(session_token, pending.connect)
        |> tap(fn _state -> answer_waiters(pending, :ok) end)

      %{owed: owed} = pending ->
        put_in(state, [:pending_handshakes, session_token], %{pending | owed: owed -- [command]})

      nil ->
        state
    end
  end

  def handshake_answered(state, session_token, command, {:reply, {:error, reason}}),
    do: end_handshake(state, session_token, {command, reason})

  def handshake_answered(state, session_token, command, {:error, {reason, _control_unit}}),
    do: end_handshake(state, session_token, {command, {:control_unit_unavailable, reason}})

  @doc """
  Hold `from` until `session_token` is admitted, when it is still in its
  handshake: the handshake's end answers it. `:not_pending` for any other
  session, which has nothing to wait for.
  """
  def hold_until_admitted(state, session_token, from) do
    case Map.fetch(state.pending_handshakes, session_token) do
      {:ok, pending} ->
        pending = Map.update(pending, :waiters, [from], &[from | &1])
        {:held, put_in(state, [:pending_handshakes, session_token], pending)}

      :error ->
        :not_pending
    end
  end

  defp answer_waiters(pending, answer),
    do: pending |> Map.get(:waiters, []) |> Enum.each(&GenServer.reply(&1, answer))

  @doc """
  End a session whose handshake did not complete: refused, timed out, revoked,
  disconnected, or its namespace stopped. It was never recorded, so there is no
  roster entry and no binding to undo; closing the stream is what tells the
  runner, whose ladder counts it as a failed attempt.
  """
  def end_handshake(state, session_token, reason) do
    case pop_in(state, [:pending_handshakes, session_token]) do
      {nil, state} ->
        state

      {%{connect: %{namespace: namespace}} = pending, state} ->
        Logger.log(
          handshake_end_level(reason),
          "runner #{session_token} on #{namespace} did not complete its handshake " <>
            "(#{inspect(reason)}); ending the session"
        )

        stop_runner_sup(session_token)
        Flezha.Transport.Stream.close(pending.stream_ref)
        answer_waiters(pending, {:error, :not_admitted})
        cancel_timer(state, {:handshake, session_token})
    end
  end

  @doc """
  End every handshake whose connect record `match?` selects, for a teardown that
  reaches sessions the roster does not hold yet. Answers the state and how many
  ended.
  """
  def end_handshakes(state, match?, reason) do
    sessions =
      for {session, %{connect: connect}} <- state.pending_handshakes,
          match?.(connect),
          do: session

    {Enum.reduce(sessions, state, &end_handshake(&2, &1, reason)), length(sessions)}
  end

  # A refusal is the runner's or the control unit's fault and an operator's to
  # read; a silent runner is the liveness watchdog's kind of warning; the rest
  # are a teardown something else already reported.
  defp handshake_end_level({_command, _reason}), do: :error
  defp handshake_end_level(:timeout), do: :warning
  defp handshake_end_level(_teardown), do: :info

  # On-demand hub creation takes the same cluster gate every hub-creating path
  # takes: a runner connecting during the boot window must not create a
  # singleton hub before the node has joined the cluster. A hub that did not
  # start is refused with the start's own error, since the next step could
  # only report it as missing; one a concurrent connect started is this
  # runner's hub too.
  defp ensure_hub(namespace, runner_id) do
    if !HubManager.hub_alive?(namespace) do
      _ = HubManager.await_cluster_gate()

      case HubManager.start_hub(namespace) do
        {:error, {:already_started, _pid}} ->
          :ok

        {:error, reason} ->
          refuse(
            runner_id,
            namespace,
            "could not start its hub (#{inspect(reason)})",
            {:hub_unavailable, namespace, reason}
          )

        _started ->
          :ok
      end
    end
  end

  # A runner whose tree did not come up is refused rather than allowed to crash
  # this manager, and with the start's own error: a tree that never started
  # leaves no supervisor to ask, so asking it could only answer `:noproc`.
  defp start_runner_tree(namespace, runner_id, stream_ref, transport) do
    tree = {RunnerSupervisor, :start_link, [{namespace, runner_id, stream_ref, transport}]}
    spec = Cspec.build(runner_id, tree, :worker_permanent)

    control_channel =
      case Supervisor.start_child(FleetSupervisor, spec) do
        {:ok, _supervisor} -> RunnerSupervisor.find_control_channel(runner_id)
        {:error, reason} -> {:error, {:runner_supervisor_down, reason}}
      end

    case control_channel do
      {:ok, pid} ->
        pid

      {:error, reason} ->
        refuse(runner_id, namespace, "has no control channel (#{inspect(reason)})", reason)
    end
  end

  # The identity is recorded before the tree, so a refusal after it forgets it.
  # Forgetting is by name, so a refusal that never recorded one is a no-op.
  defp refuse(runner_id, namespace, what, reason) do
    Logger.error("runner #{runner_id} on #{namespace} #{what}; refusing the connect")
    Marks.forget_identity(runner_id)
    throw({:runner_connect_failed, reason})
  end

  @doc """
  Everything one connect settled, in the shape `runner_connect` is served from:
  the namespace and credential the proof answered (`Flezha.AuthZ.RunnerProof`),
  and the factory set and stable identity the frame declared. The frame's own
  shape is read here and nowhere else.
  """
  @spec connect_record(%{namespace: binary(), credential: binary() | nil}, map(), atom()) ::
          map()
  def connect_record(%{namespace: namespace, credential: credential}, init_payload, transport) do
    %{
      namespace: namespace,
      transport: transport,
      credential: credential,
      factories: parse_factories(init_payload),
      stable_id: parse_stable_id(init_payload)
    }
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

  def catch_runner_connect(stream_ref, connect) do
    runner_connect(stream_ref, connect)
  catch
    {:runner_connect_failed, reason} -> {:error, reason}
  end

  defp finish_runner_connect(state, session_token, connect) do
    %{namespace: namespace, factories: factories, credential: credential} = connect

    timestamp = System.monotonic_time(:millisecond)

    state = update_in(state, [:runner_namespaces], &Map.put_new(&1, namespace, %{}))
    state = put_in(state, [:runner_namespaces, namespace, session_token], {:active, timestamp})

    state = Factories.store_factories(state, namespace, session_token, factories)

    # The credential that admitted this session, remembered so revoking it can
    # close the session: a runner credential never expires, so nothing else
    # would. A certificate-proven connect presents none and records nothing.
    state = remember_credential(state, session_token, credential)

    # Resolve the runner's durable STABLE IDENTITY: on a reconnect (or a
    # newest-wins takeover) re-point the prior session's operative bindings onto
    # this fresh session token and close the old session. A first connect just
    # records the mapping; a degraded runner (no stable id) connects fresh.
    state = resolve_stable_identity(state, session_token, connect)

    # The convergence Loop subscribes to this and runs an immediate tick as a
    # safety net; the binding swap above is the synchronous primary path.
    EventManager.dispatch(
      :runner_connected_event,
      {namespace, session_token},
      %{members: :local}
    )

    sync_directory(state)
  end
end
