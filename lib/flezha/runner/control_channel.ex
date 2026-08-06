defmodule Flezha.Runner.ControlChannel do
  alias Flezha.Service.HubManager
  alias Flezha.Service.OperativeManager
  alias Flezha.Service.Identificator
  alias Flezha.Service.Payloader
  alias Flezha.Runner.FleetManager
  alias Flezha.Transport.Native

  require Logger

  use GenServer

  # Control-plane liveness: the SDK keepalive ping (and every other inbound
  # frame) re-arms `last_seen`; a jittered watchdog treats sustained silence
  # past the window as a connection close — the same signal `{:flz_closed}`
  # carries. Closes the UDS wedged-but-connected blind spot and makes UDS
  # symmetric with QUIC's idle timeout. Mirrors the data-plane `StreamChannel`
  # last_seen/threshold watchdog.
  @control_liveness_window_ms 45_000
  @control_liveness_interval_ms 15_000

  # Re-attempt cadence for a journaled outbox whose operative is not bound yet
  # (mirrors the unbounded outbox's `:outbox_retry` in `Flezha.State.WalWriter`).
  @proc_outbox_retry_ms 1_000

  def start_link({namespace, runner_id, stream_ref}) do
    GenServer.start_link(__MODULE__, {namespace, runner_id, stream_ref, nil},
      name: Identificator.control_channel(runner_id)
    )
  end

  # Test-only constructor: accepts an explicit `write_fun` that the
  # `RunnerShutdownAck` emit path uses in place of `Native.send_data/2`.
  # Production callers go through the 3-tuple form; the 4-tuple is here
  # because `Flezha.Transport.Native` is a NIF and cannot be Mimic-copied
  # (its `@on_load` triggers under the renamed module name and fails),
  # so unit tests inject a capturing closure instead.
  def start_link({namespace, runner_id, stream_ref, write_fun})
      when is_function(write_fun, 2) do
    GenServer.start_link(__MODULE__, {namespace, runner_id, stream_ref, write_fun},
      name: Identificator.control_channel(runner_id)
    )
  end

  def send_control(runner_id, command) do
    control_channel = Identificator.control_channel(runner_id)

    case GenServer.whereis(control_channel) do
      nil ->
        {:error, :no_control_channel}

      _ ->
        ref = gen_ref()

        req =
          struct(
            RunnerCommands.CommandRequest,
            reference_id: ref,
            command: command
          )

        {:ok, iodata, _size} = Protox.encode(req)

        payload =
          Payloader.build_from_bin(
            "control_command",
            IO.iodata_to_binary(iodata)
          )

        # Propagate the transport result instead of always returning {:ok, ref}.
        # A reset/dead stream replies {:error, :no_connection_available}; the
        # caller (ControlUnit) must then surface a clean error to the operative
        # rather than registering a reply ref that will never resolve.
        case GenServer.call(control_channel, {:send_control, payload}) do
          :ok -> {:ok, ref}
          {:error, reason} -> {:error, reason}
        end
    end
  end

  def send_control_async(runner_id, command) do
    control_channel = Identificator.control_channel(runner_id)

    case GenServer.whereis(control_channel) do
      nil ->
        :ok

      _ ->
        ref = gen_ref()

        req =
          struct(
            RunnerCommands.CommandRequest,
            reference_id: ref,
            command: command
          )

        {:ok, iodata, _size} = Protox.encode(req)

        payload =
          Payloader.build_from_bin(
            "control_command",
            IO.iodata_to_binary(iodata)
          )

        GenServer.cast(control_channel, {:send_control_async, payload})
        :ok
    end
  end

  @impl true
  def init({namespace, runner_id, stream_ref, write_fun}) do
    schedule_liveness_check()

    {:ok,
     %{
       namespace: namespace,
       runner_id: runner_id,
       stream_ref: stream_ref,
       write_fun: write_fun || (&Native.send_data/2),
       control_unit: Identificator.control_unit(runner_id),
       last_seen: System.monotonic_time(:millisecond)
     }}
  end

  @doc """
  Synchronously clear this channel's `stream_ref` to `nil` so the existing
  rebind machinery (see `runner-controller-connection-healing`) can claim
  the next data stream the SDK opens. Used by
  `Flezha.Runner.FleetManager.reset_control_channel/1` after an end-to-end
  ping fails — the controller-side `ControlChannel` GenServer is alive but
  its underlying transport handle is stale, so subsequent `send_control` calls
  enqueue into a closed transport with no failure signal.
  """
  @spec reset_stream_ref(binary()) :: :ok | {:error, :no_control_channel}
  def reset_stream_ref(runner_id) when is_binary(runner_id) do
    control_channel = Identificator.control_channel(runner_id)

    case GenServer.whereis(control_channel) do
      nil -> {:error, :no_control_channel}
      pid -> GenServer.call(pid, :reset_stream_ref)
    end
  end

  @impl true
  def handle_call(:reset_stream_ref, _from, state) do
    {:reply, :ok, %{state | stream_ref: nil}}
  end

  @impl true
  def handle_call({:send_control, payload}, _from, state) do
    res =
      case Map.get(state, :stream_ref, nil) do
        nil ->
          {:error, :no_connection_available}

        stream_ref ->
          Native.send_data(stream_ref, payload)
      end

    {:reply, res, state}
  end

  @impl true
  def handle_call({:handle_frame, _frame}, _from, state) do
    # No longer needed — framing is done in Rust NIF
    {:reply, :ok, state}
  end

  @impl true
  def handle_cast({:send_control_async, payload}, state) do
    case Map.get(state, :stream_ref, nil) do
      nil -> :ok
      stream_ref -> Native.send_data(stream_ref, payload)
    end

    {:noreply, state}
  end

  # Steady-state control-channel keepalive from the SDK (see flz_transport
  # liveness). Echoed back verbatim on the same dedicated `control_ping`
  # payload so the runner's idle watchdog re-arms; it never enters the
  # command path on either side. A wedged controller stops echoing, and the
  # runner's watchdog drives reconnect.
  @impl true
  def handle_info({:flz_data, _stream_id, "control_ping", message, _metadata}, state) do
    case Map.get(state, :stream_ref) do
      nil ->
        :ok

      stream_ref ->
        state.write_fun.(stream_ref, Payloader.build_from_bin("control_ping", message))
    end

    {:noreply, bump_last_seen(state)}
  end

  # Unified NIF transport message — complete parsed frame
  #
  # Most inbound traffic on the control channel is `CommandResponse` from
  # the SDK (replies to controller-initiated `CommandRequest`s) and is forwarded
  # as-is to the `ControlUnit`. The exception is the SDK-initiated
  # `RunnerShutdownNotification` introduced by ship-fresh-state-on-sdk-
  # shutdown: it reuses the `CommandRequest` envelope and the same
  # `"control_command"` payload_id, so the only way to tell it apart on
  # the wire is to peek at the oneof tag. We attempt the
  # `CommandRequest` decode first; if the message has a recognised
  # SDK-initiated command, dispatch it here and skip the `ControlUnit`
  # handover.
  @impl true
  def handle_info({:flz_data, _stream_id, _payload_id, message, metadata}, state) do
    state = bump_last_seen(state)

    case decode_sdk_initiated(message) do
      {:runner_shutdown_notification, %{} = notif, reference_id} ->
        handle_runner_shutdown_notification(state, notif, reference_id)
        {:noreply, state}

      {:bounded_completed, %{} = completed, _reference_id} ->
        handle_bounded_completed(state, completed)
        {:noreply, state}

      {:proc_commit, %{} = commit, reference_id} ->
        handle_proc_commit(state, commit, reference_id)
        {:noreply, state}

      {:iter_commit, %{} = commit, reference_id} ->
        handle_iter_commit(state, commit, reference_id)
        {:noreply, state}

      {:iter_park, %{} = park, _reference_id} ->
        handle_iter_park(state, park)
        {:noreply, state}

      {:update_factories, %{} = update, _reference_id} ->
        handle_update_factories(state, update)
        {:noreply, state}

      {:spawn_bounded_request, %{} = request, reference_id} ->
        handle_spawn_bounded(state, request, reference_id)
        {:noreply, state}

      :not_sdk_initiated ->
        GenServer.cast(state.control_unit, {:handle_payload, message, metadata})
        {:noreply, state}
    end
  end

  # NIF transport: connection closed
  @impl true
  def handle_info({:flz_closed, _stream_id, _reason}, state) do
    handle_connection_closed(state)
  end

  # The spawn resolution ran off-process; write its reply on this channel, which
  # is the one process allowed to write control frames.
  @impl true
  def handle_info({:spawn_bounded_result, reference_id, outcome, error}, state) do
    emit_control_request(
      state,
      reference_id,
      {:spawn_bounded_response,
       %RunnerCommands.SpawnBoundedResponse{outcome: outcome, error: error}},
      "spawn-bounded response #{outcome}"
    )

    {:noreply, state}
  end

  # Retry a journaled outbox whose operative was not bound when the flush was
  # first attempted. The journal is the source of truth, so the retry simply
  # re-runs the same flush; it stops when nothing is owed.
  @impl true
  def handle_info({:proc_outbox_retry, operative_id}, state) do
    flush_proc_outbox(operative_id)
    {:noreply, state}
  end

  # Control-plane liveness watchdog. Sustained silence past the window — no
  # inbound frame, not even the SDK keepalive ping — is a process/control-channel
  # wedge UDS never surfaces as EOF. Route through the SAME
  # `handle_connection_closed/1` a real socket close uses (no parallel eviction
  # path). The window is generous (3x the keepalive) and jittered, so a brief
  # GC/contention pause or a single missed ping never trips it.
  @impl true
  def handle_info(:liveness_tick, state) do
    silent_ms = System.monotonic_time(:millisecond) - state.last_seen

    if silent_ms > liveness_window_ms() do
      Logger.warning(
        "[control_channel] no inbound control frame for #{silent_ms}ms for " <>
          "#{state.runner_id} on #{state.namespace}; treating as connection close"
      )

      :telemetry.execute(
        [:flezha, :runner, :liveness_timeout],
        %{count: 1, silent_ms: silent_ms},
        %{runner_id: state.runner_id, namespace: state.namespace}
      )

      handle_connection_closed(state)
    else
      schedule_liveness_check()
      {:noreply, state}
    end
  end

  @impl true
  def handle_info(msg, state) do
    Logger.warning("Unexpected message at control channel: #{inspect(msg)}")

    {:noreply, state}
  end

  defp handle_connection_closed(state) do
    Logger.warning(
      "Connection closed for control socket for #{state.runner_id} on #{state.namespace}"
    )

    GenServer.call(FleetManager, {
      :runner_disconnected,
      state.namespace,
      state.runner_id
    })

    {:noreply, state}
  end

  defp bump_last_seen(state) do
    %{state | last_seen: System.monotonic_time(:millisecond)}
  end

  defp schedule_liveness_check do
    interval = liveness_interval_ms()
    if interval > 0, do: Process.send_after(self(), :liveness_tick, jittered(interval))
  end

  defp liveness_window_ms,
    do: Application.get_env(:flezha, :control_liveness_window_ms, @control_liveness_window_ms)

  defp liveness_interval_ms,
    do: Application.get_env(:flezha, :control_liveness_interval_ms, @control_liveness_interval_ms)

  # Full jitter into [interval/2, interval] so a fleet's control channels do
  # not all wake in lockstep (mirrors the SDK's `liveness::jittered`).
  defp jittered(ms) when ms < 2, do: ms

  defp jittered(ms) do
    half = div(ms, 2)
    half + :rand.uniform(half + 1) - 1
  end

  defp gen_ref() do
    make_ref()
    |> :erlang.ref_to_list()
    |> List.to_string()
  end

  # Inspect an inbound control-channel payload for an SDK-initiated
  # `CommandRequest`. The controller's normal inbound flow is `CommandResponse`,
  # so a successful `CommandRequest` decode with a recognised SDK-initiated
  # oneof tag (e.g. `:runner_shutdown_notification`) is the discriminator.
  # Anything else falls through to the existing `ControlUnit` path.
  #
  # Note: a `CommandResponse` will not decode cleanly as a `CommandRequest`
  # because their field-2 wire types disagree (string vs nested message);
  # Protox returns `{:error, _}` in that case, which maps to
  # `:not_sdk_initiated`.
  @sdk_initiated_commands [
    :runner_shutdown_notification,
    :bounded_completed,
    :proc_commit,
    :iter_commit,
    :iter_park,
    :update_factories,
    :spawn_bounded_request
  ]

  defp decode_sdk_initiated(message) do
    case RunnerCommands.CommandRequest.decode(message) do
      {:ok, %RunnerCommands.CommandRequest{reference_id: ref, command: {tag, payload}}}
      when tag in @sdk_initiated_commands ->
        {tag, payload, ref}

      _ ->
        :not_sdk_initiated
    end
  end

  # Apply the SDK's final state to the per-operative `Overwatch` and emit
  # an `RunnerShutdownAck` so the SDK's bounded ack-wait can release.
  # The cast-not-call ordering is deliberate: the snapshot is enqueued on
  # Overwatch's mailbox before the ack flows back, so even if the controller
  # crashes between the cast and the write, the snapshot is durable in
  # the GenServer's state. See ship-fresh-state-on-sdk-shutdown design §D2.
  #
  # The ack is emitted unconditionally — including the
  # `:overwatch_missing` branch (Overwatch already migrated to a peer
  # controller) — so the SDK never blocks its container's shutdown grace
  # period waiting for an ack that will never arrive.
  defp handle_runner_shutdown_notification(state, notif, reference_id) do
    %RunnerCommands.RunnerShutdownNotification{
      operative_id: operative_id,
      serialized_state: serialized_state
    } = notif

    case HubManager.get_pid(state.namespace, operative_id) do
      {:ok, pid} when is_pid(pid) ->
        GenServer.cast(pid, {:apply_external_snapshot, serialized_state})

      _ ->
        Logger.warning(
          "[control_channel] Overwatch missing for shutdown notification — assumed migrated. " <>
            "namespace=#{inspect(state.namespace)} operative_id=#{inspect(operative_id)}"
        )
    end

    emit_runner_shutdown_ack(state, operative_id, reference_id)
  end

  # Converged teardown for every bounded exit trigger (quiescence, ctx.complete,
  # loop terminal, max_duration, cooperative stop): surface the outcome + result
  # payload, then deregister-on-completion so the operative never replays on boot.
  # A `status = "error"` tears down identically — no retry.
  defp handle_bounded_completed(state, completed) do
    %RunnerCommands.BoundedCompleted{
      operative_id: operative_id,
      status: status,
      error: error,
      result: result
    } = completed

    :telemetry.execute(
      [:flezha, :bounded, :completed],
      %{count: 1},
      %{
        namespace: state.namespace,
        operative_id: operative_id,
        status: status,
        result: result
      }
    )

    if status == "error" do
      Logger.warning(
        "[control_channel] bounded operative #{inspect(operative_id)} failed in namespace " <>
          "#{inspect(state.namespace)}: #{inspect(error)}"
      )

      :telemetry.execute(
        [:flezha, :bounded, :failed],
        %{count: 1},
        %{namespace: state.namespace, operative_id: operative_id, error: error}
      )
    end

    namespace = state.namespace

    # Finished-job retention: record the terminal outcome (durably, before the
    # teardown) so `flz operative info` can surface it and `stop_operative`
    # retains the state instead of deleting it. `finished_at` is stamped
    # controller-side (the wire carries no timestamp).
    _ =
      Flezha.State.Completion.write(operative_id, %{
        namespace: namespace,
        status: status,
        error: error,
        result: result,
        finished_at: System.system_time(:second)
      })

    # Tear down off the control-channel process: `stop_operative` drives a
    # RemOperative round-trip whose CommandResponse returns on THIS control
    # stream, so calling it inline would deadlock the reader.
    Task.start(fn ->
      case OperativeManager.stop_operative(namespace, operative_id) do
        :ok ->
          :ok

        {:error, reason} ->
          Logger.warning(
            "[control_channel] bounded operative #{inspect(operative_id)} teardown failed: #{inspect(reason)}"
          )
      end
    end)
  end

  defp emit_runner_shutdown_ack(state, operative_id, reference_id) do
    emit_control_request(
      state,
      reference_id,
      {:runner_shutdown_ack, %RunnerCommands.RunnerShutdownAck{operative_id: operative_id}},
      "RunnerShutdownAck operative=#{inspect(operative_id)}"
    )
  end

  # Proc-boundary commit (bounded-proc-journal + bounded-transactional-outbox):
  # append the `proc_committed` entry — entities ran, handoff, AND the procedure's
  # staged outbox in ONE record — and fsync BEFORE replying. The ack IS the
  # durable point the driver gates the next procedure on, and the point past
  # which the staged output may finally be delivered. A failed append (disk
  # error, oversized payload) rides back in `error` and terminates the run
  # abnormally on the SDK side. The append + fsync run in the journal's
  # WalWriter process; proc boundaries are coarse, so blocking this channel for
  # one fsync round-trip is acceptable (same posture as the awaited restore).
  #
  # `compensating` marks the undo twin: the same handshake carrying a
  # compensation hook's staged undo output, journaled as a `compensated` entry.
  defp handle_proc_commit(state, commit, reference_id) do
    %RunnerCommands.ProcCommit{
      operative_id: operative_id,
      proc_idx: proc_idx,
      iter: iter,
      entities_ran: entities_ran,
      handoff: handoff,
      outbox: outbox,
      compensating: compensating
    } = commit

    outbox =
      Enum.map(outbox, fn %RunnerCommands.ProcOutboxItem{} = item ->
        %{
          entity_id: item.entity_id,
          seq_in_proc: item.seq_in_proc,
          channel: item.channel,
          payload: item.payload
        }
      end)

    result =
      if compensating do
        Flezha.State.ProcJournal.append_compensated(operative_id, proc_idx, iter, outbox)
      else
        Flezha.State.ProcJournal.append_proc_committed(
          operative_id,
          proc_idx,
          iter,
          entities_ran,
          Enum.map(handoff, fn %RunnerCommands.ProcHandoffItem{channel_id: cid, payload: bytes} ->
            {cid, bytes}
          end),
          outbox
        )
      end

    :telemetry.execute(
      [:flezha, :state, :proc_commit],
      %{count: 1, handoff_items: length(handoff), outbox_items: length(outbox)},
      %{
        namespace: state.namespace,
        operative_id: operative_id,
        proc_idx: proc_idx,
        compensating: compensating,
        status: commit_status(result)
      }
    )

    durable? =
      journal_then_ack(
        state,
        reference_id,
        result,
        "proc-journal commit operative=#{inspect(operative_id)} proc_idx=#{proc_idx}",
        &{:proc_commit_response,
         %RunnerCommands.ProcCommitResponse{
           operative_id: operative_id,
           proc_idx: proc_idx,
           error: &1
         }}
      )

    # Flush AFTER the ack (the durable point has passed, so the output may now
    # be seen). The next procedure is free to run while this lands — the chain
    # hands data forward through the handoff, never through the sink.
    if durable?, do: flush_proc_outbox(operative_id)
  end

  # The protocol's core invariant, in one place: the reply to a durability
  # handshake is emitted only AFTER the journal append returned, carries the
  # append's outcome in its `error` field (empty = durable), and gates whatever
  # post-ack work the caller owes. Answers whether the record is durable.
  defp journal_then_ack(state, reference_id, result, label, response_fun) do
    error =
      case result do
        :ok -> ""
        {:error, reason} -> inspect(reason)
      end

    if error != "" do
      Logger.warning("[control_channel] #{label} failed: #{error}")
    end

    emit_control_request(state, reference_id, response_fun.(error), label)
    error == ""
  end

  defp commit_status(:ok), do: :ok
  defp commit_status({:error, _}), do: :error

  # Iteration boundary of a durable structured loop (`bounded-durable-loop`):
  # ONE durable `iter_committed(iter, consumed_offset)` record — with the
  # iteration's terminal handoff, so a crash can re-seed the next iteration —
  # then the ack. Everything after the ack is cleanup the commit does not depend
  # on: the boundary snapshot (so a rehydrate loads iteration-N state instead of
  # replaying the WAL tail), the iteration's staged outbox, and the release of
  # the inbox prefix it consumed. A failure in any of them costs work, never
  # correctness — the iteration is already committed.
  defp handle_iter_commit(state, commit, reference_id) do
    %RunnerCommands.IterCommit{
      operative_id: operative_id,
      iter: iter,
      consumed_offset: consumed_offset,
      handoff: handoff
    } = commit

    result =
      Flezha.State.ProcJournal.append_iter_committed(
        operative_id,
        iter,
        consumed_offset,
        Enum.map(handoff, fn %RunnerCommands.ProcHandoffItem{channel_id: cid, payload: bytes} ->
          {cid, bytes}
        end)
      )

    :telemetry.execute(
      [:flezha, :state, :iter_commit],
      %{count: 1, consumed_offset: consumed_offset},
      %{
        namespace: state.namespace,
        operative_id: operative_id,
        iter: iter,
        status: commit_status(result)
      }
    )

    durable? =
      journal_then_ack(
        state,
        reference_id,
        result,
        "proc-journal iteration commit operative=#{inspect(operative_id)} iter=#{iter}",
        &{:iter_commit_response,
         %RunnerCommands.IterCommitResponse{
           operative_id: operative_id,
           iter: iter,
           error: &1
         }}
      )

    if durable? do
      snapshot_iteration_boundary(state.runner_id, operative_id)
      flush_proc_outbox(operative_id)
      if consumed_offset > 0, do: Flezha.State.ProcInbox.truncate(operative_id, consumed_offset)
    end
  end

  # Boundary snapshot: ask the SDK to compact every Entity of the instance, so a
  # rehydrate restores iteration-N state from a snapshot instead of replaying the
  # whole WAL.
  #
  # Off this process: `CompactEntity` is dispatched through `send_control`,
  # which is a call INTO this very channel — issuing it inline would be the
  # channel calling itself (same reason the bounded-completed teardown runs in a
  # task). Best-effort besides: a failed or dropped compaction only means the
  # restore falls back to WAL replay, which the Watchdog's own size/age triggers
  # will re-attempt.
  defp snapshot_iteration_boundary(runner_id, operative_id) do
    entities =
      Flezha.State.Registry.all_entities()
      |> Enum.filter(fn {class, _, _} -> class == operative_id end)

    Task.start(fn ->
      Enum.each(entities, fn entity_id ->
        case Flezha.Operative.Overwatch.dispatch_compact_entity(runner_id, entity_id) do
          {:ok, _ref} ->
            :ok

          {:error, reason} ->
            Logger.debug(
              "[control_channel] boundary snapshot skipped for #{inspect(entity_id)}: " <>
                "#{inspect(reason)} — rehydrate falls back to WAL replay"
            )
        end
      end)
    end)
  end

  # The SDK's loop found its inbox empty at a committed iteration boundary and
  # tore its run down WITHOUT a completion (`bounded-durable-suspension`). Journal
  # the park record — durable BEFORE the eviction, so a controller crash in
  # between still rediscovers the suspension — then hand the instance to its
  # Overwatch, which evicts it from this runner and keeps it resumable.
  #
  # Off this process: the eviction drives a `RemOperative` whose response returns
  # on THIS control stream, so doing it inline would deadlock the reader (same
  # reason as the bounded-completed teardown).
  defp handle_iter_park(state, park) do
    %RunnerCommands.IterPark{
      operative_id: operative_id,
      iter: iter,
      wake_deadline_ms: wake_deadline_ms
    } = park

    deadline = if wake_deadline_ms == 0, do: nil, else: wake_deadline_ms

    :telemetry.execute(
      [:flezha, :bounded, :parked],
      %{count: 1},
      %{namespace: state.namespace, operative_id: operative_id, iter: iter}
    )

    namespace = state.namespace

    Task.start(fn ->
      case Flezha.State.ProcJournal.append_parked(operative_id, iter, deadline) do
        :ok ->
          case HubManager.get_pid(namespace, operative_id) do
            {:ok, pid} when is_pid(pid) ->
              Flezha.Operative.Overwatch.suspend(pid, iter, deadline)

            _ ->
              # No Overwatch to suspend (already migrated / torn down). The park
              # record is durable, so the instance is rediscovered as suspended
              # whenever it is next started.
              :ok
          end

        {:error, reason} ->
          # The park is not durable, so the instance must NOT be evicted: leave
          # it bound. Its driver has already exited, so it stays idle until the
          # runner reconnects or convergence rebinds it — at which point it
          # resumes from the last committed iteration and parks again.
          Logger.warning(
            "[control_channel] park record failed for #{inspect(operative_id)} " <>
              "iter=#{iter}: #{inspect(reason)} — instance left bound"
          )
      end
    end)
  end

  # A procedure asked for a bounded instance (`ctx.spawn_bounded`). The
  # namespace is this channel's — never the requester's claim — so a request
  # cannot reach another namespace.
  #
  # Off this process: the resolution starts an operative, which drives
  # `AddOperative` and waits for its response on THIS control stream; running it
  # inline would deadlock the reader (same reason as the bounded-completed
  # teardown). The reply is written back through `handle_info/2` so control
  # writes stay on this process.
  defp handle_spawn_bounded(state, request, reference_id) do
    %RunnerCommands.SpawnBoundedRequest{
      instance_id: instance_id,
      factory_id: factory_id,
      payload: payload,
      if_running: if_running
    } = request

    namespace = state.namespace
    channel = self()

    Task.start(fn ->
      outcome =
        OperativeManager.spawn_or_deliver(
          namespace,
          instance_id,
          factory_id,
          payload,
          if_running(if_running)
        )

      {name, error} = spawn_reply(outcome)

      :telemetry.execute(
        [:flezha, :operative, :requested_spawn],
        %{count: 1},
        %{
          namespace: namespace,
          instance_id: instance_id,
          factory_id: factory_id,
          outcome: name
        }
      )

      send(channel, {:spawn_bounded_result, reference_id, Atom.to_string(name), error})
    end)
  end

  # An unrecognised policy reads as the default: a requester on a newer SDK
  # must never have its payload silently refused by an older controller.
  defp if_running("error"), do: :error
  defp if_running(_), do: :deliver

  # `{:refused, reason}` is the only outcome carrying detail; the rest are their
  # own name with nothing attached.
  defp spawn_reply({:refused, reason}), do: {:refused, reason}
  defp spawn_reply(outcome), do: {outcome, ""}

  # Full-set replacement of this runner's declared factory list after a
  # post-start define/remove (sdk-runner-entrypoint-redesign). Fire-and-forget
  # by design — no ack — and dispatched as a cast so it never blocks the
  # control channel; the reconnect init frame remains the resync of record.
  defp handle_update_factories(state, %RunnerCommands.UpdateFactories{factories: factories}) do
    factories =
      Enum.map(factories, fn %RunnerCommands.FactoryDecl{id: id, description: description} ->
        %{"id" => id, "description" => if(description == "", do: nil, else: description)}
      end)

    GenServer.cast(
      FleetManager,
      {:sync_factories, state.namespace, state.runner_id, factories}
    )
  end

  # Journal-driven flush: everything this instance owes, in journal order, with
  # the `outbox_flushed` marker written per entry after delivery. An unbound
  # operative (cold-boot re-drive before the bind lands) retries on a timer;
  # the journal still owes the entries, so nothing is dropped either way.
  defp flush_proc_outbox(operative_id) do
    case Flezha.State.ProcJournal.flush_pending(operative_id) do
      :ok ->
        :ok

      {:error, :no_overwatch} ->
        Process.send_after(self(), {:proc_outbox_retry, operative_id}, @proc_outbox_retry_ms)
        :ok
    end
  end

  # Reply to an SDK-initiated CommandRequest with a controller-emitted
  # CommandRequest carrying the SAME reference_id (the SDK wakes its pending
  # waiter on it). Best-effort: a dead stream is logged, never raised — the
  # SDK side owns the timeout.
  defp emit_control_request(state, reference_id, command, log_label) do
    request = %RunnerCommands.CommandRequest{
      reference_id: reference_id,
      command: command
    }

    {:ok, iodata, _size} = Protox.encode(request)

    payload =
      Payloader.build_from_bin("control_command", IO.iodata_to_binary(iodata))

    case Map.get(state, :stream_ref) do
      nil ->
        :ok

      stream_ref ->
        case state.write_fun.(stream_ref, payload) do
          :ok ->
            :ok

          {:error, reason} ->
            Logger.warning("[control_channel] failed to emit #{log_label}: #{inspect(reason)}")

            :ok
        end
    end
  end
end
