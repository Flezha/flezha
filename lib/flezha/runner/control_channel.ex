defmodule Flezha.Runner.ControlChannel do
  @moduledoc """
  The controller side of one runner's control stream: a GenServer that owns
  the socket, decodes every inbound `CommandRequest` and writes every outbound
  one.

  Requests the SDK initiates are answered here; requests the controller
  initiates are issued by `ControlUnit` and their responses are matched back to
  it by reference id. Durable barriers are forwarded to
  `Flezha.Runner.ControlChannel.Commits` rather than journaled inline.

  Invariant: this process is the only writer of control frames on the stream,
  which is what makes a reference id enough to pair a response with its
  request.
  """

  alias Flezha.Identificator
  alias Flezha.Operative.Migration
  alias Flezha.Runner.ControlChannel.Commits
  alias Flezha.Runner.FleetManager
  alias Flezha.Runner.FleetManager.Marks
  alias Flezha.Runner.StreamPool
  alias Flezha.Service.HubManager
  alias Flezha.Service.OperativeManager
  alias Flezha.State.Completion
  alias Flezha.State.ProcJournal
  alias Flezha.Transport.LivenessCounters
  alias Flezha.Transport.Stream, as: TStream

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

  # OTP's own `GenServer.call/2` default, made explicit because `send_control/3`
  # answers on it rather than letting a slow channel exit its caller. Tests pass
  # a short one to reach that arm without waiting five seconds.
  @control_call_timeout_ms 5_000

  def start_link({namespace, runner_id, stream_ref}) do
    GenServer.start_link(__MODULE__, {namespace, runner_id, stream_ref, nil},
      name: Identificator.control_channel(runner_id)
    )
  end

  # Test-only constructor: accepts an explicit `write_fun` — `(stream_ref,
  # payload_id, message, metadata) -> :ok | {:error, term}` — used in place of
  # the `Flezha.Transport.Stream.send_frame/4` seam. Production callers go
  # through the 3-tuple form; the 4-tuple is here because
  # `Flezha.Transport.Native` is a NIF and cannot be Mimic-copied (its
  # `@on_load` triggers under the renamed module name and fails), so unit tests
  # inject a capturing closure instead.
  def start_link({namespace, runner_id, stream_ref, write_fun})
      when is_function(write_fun, 4) do
    GenServer.start_link(__MODULE__, {namespace, runner_id, stream_ref, write_fun},
      name: Identificator.control_channel(runner_id)
    )
  end

  @spec send_control(binary(), tuple(), timeout()) :: {:ok, binary()} | {:error, term()}
  def send_control(runner_id, command, timeout_ms \\ @control_call_timeout_ms) do
    control_channel = Identificator.control_channel(runner_id)

    case GenServer.whereis(control_channel) do
      nil ->
        {:error, :no_control_channel}

      _ ->
        ref = gen_ref()
        bin = encode_request(ref, command)

        # Propagate the transport result instead of always returning {:ok, ref}.
        # A reset/dead stream replies {:error, :no_connection_available}; the
        # caller (ControlUnit) must then surface a clean error to the operative
        # rather than registering a reply ref that will never resolve.
        #
        # A channel that dies or stalls mid-call must answer its caller, not
        # exit it. The supervisor's reverse-order terminate sweep stops this
        # channel while a `ControlUnit` is still blocked here, and an exiting
        # unit loses every waiter in its `response_refs`. Same two clauses
        # `ControlUnit.ping_runner/2` uses one layer up, named for this layer.
        try do
          case GenServer.call(control_channel, {:send_control, bin}, timeout_ms) do
            :ok -> {:ok, ref}
            {:error, reason} -> {:error, reason}
          end
        catch
          :exit, {:timeout, _} -> {:error, :timeout}
          :exit, reason -> {:error, {:control_channel_exit, reason}}
        end
    end
  end

  @doc """
  Write a `CommandRequest` without waiting for the transport. `reference_id`
  defaults to a fresh one; a reply to an SDK-initiated request passes the
  request's own, which is what the SDK wakes its waiter on.
  """
  def send_control_async(runner_id, command, reference_id \\ nil) do
    control_channel = Identificator.control_channel(runner_id)

    case GenServer.whereis(control_channel) do
      nil ->
        :ok

      _ ->
        bin = encode_request(reference_id || gen_ref(), command)
        GenServer.cast(control_channel, {:send_control_async, bin})
        :ok
    end
  end

  defp encode_request(reference_id, command) do
    request = %RunnerCommands.CommandRequest{reference_id: reference_id, command: command}
    {:ok, iodata, _size} = Protox.encode(request)
    IO.iodata_to_binary(iodata)
  end

  @impl true
  def init({namespace, runner_id, stream_ref, write_fun}) do
    schedule_liveness_check()

    {:ok,
     %{
       namespace: namespace,
       runner_id: runner_id,
       stream_ref: stream_ref,
       write_fun: write_fun || (&TStream.send_frame/4),
       control_unit: Identificator.control_unit(runner_id),
       liveness_ref: LivenessCounters.register_channel(runner_id, :control),
       last_seen: System.monotonic_time(:millisecond)
     }
     |> bump_last_seen()}
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
  def handle_call({:send_control, bin}, _from, state) do
    res =
      case write_control(state, bin) do
        :unbound -> {:error, :no_connection_available}
        result -> result
      end

    {:reply, res, state}
  end

  @impl true
  def handle_call({:handle_frame, _frame}, _from, state) do
    # Framing is the NIF's; the frame never reaches this clause.
    {:reply, :ok, state}
  end

  @impl true
  def handle_cast({:send_control_async, bin}, state) do
    _ = write_control(state, bin)
    {:noreply, state}
  end

  # Every control frame leaves through this process, on its stream. `:unbound`
  # while the stream is reset: the caller decides whether that is an error.
  defp write_control(state, bin, payload_id \\ "control_command")
  defp write_control(%{stream_ref: nil}, _bin, _payload_id), do: :unbound

  defp write_control(state, bin, payload_id),
    do: state.write_fun.(state.stream_ref, payload_id, bin, %{})

  # Steady-state control-channel keepalive from the SDK (see flz_transport
  # liveness). Echoed back verbatim on the same dedicated `control_ping`
  # payload so the runner's idle watchdog re-arms; it never enters the
  # command path on either side. A wedged controller stops echoing, and the
  # runner's watchdog drives reconnect.
  @impl true
  def handle_info({:flz_data, _stream_id, "control_ping", message, _metadata}, state) do
    _ = write_control(state, message, "control_ping")
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
  #
  # This process does no disk I/O: a durability barrier is handed to the
  # runner's commit worker, so a commit storm never delays the ping, stop, park
  # or compaction queued behind it.
  @impl true
  def handle_info({:flz_data, _stream_id, _payload_id, message, metadata}, state) do
    state = bump_last_seen(state)

    case decode_sdk_initiated(message) do
      {:runner_shutdown_notification, %{} = notif, reference_id} ->
        handle_runner_shutdown(state, notif, reference_id)
        {:noreply, state}

      {:workflow_completed, %{} = completed, _reference_id} ->
        handle_workflow_completed(state, completed)
        {:noreply, state}

      {barrier, %{} = commit, reference_id} when barrier in [:proc_commit, :iter_commit] ->
        Commits.commit(state.runner_id, reference_id, commit)
        {:noreply, state}

      {:iter_park, %{} = park, _reference_id} ->
        handle_iter_park(state, park)
        {:noreply, state}

      {:update_factories, %{} = update, _reference_id} ->
        handle_update_factories(state, update)
        {:noreply, state}

      {:spawn_workflow_request, %{} = request, reference_id} ->
        handle_spawn_workflow(state, request, reference_id)
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
  def handle_info({:spawn_workflow_result, reference_id, outcome, error}, state) do
    emit_control_request(
      state,
      reference_id,
      {:spawn_workflow_response,
       %RunnerCommands.SpawnWorkflowResponse{outcome: outcome, error: error}},
      "spawn-workflow response #{outcome}"
    )

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
    # Severity only. The eviction below runs identically either way — see
    # `runner-controller-connection-healing`: a real socket close drops the
    # runner immediately and unconditionally.
    if Marks.runner_retiring?(state.runner_id) do
      Logger.info(
        "Control socket closed for retiring runner #{state.runner_id} on #{state.namespace}"
      )
    else
      Logger.warning(
        "Connection closed for control socket for #{state.runner_id} on #{state.namespace}"
      )
    end

    GenServer.call(FleetManager, {
      :runner_disconnected,
      state.namespace,
      state.runner_id
    })

    {:noreply, state}
  end

  # The watchdog's own `last_seen` and the shared counter are stamped together:
  # the first is this process's window, the second is the fact the convergence
  # loop reads beside the data-plane silence, so one row shows a live session
  # over a dead data plane. The window and its close are unchanged.
  defp bump_last_seen(state) do
    now = System.monotonic_time(:millisecond)
    LivenessCounters.set_channel(state[:liveness_ref], :last_control_frame_ms, now)
    %{state | last_seen: now}
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
    :workflow_completed,
    :proc_commit,
    :iter_commit,
    :iter_park,
    :update_factories,
    :spawn_workflow_request
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
  # the GenServer's state.
  #
  # The ack is emitted unconditionally — including the
  # `:overwatch_missing` branch (Overwatch already migrated to a peer
  # controller) — so the SDK never blocks its container's shutdown grace
  # period waiting for an ack that will never arrive.
  defp handle_runner_shutdown(state, notif, reference_id) do
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

  # Converged teardown for every workflow exit trigger (quiescence, ctx.complete,
  # loop terminal, max_duration, cooperative stop): surface the outcome + result
  # payload, then deregister-on-completion so the operative never replays on boot.
  # A `status = "error"` tears down identically — no retry.
  defp handle_workflow_completed(state, completed) do
    %RunnerCommands.WorkflowCompleted{
      operative_id: operative_id,
      status: status,
      error: error,
      result: result
    } = completed

    :telemetry.execute(
      [:flezha, :workflow, :completed],
      %{count: 1},
      %{
        namespace: state.namespace,
        operative_id: operative_id,
        status: status,
        result: result
      }
    )

    if status == "error", do: report_workflow_failure(state, operative_id, error)

    namespace = state.namespace

    # Finished-job retention: record the terminal outcome (durably, before the
    # teardown) so `flz operative info` can surface it and `stop_operative`
    # retains the state instead of deleting it. `finished_at` is stamped
    # controller-side (the wire carries no timestamp).
    _ =
      Completion.write(operative_id, %{
        namespace: namespace,
        status: status,
        error: error,
        result: result,
        finished_at: System.system_time(:second)
      })

    deregister_on_completion(state, operative_id)
  end

  defp report_workflow_failure(state, operative_id, error) do
    Logger.warning(
      "[control_channel] workflow operative #{inspect(operative_id)} failed in namespace " <>
        "#{inspect(state.namespace)}: #{inspect(error)}"
    )

    :telemetry.execute(
      [:flezha, :workflow, :failed],
      %{count: 1},
      %{namespace: state.namespace, operative_id: operative_id, error: error}
    )
  end

  # Tear down off the control-channel process: `stop_operative` drives a
  # RemOperative round-trip whose CommandResponse returns on THIS control
  # stream, so calling it inline would deadlock the reader.
  defp deregister_on_completion(%{namespace: namespace, runner_id: runner_id}, operative_id) do
    Task.start(fn ->
      drain_owed_outputs(runner_id, operative_id)

      case OperativeManager.stop_operative(namespace, operative_id) do
        :ok ->
          :ok

        {:error, reason} ->
          Logger.warning(
            "[control_channel] workflow operative #{inspect(operative_id)} teardown failed: #{inspect(reason)}"
          )
      end
    end)
  end

  # No Overwatch to suspend (already migrated / torn down) is not a failure: the
  # park record is durable, so the instance is rediscovered as suspended
  # whenever it is next started.
  defp suspend_parked(namespace, operative_id, iter, deadline) do
    case HubManager.get_pid(namespace, operative_id) do
      {:ok, pid} when is_pid(pid) -> Migration.suspend(pid, iter, deadline)
      _ -> :ok
    end
  end

  # A completion must not tear the Overwatch down under output still on its
  # way: a durable class's last outbox is flushed after its commit ack on this
  # channel, and an opted-out class's direct outputs travel the data stream
  # while the completion travels this one. Two debts, settled differently:
  #
  #   * the data stream is FLUSHED, once, by a barrier across the runner's
  #     stream channels. Nothing is counted — counting what the runner wrote
  #     against what the controller ran is decidable only while nothing is
  #     ever lost, and a frame routed to an Overwatch that has since been
  #     replaced is exactly that loss.
  #   * the journal is WAITED for, unchanged: `pending_entries/1` is an exact,
  #     durable, re-drivable debt, and `@drain_owed_ms` keeps a sink that never
  #     confirms from holding the teardown — the retained journal keeps the
  #     debt. Under a start storm the flush itself queues behind the channel
  #     for tens of seconds, and the instance is retained either way.
  @drain_owed_ms 60_000

  defp drain_owed_outputs(runner_id, operative_id) do
    StreamPool.flush_channels(runner_id)
    await_owed_journal(operative_id, System.monotonic_time(:millisecond) + @drain_owed_ms)
  end

  defp await_owed_journal(operative_id, deadline) do
    pending = ProcJournal.pending_entries(operative_id)

    cond do
      pending == [] ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        Logger.warning(
          "[control_channel] workflow operative #{inspect(operative_id)} completed with " <>
            "#{length(pending)} outbox entries still owed"
        )

      true ->
        ProcJournal.flush_pending(operative_id)
        Process.sleep(50)
        await_owed_journal(operative_id, deadline)
    end
  end

  defp emit_runner_shutdown_ack(state, operative_id, reference_id) do
    emit_control_request(
      state,
      reference_id,
      {:runner_shutdown_ack, %RunnerCommands.RunnerShutdownAck{operative_id: operative_id}},
      "RunnerShutdownAck operative=#{inspect(operative_id)}"
    )
  end

  # The SDK's loop found its inbox empty at a committed iteration boundary and
  # tore its run down WITHOUT a completion (`workflow-durable-suspension`). Journal
  # the park record — durable BEFORE the eviction, so a controller crash in
  # between still rediscovers the suspension — then hand the instance to its
  # Overwatch, which evicts it from this runner and keeps it resumable.
  #
  # Off this process: the eviction drives a `RemOperative` whose response returns
  # on THIS control stream, so doing it inline would deadlock the reader (same
  # reason as the workflow-completed teardown).
  defp handle_iter_park(state, park) do
    %RunnerCommands.IterPark{
      operative_id: operative_id,
      iter: iter,
      wake_deadline_ms: wake_deadline_ms
    } = park

    deadline = if wake_deadline_ms == 0, do: nil, else: wake_deadline_ms

    :telemetry.execute(
      [:flezha, :workflow, :parked],
      %{count: 1},
      %{namespace: state.namespace, operative_id: operative_id, iter: iter}
    )

    namespace = state.namespace

    Task.start(fn ->
      case ProcJournal.append_parked(operative_id, iter, deadline) do
        :ok ->
          suspend_parked(namespace, operative_id, iter, deadline)

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

  # A procedure asked for a workflow instance (`ctx.spawn_workflow`). The
  # namespace is this channel's — never the requester's claim — so a request
  # cannot reach another namespace.
  #
  # Off this process: the resolution starts an operative, which drives
  # `AddOperative` and waits for its response on THIS control stream; running it
  # inline would deadlock the reader (same reason as the workflow-completed
  # teardown). The reply is written back through `handle_info/2` so control
  # writes stay on this process.
  defp handle_spawn_workflow(state, request, reference_id) do
    %RunnerCommands.SpawnWorkflowRequest{
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

      send(channel, {:spawn_workflow_result, reference_id, Atom.to_string(name), error})
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
      Enum.map(factories, fn %RunnerCommands.FactoryDecl{
                               id: id,
                               description: description,
                               sdk_lang: sdk_lang
                             } ->
        %{
          "id" => id,
          "description" => if(description == "", do: nil, else: description),
          "sdk_lang" => sdk_lang
        }
      end)

    GenServer.cast(
      FleetManager,
      {:sync_factories, state.namespace, state.runner_id, factories}
    )
  end

  # Reply to an SDK-initiated CommandRequest with a controller-emitted
  # CommandRequest carrying the SAME reference_id (the SDK wakes its pending
  # waiter on it). Best-effort: a dead stream is logged, never raised — the
  # SDK side owns the timeout.
  defp emit_control_request(state, reference_id, command, log_label) do
    case write_control(state, encode_request(reference_id, command)) do
      {:error, reason} ->
        Logger.warning("[control_channel] failed to emit #{log_label}: #{inspect(reason)}")

      _ok_or_unbound ->
        :ok
    end

    :ok
  end
end
