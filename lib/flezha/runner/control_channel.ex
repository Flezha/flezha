defmodule Flezha.Runner.ControlChannel do
  @moduledoc """
  The controller side of one runner's control stream: a GenServer that owns
  the socket, decodes every inbound `CommandRequest` and writes every outbound
  one.

  Requests the SDK initiates are recognised here and carried out by
  `Flezha.Runner.ControlChannel.Requests`; requests the controller initiates
  are issued by `ControlUnit` and their responses are matched back to it by
  reference id. Durable barriers are forwarded to
  `Flezha.Runner.ControlChannel.Commits` rather than journaled inline.

  It also repairs the session's data plane: a data channel that loses its stream
  asks for a rebind, one runs at a time, and the loss after the session's rebind
  budget (`@rebinds`) ends it, so a data plane that cannot be bound is reclaimed.

  Invariant: this process is the only writer of control frames on the stream,
  which is what makes a reference id enough to pair a response with its
  request.
  """

  alias Flezha.Config.Durations
  alias Flezha.Identificator
  alias Flezha.Runner.ControlChannel.Requests
  alias Flezha.Runner.FleetManager
  alias Flezha.Runner.FleetManager.Marks
  alias Flezha.Runner.StreamChannel.Gate
  alias Flezha.SDK.SafeCall
  alias Flezha.Transport.LivenessCounters
  alias Flezha.Transport.Stream, as: TStream

  require Logger

  use GenServer
  use Flezha.Process.Timer, keys: [:liveness]

  # The rebinds one session may spend before its next data-plane loss ends it.
  @rebinds 3

  # Control-plane liveness: the SDK keepalive ping (and every other inbound
  # frame) re-arms `last_seen`; a jittered watchdog treats sustained silence
  # past the window as a connection close — the same signal `{:flz_closed}`
  # carries. Closes the UDS wedged-but-connected blind spot and makes UDS
  # symmetric with QUIC's idle timeout. Mirrors the data-plane `StreamChannel`
  # last_seen/threshold watchdog.

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
  def send_control(runner_id, command, timeout_ms \\ Durations.get(:control_call_timeout_ms)) do
    control_channel = Identificator.control_channel(runner_id)

    case GenServer.whereis(control_channel) do
      nil ->
        {:error, :no_control_channel}

      _ ->
        ref = gen_ref()
        bin = encode_request(ref, command)

        # Propagate the transport result instead of always returning {:ok, ref}.
        # A reset/dead stream replies {:error, :no_connection_available}; the
        # caller (ControlUnit) must then surface a clean error to the relay
        # rather than registering a reply ref that will never resolve.
        #
        # A channel that dies or stalls mid-call must answer its caller, not
        # exit it. The supervisor's reverse-order terminate sweep stops this
        # channel while a `ControlUnit` is still blocked here, and an exiting
        # unit loses every waiter in its `response_refs`. Same two clauses
        # `ControlUnit.ping_runner/2` uses one layer up, named for this layer.
        #
        # A full send buffer holds the caller until it drains, as a data
        # stream's sender is held: a refused frame was never sent, and a
        # restore that never reaches the runner leaves its Cell empty there.
        write = fn ->
          SafeCall.bounded(control_channel, {:send_control, bin}, timeout_ms,
            on_error: %{
              timeout: {:error, :timeout},
              other: &{:error, {:control_channel_exit, &1}}
            }
          )
        end

        case Gate.send_held(control_channel, write) do
          :ok -> {:ok, ref}
          :closed -> {:error, :no_connection_available}
          {:error, reason} -> {:error, reason}
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

  @doc "Rebuild `runner_id`'s data plane inside its live session (see the moduledoc)."
  def rebind_data_streams(runner_id),
    do: GenServer.cast(Identificator.control_channel(runner_id), :rebind_data_streams)

  defp encode_request(reference_id, command) do
    request = %RunnerCommands.CommandRequest{reference_id: reference_id, command: command}
    {:ok, iodata, _size} = Protox.encode(request)
    IO.iodata_to_binary(iodata)
  end

  @impl true
  def init({namespace, runner_id, stream_ref, write_fun}) do
    {:ok,
     %{
       namespace: namespace,
       runner_id: runner_id,
       identity: Marks.identity_of(runner_id),
       stream_ref: stream_ref,
       write_fun: write_fun || (&TStream.send_frame/4),
       control_unit: Identificator.control_unit(runner_id),
       liveness_ref: LivenessCounters.register_channel(runner_id, :control),
       last_seen: System.monotonic_time(:millisecond),
       rebinding?: false,
       rebinds: 0,
       send_gate_raised?: false,
       waiters: %{send_gate: MapSet.new()},
       timers: Flezha.Process.Timer.new()
     }
     |> bump_last_seen()
     |> arm_liveness_check()}
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
  def reset_stream_ref(runner_id) when is_binary(runner_id),
    do: call_channel(runner_id, :reset_stream_ref, {:error, :no_control_channel}, :warning)

  @doc """
  The process id of the runner on this channel's socket, or `nil` when there is
  none to give: no channel, a channel between streams, or a transport that
  cannot say. It is shown to the user and never trusted.
  """
  @spec peer_pid(binary()) :: non_neg_integer() | nil
  def peer_pid(runner_id) when is_binary(runner_id),
    do: call_channel(runner_id, :peer_pid, nil, false)

  @doc """
  Close `runner_id`'s control stream and end its session the way a socket
  close ends it, without waiting. The runner sees the close and reconnects.
  A caller that wants the close read as one it asked for marks the runner
  retiring first.
  """
  @spec close(binary()) :: :ok
  def close(runner_id) when is_binary(runner_id),
    do: GenServer.cast(Identificator.control_channel(runner_id), :close)

  defp call_channel(runner_id, request, on_error, log) do
    case GenServer.whereis(Identificator.control_channel(runner_id)) do
      nil ->
        on_error

      pid ->
        SafeCall.bounded(pid, request, Durations.get(:control_call_timeout_ms),
          on_error: on_error,
          log: log
        )
    end
  end

  @impl true
  def handle_call(:reset_stream_ref, _from, state) do
    {:reply, :ok, %{state | stream_ref: nil}}
  end

  def handle_call(:peer_pid, _from, %{stream_ref: nil} = state), do: {:reply, nil, state}

  def handle_call(:peer_pid, _from, state),
    do: {:reply, TStream.peer_pid(state.stream_ref), state}

  # A refusal is recorded in the same turn as the write, and a drain clears it
  # in this process too, so a sender that asks to wait after its refusal can
  # never miss the drain that answers it.
  @impl true
  def handle_call({:send_control, bin}, _from, state) do
    case write_control(state, bin) do
      :unbound -> {:reply, {:error, :no_connection_available}, state}
      {:error, :send_buffer_full} = full -> {:reply, full, %{state | send_gate_raised?: true}}
      result -> {:reply, result, state}
    end
  end

  def handle_call({:await_send_gate, pid}, _from, %{send_gate_raised?: true} = state),
    do: {:reply, :held, Gate.record_waiter(state, :send_gate, pid)}

  def handle_call({:await_send_gate, _pid}, _from, state), do: {:reply, :open, state}

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

  def handle_cast(:rebind_data_streams, %{rebinding?: true} = state), do: {:noreply, state}

  def handle_cast(:rebind_data_streams, %{rebinds: n} = state) when n >= @rebinds do
    Logger.warning("[control_channel] #{state.runner_id} lost its data plane after #{n} rebinds")
    handle_connection_closed(state)
  end

  # Off this process, so the channel keeps serving the session while it waits.
  def handle_cast(:rebind_data_streams, %{runner_id: runner_id} = state) do
    channel = self()

    Task.start_link(fn ->
      send(channel, {:rebind_answered, FleetManager.rebind_data_streams(runner_id)})
    end)

    {:noreply, %{state | rebinding?: true, rebinds: state.rebinds + 1}}
  end

  def handle_cast(:close, state) do
    if state.stream_ref, do: TStream.close(state.stream_ref)
    handle_connection_closed(%{state | stream_ref: nil}, "closed by the controller")
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
        {ack, log_label} = Requests.runner_shutdown(state, notif)
        emit_control_request(state, reference_id, ack, log_label)

      {:relay_completed, %{} = completed, _reference_id} ->
        Requests.relay_completed(state, completed)

      {barrier, %{} = commit, reference_id} when barrier in [:task_commit, :iter_commit] ->
        Requests.barrier(state, barrier, commit, reference_id)

      {:iter_park, %{} = park, _reference_id} ->
        Requests.iter_park(state, park)

      {:update_factories, %{} = update, _reference_id} ->
        Requests.update_factories(state, update)

      {:spawn_steps_request, %{} = request, reference_id} ->
        Requests.spawn_steps(state, request, reference_id)

      {:effect_result, %{} = result, reference_id} ->
        Requests.effect_result(state, result, reference_id)

      {:admin_request, %{} = request, reference_id} ->
        Requests.admin(state, request, reference_id)

      :not_sdk_initiated ->
        GenServer.cast(state.control_unit, {:handle_payload, message, metadata})
    end

    {:noreply, state}
  end

  # NIF transport: a send-buffer watermark crossing. A drain wakes the senders
  # held on the gate.
  @impl true
  def handle_info({:flz_send_gate, _stream_id, true}, state),
    do: {:noreply, %{state | send_gate_raised?: true}}

  def handle_info({:flz_send_gate, _stream_id, false}, state),
    do: {:noreply, wake_gate_waiters(%{state | send_gate_raised?: false}, :open)}

  # NIF transport: connection closed
  def handle_info({:flz_closed, _stream_id, reason}, state) do
    handle_connection_closed(state, reason)
  end

  # A refused rebind, whose reason the control unit logged, is asked for again,
  # and the budget bounds the asking.
  def handle_info({:rebind_answered, {:ok, _}}, state),
    do: {:noreply, %{state | rebinding?: false}}

  def handle_info({:rebind_answered, _refused}, state),
    do: handle_cast(:rebind_data_streams, %{state | rebinding?: false})

  # A request `Requests` resolved off this process; writing it is this
  # process's job, because it is the only writer of control frames.
  @impl true
  def handle_info({:sdk_reply, reference_id, command, log_label}, state) do
    emit_control_request(state, reference_id, command, log_label)
    {:noreply, state}
  end

  # Control-plane liveness watchdog. Sustained silence past the window — no
  # inbound frame, not even the SDK keepalive ping — is a process/control-channel
  # wedge UDS never surfaces as EOF. Route through the SAME
  # `handle_connection_closed/1` a real socket close uses (no parallel eviction
  # path). The window is the `control_liveness_window_ms` row — derived from the
  # keepalive it must outlast, not written beside it — and jittered, so a brief
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
      {:noreply, arm_liveness_check(state)}
    end
  end

  @impl true
  def handle_info(msg, state) do
    Logger.warning("Unexpected message at control channel: #{inspect(msg)}")

    {:noreply, state}
  end

  defp handle_connection_closed(state, reason \\ nil) do
    # Severity only. The eviction below runs identically either way — see
    # `runner-controller-connection-healing`: a real socket close drops the
    # runner immediately and unconditionally.
    if Marks.runner_retiring?(state.runner_id) do
      Logger.info(
        "Control socket closed for retiring runner #{state.runner_id} on #{state.namespace}"
      )
    else
      Logger.warning(
        "Connection closed for control socket for #{state.runner_id} on #{state.namespace}" <>
          if(reason, do: " (#{reason})", else: "")
      )
    end

    GenServer.cast(FleetManager, {:runner_disconnected, state.namespace, state.runner_id})

    {:noreply, wake_gate_waiters(state, :closed)}
  end

  defp wake_gate_waiters(state, result) do
    message = {:send_gate, Identificator.control_channel(state.runner_id), result}
    Gate.wake_waiters(state, :send_gate, message)
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

  # A non-positive interval turns the check off: nothing is armed and the key
  # simply holds no timer.
  defp arm_liveness_check(state) do
    interval = liveness_interval_ms()

    if interval > 0,
      do: arm_timer(state, :liveness, jittered(interval), :liveness_tick),
      else: state
  end

  defp liveness_window_ms,
    do: Durations.get(:control_liveness_window_ms)

  defp liveness_interval_ms,
    do: Durations.get(:control_liveness_interval_ms)

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
    :relay_completed,
    :task_commit,
    :iter_commit,
    :iter_park,
    :update_factories,
    :spawn_steps_request,
    :effect_result,
    :admin_request
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
