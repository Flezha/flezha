defmodule Flezha.Executor.ControlChannel do
  alias Flezha.Service.HubManager
  alias Flezha.Service.OperativeManager
  alias Flezha.Service.Identificator
  alias Flezha.Service.Payloader
  alias Flezha.Executor.FleetManager
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

  def start_link({namespace, executor_id, stream_ref}) do
    GenServer.start_link(__MODULE__, {namespace, executor_id, stream_ref, nil},
      name: Identificator.control_channel(executor_id)
    )
  end

  # Test-only constructor: accepts an explicit `write_fun` that the
  # `ExecutorShutdownAck` emit path uses in place of `Native.send_data/2`.
  # Production callers go through the 3-tuple form; the 4-tuple is here
  # because `Flezha.Transport.Native` is a NIF and cannot be Mimic-copied
  # (its `@on_load` triggers under the renamed module name and fails),
  # so unit tests inject a capturing closure instead.
  def start_link({namespace, executor_id, stream_ref, write_fun})
      when is_function(write_fun, 2) do
    GenServer.start_link(__MODULE__, {namespace, executor_id, stream_ref, write_fun},
      name: Identificator.control_channel(executor_id)
    )
  end

  def send_control(executor_id, command) do
    control_channel = Identificator.control_channel(executor_id)

    case GenServer.whereis(control_channel) do
      nil ->
        {:error, :no_control_channel}

      _ ->
        ref = gen_ref()

        req =
          struct(
            ExecutorCommands.CommandRequest,
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

  def send_control_async(executor_id, command) do
    control_channel = Identificator.control_channel(executor_id)

    case GenServer.whereis(control_channel) do
      nil ->
        :ok

      _ ->
        ref = gen_ref()

        req =
          struct(
            ExecutorCommands.CommandRequest,
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
  def init({namespace, executor_id, stream_ref, write_fun}) do
    schedule_liveness_check()

    {:ok,
     %{
       namespace: namespace,
       executor_id: executor_id,
       stream_ref: stream_ref,
       write_fun: write_fun || (&Native.send_data/2),
       control_unit: Identificator.control_unit(executor_id),
       last_seen: System.monotonic_time(:millisecond)
     }}
  end

  @doc """
  Synchronously clear this channel's `stream_ref` to `nil` so the existing
  rebind machinery (see `executor-controller-connection-healing`) can claim
  the next data stream the SDK opens. Used by
  `Flezha.Executor.FleetManager.reset_control_channel/1` after an end-to-end
  ping fails — the controller-side `ControlChannel` GenServer is alive but
  its underlying transport handle is stale, so subsequent `send_control` calls
  enqueue into a closed transport with no failure signal.
  """
  @spec reset_stream_ref(binary()) :: :ok | {:error, :no_control_channel}
  def reset_stream_ref(executor_id) when is_binary(executor_id) do
    control_channel = Identificator.control_channel(executor_id)

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
  # payload so the executor's idle watchdog re-arms; it never enters the
  # command path on either side. A wedged controller stops echoing, and the
  # executor's watchdog drives reconnect.
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
  # `ExecutorShutdownNotification` introduced by ship-fresh-state-on-sdk-
  # shutdown: it reuses the `CommandRequest` envelope and the same
  # `"control_command"` payload_id, so the only way to tell it apart on
  # the wire is to peek at the oneof tag. We attempt the
  # `CommandRequest` decode first; if the message has a recognised
  # SDK-initiated command, dispatch it here and skip the `ControlUnit`
  # handover.
  @impl true
  def handle_info({:flz_data, _stream_id, _payload_id, message, _metadata}, state) do
    state = bump_last_seen(state)

    case decode_sdk_initiated(message) do
      {:executor_shutdown_notification, %{} = notif, reference_id} ->
        handle_executor_shutdown_notification(state, notif, reference_id)
        {:noreply, state}

      {:bounded_completed, %{} = completed, _reference_id} ->
        handle_bounded_completed(state, completed)
        {:noreply, state}

      :not_sdk_initiated ->
        GenServer.cast(state.control_unit, {:handle_payload, message})
        {:noreply, state}
    end
  end

  # NIF transport: connection closed
  @impl true
  def handle_info({:flz_closed, _stream_id, _reason}, state) do
    handle_connection_closed(state)
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
          "#{state.executor_id} on #{state.namespace}; treating as connection close"
      )

      :telemetry.execute(
        [:flezha, :executor, :liveness_timeout],
        %{count: 1, silent_ms: silent_ms},
        %{executor_id: state.executor_id, namespace: state.namespace}
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
      "Connection closed for control socket for #{state.executor_id} on #{state.namespace}"
    )

    GenServer.call(FleetManager, {
      :executor_disconnected,
      state.namespace,
      state.executor_id
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
  # oneof tag (e.g. `:executor_shutdown_notification`) is the discriminator.
  # Anything else falls through to the existing `ControlUnit` path.
  #
  # Note: a `CommandResponse` will not decode cleanly as a `CommandRequest`
  # because their field-2 wire types disagree (string vs nested message);
  # Protox returns `{:error, _}` in that case, which maps to
  # `:not_sdk_initiated`.
  defp decode_sdk_initiated(message) do
    case ExecutorCommands.CommandRequest.decode(message) do
      {:ok,
       %ExecutorCommands.CommandRequest{
         reference_id: ref,
         command: {:executor_shutdown_notification, notif}
       }} ->
        {:executor_shutdown_notification, notif, ref}

      {:ok,
       %ExecutorCommands.CommandRequest{
         reference_id: ref,
         command: {:bounded_completed, completed}
       }} ->
        {:bounded_completed, completed, ref}

      _ ->
        :not_sdk_initiated
    end
  end

  # Apply the SDK's final state to the per-operative `Overwatch` and emit
  # an `ExecutorShutdownAck` so the SDK's bounded ack-wait can release.
  # The cast-not-call ordering is deliberate: the snapshot is enqueued on
  # Overwatch's mailbox before the ack flows back, so even if the controller
  # crashes between the cast and the write, the snapshot is durable in
  # the GenServer's state. See ship-fresh-state-on-sdk-shutdown design §D2.
  #
  # The ack is emitted unconditionally — including the
  # `:overwatch_missing` branch (Overwatch already migrated to a peer
  # controller) — so the SDK never blocks its container's shutdown grace
  # period waiting for an ack that will never arrive.
  defp handle_executor_shutdown_notification(state, notif, reference_id) do
    %ExecutorCommands.ExecutorShutdownNotification{
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

    emit_executor_shutdown_ack(state, operative_id, reference_id)
  end

  # Converged teardown for every bounded exit trigger (quiescence, ctx.complete,
  # loop terminal, max_duration, cooperative stop): surface the outcome + result
  # payload, then deregister-on-completion so the operative never replays on boot.
  # A `status = "error"` tears down identically — no retry.
  defp handle_bounded_completed(state, completed) do
    %ExecutorCommands.BoundedCompleted{
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

  defp emit_executor_shutdown_ack(state, operative_id, reference_id) do
    ack_request = %ExecutorCommands.CommandRequest{
      reference_id: reference_id,
      command: {:executor_shutdown_ack, %ExecutorCommands.ExecutorShutdownAck{operative_id: operative_id}}
    }

    {:ok, iodata, _size} = Protox.encode(ack_request)

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
            Logger.warning(
              "[control_channel] failed to emit ExecutorShutdownAck for " <>
                "operative=#{inspect(operative_id)}: #{inspect(reason)}"
            )

            :ok
        end
    end
  end
end
