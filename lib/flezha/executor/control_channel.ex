defmodule Flezha.Executor.ControlChannel do
  alias Flezha.Service.HubManager
  alias Flezha.Service.Identificator
  alias Flezha.Service.Payloader
  alias Flezha.Executor.FleetManager
  alias Flezha.Transport.Native

  require Logger

  use GenServer

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

        GenServer.call(control_channel, {:send_control, payload})

        {:ok, ref}
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
    {:ok,
     %{
       namespace: namespace,
       executor_id: executor_id,
       stream_ref: stream_ref,
       write_fun: write_fun || (&Native.send_data/2),
       control_unit: Identificator.control_unit(executor_id)
     }}
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
    case decode_sdk_initiated(message) do
      {:executor_shutdown_notification, %{} = notif, reference_id} ->
        handle_executor_shutdown_notification(state, notif, reference_id)
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

      _ ->
        :not_sdk_initiated
    end
  end

  # Apply the SDK's final state to the per-relay `Overwatch` and emit
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
      relay_id: relay_id,
      serialized_state: serialized_state
    } = notif

    case HubManager.get_pid(state.namespace, relay_id) do
      {:ok, pid} when is_pid(pid) ->
        GenServer.cast(pid, {:apply_external_snapshot, serialized_state})

      _ ->
        Logger.warning(
          "[control_channel] Overwatch missing for shutdown notification — assumed migrated. " <>
            "namespace=#{inspect(state.namespace)} relay_id=#{inspect(relay_id)}"
        )
    end

    emit_executor_shutdown_ack(state, relay_id, reference_id)
  end

  defp emit_executor_shutdown_ack(state, relay_id, reference_id) do
    ack_request = %ExecutorCommands.CommandRequest{
      reference_id: reference_id,
      command:
        {:executor_shutdown_ack,
         %ExecutorCommands.ExecutorShutdownAck{relay_id: relay_id}}
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
                "relay=#{inspect(relay_id)}: #{inspect(reason)}"
            )

            :ok
        end
    end
  end
end
