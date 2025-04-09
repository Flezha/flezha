defmodule Aflecha.Operator.ControlChannel do
  alias Aflecha.Socket.Support, as: SocketSupport
  alias Aflecha.Service.Identificator
  alias Aflecha.Service.Payloader

  require Logger

  use GenServer

  def start_link({group_id, operator_id, sock}) do
    GenServer.start_link(__MODULE__, {group_id, operator_id, sock},
      name: Identificator.control_channel(operator_id)
    )
  end

  def send_control(control_channel, payload_id, data) do
    ref = gen_ref()

    command =
      Jason.encode!(%{
        "data" => data,
        "reference_id" => ref
      })

    payload = Payloader.build_from_bin(payload_id, command)

    GenServer.call(control_channel, {:send_control, payload})

    ref
  end

  @impl true
  def init({group_id, operator_id, sock}) do
    SocketSupport.set_active(sock)

    {:ok,
     %{
       group_id: group_id,
       operator_id: operator_id,
       sock: sock,
       control_unit: Identificator.control_unit(operator_id),
       buffer: {:empty, <<>>},
       bytes_left: <<>>
     }}
  end

  @impl true
  def handle_call({:send_control, payload}, _from, state) do
    res =
      case Map.get(state, :sock, nil) do
        nil ->
          {:error, :no_connection_available}

        socket ->
          :gen_tcp.send(socket, payload)
      end

    {:reply, res, state}
  end

  @impl true
  def handle_call({:handle_frame, frame}, _from, state) do
    state = handle_frame(state, frame)

    {:reply, :ok, state}
  end

  @impl true
  def handle_info({:tcp_closed, _socket}, state) do
    Logger.warning("TCP connection closed for control socket.")

    {:noreply, state}
  end

  @impl true
  def handle_info({:tcp, _port, frame}, state) do
    state = handle_frame(state, frame)

    {:noreply, state}
  end

  @impl true
  def handle_info(msg, state) do
    Logger.warning("Unexpected message at control channel: #{inspect(msg)}")

    {:noreply, state}
  end

  defp handle_frame(%{buffer: buffer} = state, frame) do
    buffer =
      buffer
      |> fill_buffer(frame)
      |> handle_buffer(state.control_unit)

    Map.put(state, :buffer, buffer)
  end

  defp handle_buffer({buffer, bytes_left}, control_unit) do
    case buffer do
      {:complete, {payload_id, message}} ->
        # Send the payload the the control unit.
        GenServer.cast(
          control_unit,
          {:handle_payload, payload_id, message}
        )

        fill_buffer({:empty, <<>>}, bytes_left)

      _ ->
        buffer
    end
  end

  defp handle_incomplete_buf(buf, payload_id, frame, bytes_to_take) do
    frame_size = byte_size(frame)

    if frame_size >= bytes_to_take do
      <<message::binary-size(bytes_to_take), bytes_left::binary>> = frame

      {{:complete, {payload_id, buf <> message}}, bytes_left}
    else
      <<message::binary-size(frame_size), _bytes_left::binary>> = frame
      bytes_to_take = bytes_to_take - frame_size

      {{:incomplete, {payload_id, buf <> message, bytes_to_take}}, <<>>}
    end
  end

  defp fill_buffer(buffer, <<>>), do: buffer

  defp fill_buffer(buffer, frame) do
    case frame do
      # TODO: update the prefix
      # FLZP payload prefix means its new payload.
      <<70, 76, 90, 80, bytes_rest::binary>> ->
        parse_payload_new(bytes_rest)

      # The frame is not a new payload.
      # It is a continuation of the previous payload.
      _ ->
        case buffer do
          # The existing buffer was empty, drop the frame
          {:empty, _buf} ->
            {{:empty, <<>>}, <<>>}

          {:incomplete, {payload_id, message_buf, bytes_to_take}} ->
            handle_incomplete_buf(message_buf, payload_id, frame, bytes_to_take)
        end
    end
  end

  defp parse_payload_new(frame) do
    case frame do
      <<id::binary-size(32), payload_size::64, frame_bytes::binary>> ->
        payload_id_trimmed = String.trim_trailing(id)

        handle_incomplete_buf(
          <<>>,
          payload_id_trimmed,
          frame_bytes,
          payload_size
        )

      _ ->
        # The frame is malformed, drop it.
        {{:empty, <<>>}, <<>>}
    end
  end

  defp gen_ref() do
    make_ref()
    |> :erlang.ref_to_list()
    |> List.to_string()
  end
end
