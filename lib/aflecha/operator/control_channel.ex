defmodule Aflecha.Operator.ControlChannel do
  alias Aflecha.Socket.Support, as: SocketSupport
  alias Aflecha.Service.Identificator
  alias Aflecha.Service.Payloader
  alias Aflecha.Operator.FleetManager

  require Logger

  use GenServer

  def start_link({group_id, operator_id, sock}) do
    GenServer.start_link(__MODULE__, {group_id, operator_id, sock},
      name: Identificator.control_channel(operator_id)
    )
  end

  def send_control(operator_id, payload_id, {command_type, command}) do
    control_channel = Identificator.control_channel(operator_id)

    case GenServer.whereis(control_channel) do
      nil ->
        {:error, :no_control_channel}

      _ ->
        ref = gen_ref()

        req =
          struct(OperatorCommands.CommandRequest, [{:reference_id, ref}, {command_type, command}])

        {:ok, iodata, _size} = Protox.encode(req)

        payload = Payloader.build_from_bin(payload_id, iodata)

        GenServer.call(control_channel, {:send_control, payload})

        {:ok, ref}
    end
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
       buffer: {:empty, <<>>}
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
    {:reply, :ok, handle_payload(state, frame)}
  end

  @impl true
  def handle_info({:tcp_closed, _socket}, state) do
    Logger.warning(
      "TCP connection closed for control socket for #{state.operator_id} on #{state.group_id}"
    )

    GenServer.call(FleetManager, {
      :operator_disconnected,
      state.group_id,
      state.operator_id
    })

    {:noreply, state}
  end

  @impl true
  def handle_info({:tcp, _port, frame}, state) do
    {:noreply, handle_payload(state, frame)}
  end

  @impl true
  def handle_info(msg, state) do
    Logger.warning("Unexpected message at control channel: #{inspect(msg)}")

    {:noreply, state}
  end

  defp handle_payload(%{buffer: buffer} = state, frame) do
    {payloads, buffer} = Payloader.parse_frame(buffer, frame, [])

    Enum.each(payloads, fn {payload_id, message} ->
      # Send the payload the the control unit.
      GenServer.cast(
        state.control_unit,
        {:handle_payload, payload_id, message}
      )
    end)

    Map.put(state, :buffer, buffer)
  end

  defp gen_ref() do
    make_ref()
    |> :erlang.ref_to_list()
    |> List.to_string()
  end
end
