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
    GenServer.call(FleetManager, {
      :operator_disconnected,
      state.group_id,
      state.operator_id
    })

    dbg()
    # TODO: continue from here:
    # We need to send a signal to the fleet manager to stop the operator.
    # But first we need to check if current agent does not have any other operators under
    # the group.
    # Case 1: No other operators under the group. We can simply stop the whole operator with the hub.
    # But make sure the hub is stopped gracefully to allow migration to other agents.
    # Now do this only if other agents are available. If no other agents are available we should preserve the
    # states somewhere.
    # Case 2: Other operators under the group. We need to get the current overwatch states
    # and stop the operator with the hub manager and pass start the state on the existing operator.

    Logger.warning("TCP connection closed for control socket.")

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
