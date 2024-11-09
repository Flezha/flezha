defmodule Aflecha.Subscriber.ControlChannel do
  alias Aflecha.Socket.Support, as: SocketSupport
  alias Aflecha.Service.Identificator
  alias Aflecha.Service.Payloader

  require Logger

  use GenServer

  def start_link({subscriber_id, sock}) do
    GenServer.start_link(__MODULE__, {subscriber_id, sock},
      name: Identificator.control_channel(subscriber_id)
    )
  end

  def send_control(control_channel, payload_id, data) do
    ref = gen_ref()

    command =
      Jason.encode!(%{
        "data" => data,
        "reference_id" => ref
      })

    payload = Payloader.build_from_str(payload_id, command)

    GenServer.call(control_channel, {:send_control, payload})

    ref
  end

  @impl true
  def init({subscriber_id, sock}) do
    SocketSupport.set_active(sock)

    {:ok,
     %{
       subscriber_id: subscriber_id,
       sock: sock,
       control_unit: Identificator.control_unit(subscriber_id)
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
  def handle_info({:tcp_closed, _socket}, state) do
    Logger.warning("TCP connection closed for control socket.")

    {:noreply, state}
  end

  @impl true
  def handle_info({:tcp, _port, payload}, state) do
    GenServer.cast(state.control_unit, {:handle_payload, payload})

    {:noreply, state}
  end

  @impl true
  def handle_info(msg, state) do
    Logger.warning("Unexpected message at control channel: #{inspect(msg)}")

    {:noreply, state}
  end

  defp gen_ref() do
    make_ref()
    |> :erlang.ref_to_list()
    |> List.to_string()
  end
end
