defmodule Flecha.Engine.ControlChannel do
  alias Flecha.EnginesManager
  alias Flecha.Socket.Support
  alias Flecha.Utility.Identifier
  alias Flecha.Service.Payloader
  alias Flecha.Service.ConcurrencyMapper

  require Logger

  use GenServer

  @payload_id_connect_sockets "40"

  def start_link({engine_id, sock}) do
    GenServer.start_link(__MODULE__, {engine_id, sock},
      name: Identifier.control_channel(engine_id)
    )
  end

  def command_sockets_connect(engine_id) do
    sockets_count = ConcurrencyMapper.concurrent_entity_count()

    send_control(
      Identifier.control_channel(engine_id),
      @payload_id_connect_sockets,
      %{"sockets_count" => sockets_count}
    )
  end

  def send_control(control_channel, payload_id, data) do
    ref = gen_ref()

    command =
      Jason.encode!(%{
        "data" => data,
        "reference_id" => ref
      })

    payload = Payloader.build_payload(payload_id, command)

    GenServer.call(control_channel, {:send_control, payload})

    ref
  end

  @impl true
  def init({engine_id, sock}) do
    Support.take_ownership(sock)

    {:ok, %{engine_id: engine_id, sock: sock}}
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

    # TODO: shutdown engine

    {:noreply, state}
  end

  @impl true
  def handle_info({:tcp, _port, payload}, state) do
    GenServer.cast(EnginesManager, {:handle_payload, payload})

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
