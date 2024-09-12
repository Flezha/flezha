defmodule Flecha.Engine.StreamChannel do
  alias Flecha.Service.Payloader

  require Logger

  use GenServer

  def start_link(name) do
    GenServer.start_link(__MODULE__, nil, name: name)
  end

  @impl true
  def init(_arg) do
    lsock = GenServer.call(:io_feed_listener, :get_socket)

    {:ok, %{lsock: lsock, sock: nil}, {:continue, :accept_connection}}
  end

  @impl true
  def handle_continue(:accept_connection, state) do
    socket = accept_connection(state.lsock)

    {:noreply, %{state | sock: socket}}
  end

  @impl true
  def handle_cast({:io_in, _type, computation_id, message}, %{socket_connection: sc} = state) do
    :gen_tcp.send(sc, Payloader.build_payload(computation_id, message))

    {:noreply, state}
  end

  @impl true
  def handle_cast({:computation_pid_update, computation_id, updated_pids}, state) do
    computation_mapping = Map.put(state[:computation_mapping], computation_id, updated_pids)

    {:noreply, Map.put(state, :computation_mapping, computation_mapping)}
  end

  @impl true
  def handle_info({:tcp_closed, _socket}, state) do
    Logger.warning("TCP connection closed for IO socket.")

    socket = accept_connection(state.lsock)

    {:noreply, %{state | sock: socket}}
  end

  @impl true
  def handle_info({:tcp, _port, payload}, state) do
    compute_mappings = state.computation_mapping

    Payloader.parse_payload(payload)
    |> Enum.each(fn {computation_id, message} ->
      computations = Map.get(compute_mappings, computation_id)

      Enum.each(computations, fn pid -> GenServer.cast(pid, {:io_out, :text, message}) end)
    end)

    {:noreply, state}
  end

  @impl true
  def handle_info(msg, state) do
    Logger.warning("Unexpected message at io channel: #{inspect(msg)}")

    {:noreply, state}
  end

  defp accept_connection(lsock) do
    {:ok, socket} = :gen_tcp.accept(lsock)

    :inet.setopts(socket, [{:active, true}])
    :gen_tcp.controlling_process(socket, self())
  end
end
