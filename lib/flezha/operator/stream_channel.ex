defmodule Flezha.Operator.StreamChannel do
  alias Flezha.Service.Payloader
  alias Flezha.Service.Identificator

  require Logger

  use GenServer

  def start_link({operator_id, name, type}) do
    GenServer.start_link(__MODULE__, {operator_id, type, name}, name: name)
  end

  @impl true
  def init({operator_id, type, name}) do
    lsock = GenServer.call(Identificator.stream_listener(operator_id, type), :get_socket)

    {
      :ok,
      %{
        lsock: lsock,
        sock: nil,
        processor_mapping: %{},
        name: name,
        buffer: {:empty, <<>>}
      },
      {:continue, :accept_connection}
    }
  end

  @impl true
  def handle_continue(:accept_connection, state) do
    socket = accept_connection(state.lsock)

    {:noreply, %{state | sock: socket}}
  end

  @impl true
  def handle_cast(
        {:io_in, processor_id, {message, metadata}},
        %{sock: sock} = state
      ) do
    send_channel_payload(sock, processor_id, message, metadata)

    {:noreply, state}
  end

  @impl true
  def handle_call(
        {:io_in, processor_id, {message, metadata}},
        _from,
        %{sock: sock} = state
      ) do
    send_channel_payload(sock, processor_id, message, metadata)

    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:processor_pid_update, processor_id, updated_pids}, _from, state) do
    processor_mapping = Map.put(state[:processor_mapping], processor_id, updated_pids)

    {:reply, :ok, Map.put(state, :processor_mapping, processor_mapping)}
  end

  @impl true
  def handle_call({:processor_bulk_insert, processors}, _from, state) do
    processor_mapping =
      Enum.reduce(processors, state.processor_mapping, fn {processor_id, updated_pids}, acc ->
        Map.put(acc, processor_id, updated_pids)
      end)

    {:reply, :ok, Map.put(state, :processor_mapping, processor_mapping)}
  end

  @impl true
  def handle_info({:tcp_closed, _socket}, state) do
    Logger.warning("TCP connection closed for IO socket.")

    socket = accept_connection(state.lsock)

    {:noreply, %{state | sock: socket}}
  end

  @impl true
  def handle_info({:tcp, _port, frame}, %{buffer: buffer} = state) do
    processor_mappings = state.processor_mapping

    {payloads, buffer} = Payloader.parse_frame(buffer, frame, [])

    Enum.each(payloads, fn {processor_id, message, metadata} ->
      processors = Map.get(processor_mappings, processor_id, [])

      Enum.each(processors, fn pid ->
        GenServer.cast(pid, {:io_out, message, metadata})
      end)
    end)

    state = Map.put(state, :buffer, buffer)

    {:noreply, state}
  end

  @impl true
  def handle_info(msg, state) do
    Logger.warning("Unexpected message at io channel: #{inspect(msg)}")

    {:noreply, state}
  end

  defp send_channel_payload(sock, processor_id, message, metadata) do
    :gen_tcp.send(
      sock,
      Payloader.build_from_bin(processor_id, message, metadata)
    )
  end

  defp accept_connection(lsock) do
    {:ok, socket} = :gen_tcp.accept(lsock)

    :inet.setopts(socket, [{:active, true}])
    :gen_tcp.controlling_process(socket, self())

    socket
  end
end
