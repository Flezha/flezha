defmodule Flezha.ComputeEngine.StreamChannel do
  alias Flezha.Service.Payloader
  alias Flezha.Service.Identificator
  alias Flezha.Transport.Resolver

  require Logger

  use GenServer

  def start_link({compute_engine_id, name, type}) do
    GenServer.start_link(__MODULE__, {compute_engine_id, type, name}, name: name)
  end

  @impl true
  def init({compute_engine_id, type, name}) do
    transport = Resolver.transport_module()

    case Application.get_env(:flezha, :transport) do
      :quic ->
        # In QUIC mode, no per-stream socket listeners exist.
        # Wait for QuicStreamAcceptor to attach a stream via {:quic_stream_attached, stream}.
        {
          :ok,
          %{
            lsock: nil,
            sock: nil,
            transport: transport,
            processor_mapping: %{},
            name: name,
            buffer: {:empty, <<>>}
          }
        }

      _ ->
        # UDS mode: connect to listener and accept connection.
        lsock = GenServer.call(Identificator.stream_listener(compute_engine_id, type), :get_socket)

        {
          :ok,
          %{
            lsock: lsock,
            sock: nil,
            transport: transport,
            processor_mapping: %{},
            name: name,
            buffer: {:empty, <<>>}
          },
          {:continue, :accept_connection}
        }
    end
  end

  @impl true
  def handle_continue(:accept_connection, state) do
    socket = accept_connection(state.lsock, state.transport)

    {:noreply, %{state | sock: socket}}
  end

  @impl true
  def handle_cast(
        {:io_in, processor_id, {message, metadata}},
        %{sock: sock} = state
      ) do
    send_channel_payload(state.transport, sock, processor_id, message, metadata)

    {:noreply, state}
  end

  @impl true
  def handle_call(
        {:io_in, processor_id, {message, metadata}},
        _from,
        %{sock: sock} = state
      ) do
    send_channel_payload(state.transport, sock, processor_id, message, metadata)

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
  def handle_call({:quic_stream_attached, stream}, _from, state) do
    {:reply, :ok, %{state | sock: stream}}
  end

  # UDS transport messages
  @impl true
  def handle_info({:tcp_closed, _socket}, state) do
    Logger.warning("Connection closed for IO socket.")

    socket = accept_connection(state.lsock, state.transport)

    {:noreply, %{state | sock: socket}}
  end

  @impl true
  def handle_info({:tcp, _port, frame}, state) do
    handle_incoming_data(state, frame)
  end

  # QUIC transport messages
  @impl true
  def handle_info({:quic, :data, _stream, frame}, state) do
    handle_incoming_data(state, frame)
  end

  @impl true
  def handle_info({:quic, :stream_closed, _stream, _flags}, state) do
    Logger.warning("QUIC stream closed for IO socket.")

    {:noreply, %{state | sock: nil}}
  end

  @impl true
  def handle_info(msg, state) do
    Logger.warning("Unexpected message at io channel: #{inspect(msg)}")

    {:noreply, state}
  end

  defp handle_incoming_data(%{buffer: buffer} = state, frame) do
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

  defp send_channel_payload(transport, sock, processor_id, message, metadata) do
    transport.send_data(
      sock,
      Payloader.build_from_bin(processor_id, message, metadata)
    )
  end

  defp accept_connection(lsock, transport) do
    {:ok, socket} = :gen_tcp.accept(lsock)

    transport.set_active(socket)
    transport.controlling_process(socket, self())

    socket
  end
end
