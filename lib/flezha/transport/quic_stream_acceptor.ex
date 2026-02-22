defmodule Flezha.Transport.QuicStreamAcceptor do
  @moduledoc """
  Accepts incoming QUIC bidi streams from the SDK and routes them
  to the appropriate StreamChannel processes.

  When the SDK opens data streams (after ConnectSocket command), each stream
  sends a first byte identifying its type:
  - 0x01: Primary stream (agent writes input to SDK)
  - 0x02: Secondary stream (SDK writes output to agent)
  """

  alias Flezha.Transport.Protocol
  alias Flezha.Service.Identificator

  require Logger

  use GenServer

  def start_link({compute_engine_id, conn, stream_count}) do
    GenServer.start_link(__MODULE__, {compute_engine_id, conn, stream_count})
  end

  @impl true
  def init({compute_engine_id, conn, stream_count}) do
    # Accept 2*N streams (N primary + N secondary)
    total_streams = stream_count * 2

    send(self(), {:accept_streams, total_streams})

    {:ok,
     %{
       compute_engine_id: compute_engine_id,
       conn: conn,
       primary_index: 0,
       secondary_index: 0
     }}
  end

  @impl true
  def handle_info({:accept_streams, 0}, state) do
    Logger.info("All QUIC data streams accepted for #{state.compute_engine_id}")
    {:noreply, state}
  end

  @impl true
  def handle_info({:accept_streams, remaining}, state) do
    case :quicer.accept_stream(state.conn, []) do
      {:ok, stream} ->
        state = route_stream(state, stream)
        send(self(), {:accept_streams, remaining - 1})
        {:noreply, state}

      {:error, reason} ->
        Logger.error("Failed to accept QUIC stream: #{inspect(reason)}")
        {:noreply, state}
    end
  end

  @impl true
  def handle_info(msg, state) do
    Logger.warning("QuicStreamAcceptor received unexpected message: #{inspect(msg)}")
    {:noreply, state}
  end

  defp route_stream(state, stream) do
    # Read the type identifier byte
    case :quicer.recv(stream, 1) do
      {:ok, <<type_byte>>} ->
        case type_byte do
          unquote(Protocol.stream_type_primary()) ->
            route_to_channel(state, stream, :primary)

          unquote(Protocol.stream_type_secondary()) ->
            route_to_channel(state, stream, :secondary)

          other ->
            Logger.error("Unknown QUIC stream type byte: #{other}")
            :quicer.close_stream(stream)
            state
        end

      {:error, reason} ->
        Logger.error("Failed to read stream type byte: #{inspect(reason)}")
        :quicer.close_stream(stream)
        state
    end
  end

  defp route_to_channel(state, stream, :primary) do
    index = state.primary_index + 1

    channel_name =
      Identificator.stream_channel_primary(state.compute_engine_id, index)

    case GenServer.whereis(channel_name) do
      nil ->
        Logger.error("Primary stream channel #{inspect(channel_name)} not found")

      pid ->
        :quicer.controlling_process(stream, pid)
        :quicer.setopt(stream, :active, true)
        GenServer.call(pid, {:quic_stream_attached, stream})
    end

    %{state | primary_index: index}
  end

  defp route_to_channel(state, stream, :secondary) do
    index = state.secondary_index + 1

    channel_name =
      Identificator.stream_channel_secondary(state.compute_engine_id, index)

    case GenServer.whereis(channel_name) do
      nil ->
        Logger.error("Secondary stream channel #{inspect(channel_name)} not found")

      pid ->
        :quicer.controlling_process(stream, pid)
        :quicer.setopt(stream, :active, true)
        GenServer.call(pid, {:quic_stream_attached, stream})
    end

    %{state | secondary_index: index}
  end
end
