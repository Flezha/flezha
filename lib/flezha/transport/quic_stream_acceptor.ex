defmodule Flezha.Transport.QuicStreamAcceptor do
  @moduledoc """
  Accepts incoming QUIC bidi streams from the SDK and routes them
  to the appropriate StreamChannel processes.

  Spawns concurrent accept tasks so that multiple streams arriving in
  rapid succession are all accepted (each task has its own acceptor in
  MsQuic's acceptor queue).

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
    total_streams = stream_count * 2

    # Spawn concurrent accept tasks so each has its own acceptor slot.
    for _ <- 1..total_streams do
      Task.start(fn -> accept_and_route(compute_engine_id, conn) end)
    end

    {:ok, %{compute_engine_id: compute_engine_id, conn: conn}}
  end

  @impl true
  def handle_info(msg, state) do
    Logger.warning("QuicStreamAcceptor received unexpected message: #{inspect(msg)}")
    {:noreply, state}
  end

  defp accept_and_route(compute_engine_id, conn) do
    case :quicer.accept_stream(conn, %{active: false}, 30_000) do
      {:ok, stream} ->
        route_stream(compute_engine_id, stream)

      {:error, reason} ->
        Logger.error("Failed to accept QUIC data stream: #{inspect(reason)}")
    end
  end

  defp route_stream(compute_engine_id, stream) do
    case :quicer.recv(stream, 1) do
      {:ok, <<type_byte>>} ->
        case type_byte do
          unquote(Protocol.stream_type_primary()) ->
            route_to_channel(compute_engine_id, stream, :primary)

          unquote(Protocol.stream_type_secondary()) ->
            route_to_channel(compute_engine_id, stream, :secondary)

          other ->
            Logger.error("Unknown QUIC stream type byte: #{other}")
            :quicer.close_stream(stream)
        end

      {:error, reason} ->
        Logger.error("Failed to read stream type byte: #{inspect(reason)}")
        :quicer.close_stream(stream)
    end
  end

  defp route_to_channel(compute_engine_id, stream, type) do
    # Determine channel index based on stream type
    # Since tasks run concurrently, we use a simple approach:
    # each type gets index 1 (for single channel) or we track via the GenServer.
    # For now, with concurrent_channel_count streams, we use index 1.
    # TODO: Support multiple channels per type with proper index tracking.
    index = 1

    channel_name =
      case type do
        :primary -> Identificator.stream_channel_primary(compute_engine_id, index)
        :secondary -> Identificator.stream_channel_secondary(compute_engine_id, index)
      end

    case GenServer.whereis(channel_name) do
      nil ->
        Logger.error("#{type} stream channel #{inspect(channel_name)} not found")

      pid ->
        :quicer.controlling_process(stream, pid)
        :quicer.setopt(stream, :active, true)
        GenServer.call(pid, {:quic_stream_attached, stream})
    end
  end
end
