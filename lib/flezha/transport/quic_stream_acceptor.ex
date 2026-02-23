defmodule Flezha.Transport.QuicStreamAcceptor do
  @moduledoc """
  Accepts incoming QUIC bidi streams from the SDK and routes them
  to the appropriate StreamChannel processes.

  Spawns concurrent accept tasks so that multiple streams arriving in
  rapid succession are all accepted (each task has its own acceptor in
  MsQuic's acceptor queue).

  When the SDK opens data streams (after ConnectSocket command), each stream
  sends two bytes: a type byte and a channel index byte.
  - Type 0x01: Primary stream (agent writes input to SDK)
  - Type 0x02: Secondary stream (SDK writes output to agent)
  - Index: 0-based channel index (converted to 1-based for StreamChannel routing)
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
    case :quicer.recv(stream, 2) do
      {:ok, <<type_byte, stream_index>>} ->
        case type_byte do
          unquote(Protocol.stream_type_primary()) ->
            route_to_channel(compute_engine_id, stream, :primary, stream_index + 1)

          unquote(Protocol.stream_type_secondary()) ->
            route_to_channel(compute_engine_id, stream, :secondary, stream_index + 1)

          other ->
            Logger.error("Unknown QUIC stream type byte: #{other}")
            :quicer.close_stream(stream)
        end

      {:error, reason} ->
        Logger.error("Failed to read stream type/index bytes: #{inspect(reason)}")
        :quicer.close_stream(stream)
    end
  end

  defp route_to_channel(compute_engine_id, stream, type, index) do
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
