defmodule Flezha.ComputeEngine.StreamAcceptor do
  @moduledoc """
  Starts a NIF UDS server on a per-compute-engine stream socket path
  (primary or secondary) and routes incoming SDK connections to
  StreamChannel processes in round-robin order.
  """

  alias Flezha.Transport.Native
  alias Flezha.Socket.Support, as: SocketSupport
  alias Flezha.ComputeEngine.StreamSup

  require Logger

  use GenServer

  def start_link({compute_engine_id, type}) do
    GenServer.start_link(__MODULE__, {compute_engine_id, type})
  end

  @impl true
  def init({compute_engine_id, type}) do
    path = SocketSupport.stream_socket_path(compute_engine_id, type)

    case Native.start_uds_server(path, self()) do
      {:ok, server_ref} ->
        Logger.info(
          "[StreamAcceptor] Listening on #{path} for #{type} streams (#{compute_engine_id})"
        )

        stream_ids = StreamSup.stream_ids(compute_engine_id, type)

        {:ok,
         %{
           server_ref: server_ref,
           compute_engine_id: compute_engine_id,
           type: type,
           stream_ids: stream_ids,
           next_index: 0
         }}

      {:error, reason} ->
        Logger.error("[StreamAcceptor] Failed to start #{type} listener: #{inspect(reason)}")
        {:stop, reason}
    end
  end

  # New SDK data stream connection accepted
  @impl true
  def handle_info({:flz_connection, stream_ref}, state) do
    idx = state.next_index
    count = length(state.stream_ids)

    if count == 0 do
      Logger.warning("[StreamAcceptor] No stream channels available for #{state.type}")
      {:noreply, state}
    else
      stream_name = Enum.at(state.stream_ids, rem(idx, count))
      stream_pid = GenServer.whereis(stream_name)

      if stream_pid do
        Native.controlling_process(stream_ref, stream_pid)
        Native.set_active(stream_ref)
        GenServer.call(stream_pid, {:attach_stream, stream_ref})

        Logger.debug(
          "[StreamAcceptor] Attached #{state.type} stream ##{idx} to #{inspect(stream_name)}"
        )
      else
        Logger.warning(
          "[StreamAcceptor] StreamChannel #{inspect(stream_name)} not found for #{state.type} stream ##{idx}"
        )
      end

      {:noreply, %{state | next_index: idx + 1}}
    end
  end

  @impl true
  def handle_info({:flz_data, _stream_id, _payload_id, _message, _metadata}, state) do
    # Data frames on freshly accepted streams before ownership transfer — ignore
    {:noreply, state}
  end

  @impl true
  def handle_info({:flz_closed, _stream_id, _reason}, state) do
    {:noreply, state}
  end

  @impl true
  def handle_info(msg, state) do
    Logger.warning("[StreamAcceptor] Unexpected message: #{inspect(msg)}")
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, state) do
    if Map.has_key?(state, :server_ref) do
      Native.stop_server(state.server_ref)
    end

    :ok
  end
end
