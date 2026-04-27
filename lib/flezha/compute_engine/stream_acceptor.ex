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
           stream_ids: stream_ids
         }}

      {:error, reason} ->
        Logger.error("[StreamAcceptor] Failed to start #{type} listener: #{inspect(reason)}")
        {:stop, reason}
    end
  end

  # New SDK data stream connection accepted. Bind to the first nil-ref channel
  # in the pre-spawned pool (rebind contract) — overwriting a live channel
  # would silently drop traffic on its existing stream.
  @impl true
  def handle_info({:flz_connection, stream_ref}, state) do
    case StreamSup.bind_stream_to_first_nil(state.stream_ids, stream_ref) do
      {:bound, stream_pid, stream_name} ->
        Native.controlling_process(stream_ref, stream_pid)
        Native.set_active(stream_ref)

        Logger.debug(
          "[StreamAcceptor] Bound #{state.type} stream to #{inspect(stream_name)}"
        )

      nil ->
        Logger.warning(
          "[StreamAcceptor] No rebindable #{state.type} channel for " <>
            "#{state.compute_engine_id}; closing new stream"
        )

        Native.close(stream_ref)
    end

    {:noreply, state}
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
