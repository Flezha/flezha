defmodule Flezha.Transport.Server do
  @moduledoc """
  GenServer that manages the NIF transport server lifecycle.

  Starts the NIF UDS server, receives connections, reads the initial
  `connect_control_channel` frame, and dispatches to FleetManager.

  ## Message flow

  1. NIF accepts connection → sends `{:flz_connection, stream_ref}` here
  2. NIF reads data on the new stream, parses frames →
     sends `{:flz_data, stream_id, payload_id, message, metadata}` here
  3. This module receives the `connect_control_channel` frame and dispatches
     to FleetManager, which starts the compute engine supervisor and
     transfers stream ownership to the ControlChannel process.
  """

  alias Flezha.Transport.Native
  alias Flezha.ComputeEngine.FleetManager
  alias Flezha.Socket.Support, as: SocketSupport

  require Logger

  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    socket_path = SocketSupport.control_socket_path()

    case Native.start_uds_server(socket_path, self()) do
      {:ok, server_ref} ->
        Logger.info("NIF transport server started on #{socket_path}")
        {:ok, %{server_ref: server_ref, pending_streams: %{}}}

      {:error, reason} ->
        Logger.error("Failed to start NIF transport server: #{inspect(reason)}")
        {:stop, reason}
    end
  end

  # New connection accepted — activate and store the stream_ref.
  # Streams start in passive mode by default, so we must set_active to receive data.
  @impl true
  def handle_info({:flz_connection, stream_ref}, state) do
    Logger.info("[Transport.Server] New connection accepted")

    # Activate the stream so we receive the connect_control_channel frame.
    Native.set_active(stream_ref)

    pending = Map.put(state.pending_streams, stream_ref, true)
    {:noreply, %{state | pending_streams: pending}}
  end

  # Data frame: connect_control_channel handshake
  @impl true
  def handle_info(
        {:flz_data, _stream_id, "connect_control_channel", message, _metadata},
        state
      ) do
    # Find the pending stream (most recent connection)
    case Map.keys(state.pending_streams) do
      [stream_ref | _rest] ->
        pending = Map.delete(state.pending_streams, stream_ref)
        state = %{state | pending_streams: pending}
        handle_new_compute_engine(stream_ref, message, state)

      [] ->
        Logger.warning("[Transport.Server] connect_control_channel with no pending connection")
        {:noreply, state}
    end
  end

  # Forward any other data frames (shouldn't happen normally)
  @impl true
  def handle_info({:flz_data, _stream_id, payload_id, _message, _metadata}, state) do
    Logger.warning("[Transport.Server] Unexpected data frame: payload_id=#{payload_id}")
    {:noreply, state}
  end

  @impl true
  def handle_info({:flz_closed, _stream_id, reason}, state) do
    Logger.info("[Transport.Server] Connection closed: reason=#{reason}")
    {:noreply, state}
  end

  @impl true
  def handle_info(msg, state) do
    Logger.warning("[Transport.Server] Unexpected message: #{inspect(msg)}")
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, state) do
    if Map.has_key?(state, :server_ref) do
      Native.stop_server(state.server_ref)
    end

    :ok
  end

  defp handle_new_compute_engine(stream_ref, init_message, state) do
    init_payload = Flezha.Service.Payloader.json_to_map(init_message)
    namespace = Map.get(init_payload, "namespace")

    # Dispatch to FleetManager
    FleetManager.connect_handler(stream_ref, namespace, init_payload)

    {:noreply, state}
  end
end
