defmodule Flezha.Transport.Server do
  @moduledoc """
  GenServer that manages a NIF transport server lifecycle.

  Multiple instances can run simultaneously — one per enabled transport type.
  Each instance starts either a UDS or QUIC NIF server (based on its init arg),
  receives connections, reads the initial `connect_control_channel` frame, and
  dispatches to FleetManager with the transport type attached.

  ## Message flow

  1. NIF accepts connection/stream → sends `{:flz_connection, stream_ref}` here
  2. NIF reads data on the new stream, parses frames →
     sends `{:flz_data, stream_id, payload_id, message, metadata}` here
  3. This module receives the `connect_control_channel` frame and dispatches
     to FleetManager, which starts the compute engine supervisor and
     transfers stream ownership to the ControlChannel process.

  ## Transport modes

  - **UDS**: One control channel socket. Per-runner stream sockets
    are created by StreamAcceptors inside RunnerSupervisor.
  - **QUIC**: Single UDP socket handles everything. Control channels arrive
    as `{:flz_connection, stream_ref}`. Data streams arrive as
    `{:flz_stream, stream_ref, type, index}` after the 2-byte type prefix
    is consumed by the NIF driver. This module routes data streams to the
    correct StreamChannel based on the most recently established compute engine.
  """

  alias Flezha.Transport.Native
  alias Flezha.Runner.FleetManager
  alias Flezha.Runner.StreamSup
  alias Flezha.Socket.Support, as: SocketSupport

  require Logger

  use GenServer

  # Stream type constants (match flz_transport protocol.rs)
  @stream_type_primary 1
  @stream_type_secondary 2

  def start_link(opts) do
    transport = Keyword.fetch!(opts, :transport)
    GenServer.start_link(__MODULE__, transport, name: name(transport))
  end

  def name(:uds), do: :"#{__MODULE__}.Uds"
  def name(:quic), do: :"#{__MODULE__}.Quic"

  @doc """
  List configured transports that are currently up.
  """
  @spec list_active_transports() :: [:uds | :quic]
  def list_active_transports() do
    Application.get_env(:flezha, :transports, [:uds])
    |> Enum.filter(fn t ->
      case Process.whereis(name(t)) do
        pid when is_pid(pid) -> Process.alive?(pid)
        _ -> false
      end
    end)
  end

  @doc """
  Snapshot stats for a single transport. Returns a map carrying
  `:pending_streams` (count of accepted-but-not-yet-bound streams)
  and `:status` (`:up | :down`). Returns `{:error, :not_running}` if
  the server is not alive.
  """
  @spec get_stats(:uds | :quic) :: {:ok, map()} | {:error, :not_running}
  def get_stats(transport) when transport in [:uds, :quic] do
    case Process.whereis(name(transport)) do
      pid when is_pid(pid) ->
        try do
          GenServer.call(pid, :get_stats, 1_000)
        catch
          :exit, _ -> {:error, :not_running}
        end

      _ ->
        {:error, :not_running}
    end
  end

  @doc """
  Stop every configured transport server. Used by
  `Flezha.Shutdown.Coordinator` during the finalise phase so transport
  sockets/QUIC endpoints are closed before the application supervision
  tree's normal shutdown takes over.
  """
  @spec stop_all() :: :ok
  def stop_all() do
    transports = Application.get_env(:flezha, :transports, [:uds])

    Enum.each(transports, fn t ->
      pid = Process.whereis(name(t))

      if is_pid(pid) and Process.alive?(pid) do
        try do
          GenServer.stop(pid, :normal, 5_000)
        catch
          :exit, _ -> :ok
        end
      end
    end)
  end

  @impl true
  def handle_call(:get_stats, _from, state) do
    stats =
      %{
        transport: state.transport,
        status: :up,
        pending_streams: map_size(state.pending_streams || %{})
      }
      |> put_driver_stats(state.server_ref)

    {:reply, {:ok, stats}, state}
  end

  # Merge the NIF driver self-report when available. UDS returns a map; QUIC
  # returns `:undefined` (no per-stream stats yet). A NIF/decode failure must
  # not break the stats call, so tolerate it.
  defp put_driver_stats(stats, server_ref) do
    case Native.driver_stats(server_ref) do
      driver when is_map(driver) -> Map.put(stats, :driver, driver)
      _ -> stats
    end
  rescue
    _ -> stats
  catch
    _, _ -> stats
  end

  @impl true
  def init(:uds) do
    warn_if_transport_auth_disabled()
    init_uds()
  end

  def init(:quic) do
    warn_if_transport_auth_disabled()
    init_quic()
  end

  # Secure default posture: peer authentication is required unless the
  # operator opted out explicitly, and the opt-out is warned on every boot
  # for as long as it remains set.
  @doc false
  def warn_if_transport_auth_disabled do
    unless transport_auth_required?() do
      Logger.warning(
        "[Transport.Server] FLEZHA_TRANSPORT_AUTH=off — peer authentication is DISABLED; " <>
          "any client that can reach the endpoint is accepted. Unset to restore the secure default."
      )
    end
  end

  defp transport_auth_required? do
    Application.get_env(:flezha, :transport_auth_required, true)
  end

  defp init_uds do
    socket_path = SocketSupport.control_socket_path()

    case Native.start_uds_server(socket_path, self()) do
      {:ok, server_ref} ->
        Logger.info("NIF transport server (UDS) started on #{socket_path}")
        {:ok, %{server_ref: server_ref, transport: :uds, pending_streams: %{}}}

      {:error, reason} ->
        Logger.error("Failed to start NIF UDS server: #{inspect(reason)}")
        {:stop, reason}
    end
  end

  defp init_quic do
    # Ensure TLS certificates exist before starting QUIC server.
    Flezha.Transport.CertGenerator.ensure_certs()

    bind_address = Application.get_env(:flezha, :quic_listen_address, "0.0.0.0:4433")
    cert_path = Application.get_env(:flezha, :server_cert_path, "app_data/certs/server.pem")
    key_path = Application.get_env(:flezha, :server_key_path, "app_data/certs/server.key")

    client_ca_path =
      if transport_auth_required?() do
        Application.get_env(:flezha, :quic_client_ca_path, "app_data/certs/ca.pem")
      else
        ""
      end

    Logger.info(
      "[Transport.Server] Starting QUIC server: bind=#{bind_address}, cert=#{cert_path}, " <>
        "key=#{key_path}, client_auth=#{if client_ca_path == "", do: "off", else: client_ca_path}"
    )

    case Native.start_quic_server(bind_address, cert_path, key_path, client_ca_path, self()) do
      {:ok, server_ref} ->
        Logger.info("NIF transport server (QUIC) started on #{bind_address}")

        {:ok,
         %{
           server_ref: server_ref,
           transport: :quic,
           pending_streams: %{},
           # QUIC routing state: a per-connection runner binding. Each runner
           # uses ONE QUIC connection (control channel = first stream), so data
           # streams route by the connection id the driver tags them with — NOT
           # a shared "current runner" scalar (which misrouted on concurrent
           # connect). `quic_conn_monitors` maps each ControlChannel monitor ref
           # back to its conn id so the binding is dropped when the session ends,
           # keeping the map bounded to live connections.
           quic_conn_runners: %{},
           quic_conn_monitors: %{}
         }}

      {:error, reason} ->
        Logger.error("Failed to start NIF QUIC server: #{inspect(reason)}")
        {:stop, reason}
    end
  end

  # New connection/stream accepted — activate and store the stream_ref.
  # Streams start in passive mode by default, so we must set_active to receive data.
  @impl true
  def handle_info({:flz_connection, stream_ref}, state) do
    # Key pending streams by the driver-assigned stream id (the same id carried
    # by every {:flz_data,...}/{:flz_closed,...} for this stream) so the
    # connect_control_channel frame and the close event can be matched back to
    # the exact handle. The handle resource itself is opaque and not comparable
    # to the integer id the data/close messages carry.
    stream_id = Native.stream_id(stream_ref)

    # Debug: the kubelet liveness probe opens+closes a connection every 30s,
    # so at info this pair dominates steady-state logs with no signal.
    Logger.debug(
      "[Transport.Server] New connection accepted (#{state.transport}), stream_id=#{stream_id}, stream_ref=#{inspect(stream_ref)}"
    )

    # Activate the stream so we receive the connect_control_channel frame.
    Native.set_active(stream_ref)

    pending = Map.put(state.pending_streams, stream_id, stream_ref)
    {:noreply, %{state | pending_streams: pending}}
  end

  # QUIC data stream identified — route to the correct StreamChannel.
  # Sent by the QUIC driver after reading the 2-byte type+index prefix.
  @impl true
  def handle_info({:flz_stream, stream_ref, stream_type, index, conn_id}, state) do
    Logger.info(
      "[Transport.Server] QUIC data stream arrived: type=#{stream_type}, index=#{index}, conn_id=#{conn_id}, stream_ref=#{inspect(stream_ref)}"
    )

    case Map.get(state.quic_conn_runners, conn_id) do
      nil ->
        Logger.warning(
          "[Transport.Server] {:flz_stream} for conn_id=#{conn_id} with no runner bound yet"
        )

        {:noreply, state}

      runner_id ->
        route_quic_data_stream(stream_ref, stream_type, index, runner_id, state)
    end
  end

  # Data frame: connect_control_channel handshake. Bind to the exact stream the
  # frame arrived on — picking an arbitrary pending stream mis-binds the
  # ControlChannel to the wrong (often already-closed) handle whenever more than
  # one connection is pending, e.g. when liveness probes (`nc -zU`) churn the
  # control socket.
  @impl true
  def handle_info(
        {:flz_data, stream_id, "connect_control_channel", message, _metadata},
        state
      ) do
    case Map.pop(state.pending_streams, stream_id) do
      {nil, _pending} ->
        Logger.warning(
          "[Transport.Server] connect_control_channel for unknown stream_id=#{stream_id} (no pending connection)"
        )

        {:noreply, state}

      {stream_ref, pending} ->
        Logger.info(
          "[Transport.Server] Received connect_control_channel frame, stream_id=#{stream_id}, pending_streams=#{map_size(pending)}"
        )

        state = %{state | pending_streams: pending}
        # For QUIC, stream_id is the control stream's nif id == the conn_id the
        # driver tags this connection's data streams with.
        handle_new_runner(stream_ref, stream_id, message, state)
    end
  end

  # Forward any other data frames (shouldn't happen normally)
  @impl true
  def handle_info({:flz_data, _stream_id, payload_id, _message, _metadata}, state) do
    Logger.warning("[Transport.Server] Unexpected data frame: payload_id=#{payload_id}")
    {:noreply, state}
  end

  # The driver refuses a peer whose connection dies before it opens a stream —
  # an absent or untrusted client certificate. It is the same rejection the
  # application-level clause below reports, one layer down, so it rides the same
  # signal; `:warning` because an unauthorized dial is an expected outcome on a
  # public port, not a fault of this node.
  @impl true
  def handle_info({:flz_refused, reason}, state) do
    Logger.warning("[Transport.Server] Peer refused at the handshake: #{reason}")
    emit_connect_rejected(reason, :handshake, state.transport)
    {:noreply, state}
  end

  @impl true
  def handle_info({:flz_closed, stream_id, reason}, state) do
    # Drop the pending entry so streams that close before completing the
    # handshake (e.g. liveness-probe connects) don't accumulate. Streams already
    # handed off to a ControlChannel/StreamChannel are no longer owned here, so
    # their close events never reach this clause — only still-pending ones do.
    pending = Map.delete(state.pending_streams, stream_id)
    Logger.debug("[Transport.Server] Connection closed: stream_id=#{stream_id}, reason=#{reason}")
    {:noreply, %{state | pending_streams: pending}}
  end

  # A QUIC runner's ControlChannel went down — drop its per-connection data
  # routing binding so the map stays bounded to live connections.
  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    case Map.pop(Map.get(state, :quic_conn_monitors, %{}), ref) do
      {nil, _monitors} ->
        {:noreply, state}

      {conn_id, monitors} ->
        state = %{state | quic_conn_monitors: monitors}
        {:noreply, update_in(state, [:quic_conn_runners], &Map.delete(&1, conn_id))}
    end
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

  defp handle_new_runner(stream_ref, conn_id, init_message, state) do
    init_payload = Flezha.Service.Payloader.json_to_map(init_message)
    namespace = Map.get(init_payload, "namespace")

    # Dispatch to FleetManager — this creates the RunnerSupervisor.
    case FleetManager.connect_handler(stream_ref, namespace, init_payload, state.transport) do
      {:ok, runner_id} ->
        # For QUIC, bind this connection's id to the runner so its data
        # streams route by connection identity.
        state =
          if state.transport == :quic do
            track_quic_connection(state, conn_id, runner_id)
          else
            state
          end

        {:noreply, state}

      {:error, reason} ->
        # Close the rejected stream so the runner detects the dead/draining
        # controller promptly and its reconnect loop moves on to a live one,
        # instead of dangling on a controller that will never ack (e.g. a
        # `:shutting_down` rejection during drain). Emit a counted signal so the
        # reject is observable rather than only logged.
        Logger.error(
          "[Transport.Server] Rejected runner connect (#{inspect(reason)}); closing stream"
        )

        emit_connect_rejected(reason, :authorization, state.transport)

        safe_close(stream_ref)
        {:noreply, state}
    end
  end

  # Every refused peer is counted here, whatever layer refused it, so the signal
  # carries one shape and `stage` is the only thing that varies. How loud the
  # refusal is stays with the caller — the levels differ by stage.
  defp emit_connect_rejected(reason, stage, transport) do
    :telemetry.execute(
      [:flezha, :transport, :connect_rejected],
      %{count: 1},
      %{reason: reason, transport: transport, stage: stage}
    )
  end

  # Closing a rejected/already-gone stream must never crash the transport
  # server, so tolerate a NIF decode/close failure here.
  defp safe_close(stream_ref) do
    Native.close(stream_ref)
  rescue
    e -> Logger.debug("[Transport.Server] close of rejected stream failed: #{inspect(e)}")
  catch
    kind, reason ->
      Logger.debug("[Transport.Server] close of rejected stream #{kind}: #{inspect(reason)}")
  end

  # Bind a QUIC connection id to its runner for data-stream routing, and
  # monitor the runner's ControlChannel so the binding is dropped when the
  # session ends (keeping `quic_conn_runners` bounded to live connections).
  defp track_quic_connection(state, conn_id, runner_id) do
    state = put_in(state, [:quic_conn_runners, conn_id], runner_id)

    case GenServer.whereis(Flezha.Service.Identificator.control_channel(runner_id)) do
      pid when is_pid(pid) ->
        ref = Process.monitor(pid)
        put_in(state, [:quic_conn_monitors, ref], conn_id)

      _ ->
        state
    end
  end

  # Route a QUIC data stream to the correct StreamChannel process. Bind to the
  # first nil-ref channel in the pool (rebind contract) — overwriting a live
  # channel would silently drop traffic on its existing stream. The SDK's
  # index hint is intentionally ignored: any index can map to any channel
  # since outbound routing is by operative_id, not stream index.
  defp route_quic_data_stream(stream_ref, stream_type, index, runner_id, state) do
    type =
      case stream_type do
        @stream_type_primary ->
          :primary

        @stream_type_secondary ->
          :secondary

        _ ->
          Logger.warning("[Transport.Server] Unknown stream type: #{stream_type}")
          nil
      end

    if type do
      stream_ids = StreamSup.stream_ids(runner_id, type)

      case StreamSup.bind_stream_to_first_nil(stream_ids, stream_ref) do
        {:bound, stream_pid, stream_name} ->
          Native.controlling_process(stream_ref, stream_pid)
          Native.set_active(stream_ref)

          Logger.debug(
            "[Transport.Server] QUIC #{type} stream ##{index} → #{inspect(stream_name)}"
          )

        nil ->
          Logger.warning(
            "[Transport.Server] No rebindable #{type} channel for #{runner_id}; " <>
              "closing QUIC stream ##{index}"
          )

          Native.close(stream_ref)
      end
    end

    {:noreply, state}
  end
end
