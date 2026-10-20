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

  alias Flezha.Config
  alias Flezha.Config.Durations
  alias Flezha.Identificator
  alias Flezha.Payloader
  alias Flezha.Process.Watch
  alias Flezha.Runner.FleetManager
  alias Flezha.Runner.StreamPool
  alias Flezha.SDK.SafeCall
  alias Flezha.Socket.Support, as: SocketSupport
  alias Flezha.Transport.CertGenerator
  alias Flezha.Transport.Native
  alias Flezha.Transport.Stream, as: TStream

  require Logger

  use GenServer

  # Stream type constants (match flz_transport protocol.rs)
  @stream_type_primary 1
  @stream_type_secondary 2

  @quic_experimental "[Transport.Server] the QUIC transport is experimental: its settings and " <>
                       "behaviour may change drastically or it may be removed without a " <>
                       "compatibility break. See docs/public/publishing/maturity.md#quic"

  def start_link(opts) do
    transport = Keyword.fetch!(opts, :transport)
    GenServer.start_link(__MODULE__, transport, name: name(transport))
  end

  def name(:uds), do: :"#{__MODULE__}.Uds"
  def name(:quic), do: :"#{__MODULE__}.Quic"

  # Bind a QUIC data stream to the first rebindable channel in the runner's
  # pool, transfer its ownership to that channel and activate it; close it when
  # the pool is full. `stream_type` is the wire constant (1 primary / 2
  # secondary).
  defp bind_data_stream(runner_id, stream_type, handle) do
    case stream_role(stream_type) do
      nil ->
        {:error, :unknown_stream_type}

      type ->
        stream_ids = StreamPool.stream_ids(runner_id, type)

        case StreamPool.bind_stream_to_first_nil(stream_ids, handle) do
          {:bound, stream_pid, stream_name} ->
            TStream.controlling_process(handle, stream_pid)
            TStream.set_active(handle)
            {:ok, type, stream_name}

          nil ->
            TStream.close(handle)
            {:error, :no_channel}
        end
    end
  end

  @doc """
  List configured transports that are currently up.
  """
  @spec list_active_transports() :: [:uds | :quic]
  def list_active_transports() do
    Config.get(:transports)
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
        SafeCall.bounded(pid, :get_stats, Durations.get(:transport_stats_call_ms),
          on_error: {:error, :not_running}
        )

      _ ->
        {:error, :not_running}
    end
  end

  @doc "The node stop's last finalise row: every transport terminated, so none is restarted."
  def shutdown_steps,
    do: [%{scope: :node, order: 50, name: :transports, run: &__MODULE__.transports_step/2}]

  @doc false
  def transports_step(%{supervisor: sup}, _budget) do
    Enum.each(Config.get(:transports), &(:ok = Supervisor.terminate_child(sup, {__MODULE__, &1})))
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

  # Merge the NIF driver self-report when available. Both server drivers return
  # the same map. A NIF/decode failure must not break the stats call, so
  # tolerate it.
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
  def init(transport) when transport in [:uds, :quic] do
    # Only `terminate/2` releases the listening socket, and a supervisor's
    # shutdown reaches it only through a trapped exit.
    Process.flag(:trap_exit, true)
    :ok = Native.set_notice_sink(self())
    warn_if_transport_auth_disabled()
    listen(transport)
  end

  # Secure default posture: peer authentication is required unless the
  # user opted out explicitly, and the opt-out is warned on every boot
  # for as long as it remains set.
  @doc false
  def warn_if_transport_auth_disabled do
    unless transport_auth_required?() do
      Logger.warning(
        "[Transport.Server] #{Config.env_name(:transport_auth_required)}=off — " <>
          "peer authentication is DISABLED; " <>
          "any client that can reach the endpoint is accepted. Unset to restore the secure default."
      )
    end
  end

  defp transport_auth_required? do
    Config.get(:transport_auth_required)
  end

  defp listen(:uds) do
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

  defp listen(:quic) do
    Logger.warning(@quic_experimental)
    # Ensure TLS certificates exist before starting QUIC server.
    CertGenerator.ensure_certs()

    bind_address = Config.get(:quic_listen_address)
    cert_path = Config.get(:server_cert_path)
    key_path = Config.get(:server_key_path)

    client_ca_path =
      if transport_auth_required?() do
        Config.get(:quic_client_ca_path)
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
           quic_conn_monitors: Watch.new()
         }}

      {:error, reason} ->
        Logger.error("Failed to start NIF QUIC server: #{inspect(reason)}")
        {:stop, reason}
    end
  end

  # New connection/stream accepted — activate it for its first frame and store
  # the stream_ref. Streams start passive, so nothing arrives until then.
  @impl true
  def handle_info({:flz_connection, stream_ref}, state) do
    # Key pending streams by the driver-assigned stream id (the same id carried
    # by every {:flz_data,...}/{:flz_closed,...} for this stream) so the
    # connect_control_channel frame and the close event can be matched back to
    # the exact handle. The handle resource itself is opaque and not comparable
    # to the integer id the data/close messages carry.
    stream_id = TStream.stream_id(stream_ref)

    # Debug: the kubelet liveness probe opens+closes a connection every 30s,
    # so at info this pair dominates steady-state logs with no signal.
    Logger.debug(
      "[Transport.Server] New connection accepted (#{state.transport}), stream_id=#{stream_id}, stream_ref=#{inspect(stream_ref)}"
    )

    # Only the first frame, the connect_control_channel. The connect runs inside
    # this server, and a runner sends its next frames without waiting for it:
    # they stay in the driver until the runner's control channel takes the
    # stream, where this server's mailbox would drop them.
    TStream.set_active_once(stream_ref)

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
    # handed off to a ControlChannel/StreamChannel are not owned here, so
    # their close events never reach this clause — only still-pending ones do.
    pending = Map.delete(state.pending_streams, stream_id)
    Logger.debug("[Transport.Server] Connection closed: stream_id=#{stream_id}, reason=#{reason}")
    {:noreply, %{state | pending_streams: pending}}
  end

  # A QUIC runner's ControlChannel went down — drop its per-connection data
  # routing binding so the map stays bounded to live connections.
  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    case Watch.down(Map.get(state, :quic_conn_monitors) || Watch.new(), ref) do
      {:unknown, _monitors} ->
        {:noreply, state}

      {conn_ids, monitors} ->
        state = %{state | quic_conn_monitors: monitors}
        {:noreply, update_in(state, [:quic_conn_runners], &Map.drop(&1, conn_ids))}
    end
  end

  # What the transport core says for a reader: a driver error, a refused frame.
  @impl true
  def handle_info({:flz_notice, level, target, text}, state) do
    Logger.log(Map.get(%{warn: :warning, always: :warning}, level, level), "[#{target}] #{text}")
    {:noreply, state}
  end

  @impl true
  def handle_info(msg, state) do
    Logger.warning("[Transport.Server] Unexpected message: #{inspect(msg)}")
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, state) do
    if state[:server_ref] do
      Native.stop_server(state.server_ref)
    end

    :ok
  end

  defp handle_new_runner(stream_ref, conn_id, init_message, state) do
    init_payload = Payloader.json_to_map(init_message)
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
        # A dropped process is refused at its reconnect rate for as long as it
        # lives, by the operator's own choice: counted, not raised as an error.
        # The refusal as the runner reads it; what it does not read is logged
        # where it is withheld.
        error = Flezha.Error.to_wire(Flezha.Error.from_reason(reason, :"wire.connect"))

        Logger.log(
          if(match?({:runner_dropped, _, _}, reason), do: :debug, else: :error),
          "[Transport.Server] Rejected runner connect: #{error.message} (#{error.code}); closing stream"
        )

        emit_connect_rejected(reason, :authorization, state.transport)

        answer_refusal(stream_ref, error)
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

  # A refused runner is told why before its stream closes, in the same frame
  # every control command rides, so it reports the refusal rather than retrying
  # a connection that will never be admitted. The runner may already be gone.
  defp answer_refusal(stream_ref, error) do
    request = %RunnerCommands.CommandRequest{
      command: {:connect_refused, %RunnerCommands.ConnectRefused{error: error}}
    }

    {:ok, iodata, _size} = Protox.encode(%RunnerCommands.ControlFrame{frame: {:request, request}})
    TStream.send_frame(stream_ref, "control_command", IO.iodata_to_binary(iodata), %{})
  rescue
    _ -> :ok
  catch
    _kind, _reason -> :ok
  end

  # Closing a rejected/already-gone stream must never crash the transport
  # server, so tolerate a NIF decode/close failure here.
  defp safe_close(stream_ref) do
    TStream.close(stream_ref)
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

    case GenServer.whereis(Identificator.control_channel(runner_id)) do
      pid when is_pid(pid) ->
        update_in(state, [:quic_conn_monitors], &Watch.watch(&1, pid, conn_id))

      _ ->
        state
    end
  end

  # Route a QUIC data stream to the correct StreamChannel process. Bind to the
  # first nil-ref channel in the pool (rebind contract) — overwriting a live
  # channel would silently drop traffic on its existing stream. The SDK's
  # index hint is intentionally ignored: any index can map to any channel
  # since outbound routing is by relay_id, not stream index.
  defp route_quic_data_stream(stream_ref, stream_type, index, runner_id, state) do
    case bind_data_stream(runner_id, stream_type, stream_ref) do
      {:ok, type, stream_name} ->
        Logger.debug("[Transport.Server] QUIC #{type} stream ##{index} → #{inspect(stream_name)}")

      {:error, :no_channel} ->
        Logger.warning(
          "[Transport.Server] No rebindable channel for #{runner_id}; " <>
            "closing QUIC stream ##{index}"
        )

      {:error, :unknown_stream_type} ->
        Logger.warning("[Transport.Server] Unknown stream type: #{stream_type}")
    end

    {:noreply, state}
  end

  defp stream_role(@stream_type_primary), do: :primary
  defp stream_role(@stream_type_secondary), do: :secondary
  defp stream_role(_), do: nil
end
