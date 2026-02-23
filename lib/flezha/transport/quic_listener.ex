defmodule Flezha.Transport.QuicListener do
  @moduledoc """
  GenServer that listens for incoming QUIC connections from SDK compute engines.

  Uses async_accept to avoid the problem where quicer's accept/3 consumes both
  `new_conn` and `connected` messages from the same connection, causing each
  real connection to be "accepted" twice.

  Flow:
  1. async_accept enqueues an acceptor on the listener
  2. When a client connects, we receive `{quic, new_conn, conn, _}`
  3. We spawn a per-connection handler that registers as stream acceptor
  4. We call async_handshake to start TLS
  5. When TLS completes and the SDK opens its control stream, the handler
     receives it via `{quic, new_stream, stream, _}`
  6. The handler reads the control payload and hands off to FleetManager

  The handler process is spawned BEFORE the handshake completes so that the
  stream acceptor is registered before the SDK can open streams. This avoids
  a race condition where MsQuic delivers the stream as an "orphan" to the
  connection owner (this GenServer) which would discard it.
  """

  alias Flezha.Transport.CertGenerator
  alias Flezha.ComputeEngine.FleetManager

  require Logger

  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    {:ok, _} = Application.ensure_all_started(:quicer)
    CertGenerator.ensure_certs()

    {address, port} = parse_listen_address()

    server_cert_path =
      Application.get_env(:flezha, :server_cert_path, "app_data/certs/server.pem")

    server_key_path = Application.get_env(:flezha, :server_key_path, "app_data/certs/server.key")

    listen_opts = %{
      certfile: to_charlist(server_cert_path),
      keyfile: to_charlist(server_key_path),
      alpn: [~c"flezha"],
      peer_bidi_stream_count: 100,
      peer_unidi_stream_count: 10,
      idle_timeout_ms: 30_000
    }

    case :quicer.listen(port, listen_opts) do
      {:ok, listener} ->
        Logger.info("QUIC listener started on #{address}:#{port}")
        # Enqueue the first acceptor
        :quicer.async_accept(listener, %{})
        {:ok, %{listener: listener, pending: MapSet.new()}}

      {:error, _reason, detail} ->
        Logger.error("Failed to start QUIC listener: #{inspect(detail)}")
        {:stop, detail}
    end
  end

  # A new connection arrived, needs handshake
  @impl true
  def handle_info({:quic, :new_conn, conn, _props}, state) do
    Logger.info("[QUIC] new_conn received, starting handshake")

    # Start the TLS handshake (async — will send {quic, connected, ...} when done)
    case :quicer.async_handshake(conn) do
      :ok ->
        # Spawn a per-connection handler BEFORE the handshake completes.
        # The handler registers itself as a stream acceptor so that when the
        # SDK opens the control stream (after TLS), the acceptor is already
        # in MsQuic's queue. This prevents the orphan-stream race condition.
        spawn(fn -> handle_connection(conn) end)

        # Re-register acceptor for next connection
        :quicer.async_accept(state.listener, %{})
        {:noreply, %{state | pending: MapSet.put(state.pending, conn)}}

      {:error, reason} ->
        Logger.error("QUIC async_handshake failed: #{inspect(reason)}")
        :quicer.close_connection(conn)
        :quicer.async_accept(state.listener, %{})
        {:noreply, state}
    end
  end

  # Connection handshake completed — the per-connection handler process
  # is already waiting for the control stream.
  @impl true
  def handle_info({:quic, :connected, conn, _props}, state) do
    Logger.info("[QUIC] connected received")
    {:noreply, %{state | pending: MapSet.delete(state.pending, conn)}}
  end

  # Handle transport shutdown (connection failed during handshake)
  @impl true
  def handle_info({:quic, :transport_shutdown, conn, props}, state) do
    Logger.error("QUIC transport shutdown: #{inspect(props)}")
    {:noreply, %{state | pending: MapSet.delete(state.pending, conn)}}
  end

  @impl true
  def handle_info({:quic, :closed, conn, _flags}, state) do
    Logger.warning("QUIC connection closed during setup")
    {:noreply, %{state | pending: MapSet.delete(state.pending, conn)}}
  end

  # Orphan streams that arrive before any handler registered an acceptor.
  # This is a fallback — in normal operation, the handler registers before
  # the stream arrives. Log for diagnostics.
  @impl true
  def handle_info({:quic, :new_stream, _stream, _props}, state) do
    Logger.warning("[QUIC] Received orphan stream (acceptor race condition)")
    {:noreply, state}
  end

  @impl true
  def handle_info(msg, state) do
    Logger.warning("QuicListener received unexpected message: #{inspect(msg)}")
    {:noreply, state}
  end

  # Runs in a spawned process per connection.
  # Registers as stream acceptor, waits for control stream, then hands off.
  defp handle_connection(conn) do
    # Register as stream acceptor on this connection's queue.
    # This is a NIF call that completes in microseconds, well before
    # the TLS handshake finishes and the SDK opens its stream.
    case :quicer.async_accept_stream(conn, %{active: false}) do
      {:ok, ^conn} ->
        receive do
          {:quic, :new_stream, stream, _props} ->
            Logger.info("[QUIC] Control stream accepted via handler process")
            FleetManager.connect_handler_quic(conn, stream)

          {:quic, :closed, _, _} ->
            Logger.error("[QUIC] Connection closed before control stream arrived")
        after
          20_000 ->
            Logger.error("[QUIC] Control stream accept timeout in handler")
            :quicer.close_connection(conn)
        end

      {:error, reason} ->
        Logger.error("[QUIC] async_accept_stream failed: #{inspect(reason)}")
        :quicer.close_connection(conn)
    end
  end

  defp parse_listen_address do
    address_str = Application.get_env(:flezha, :quic_listen_address, "0.0.0.0:4433")

    case String.split(address_str, ":") do
      [addr, port_str] ->
        {addr, String.to_integer(port_str)}

      _ ->
        {"0.0.0.0", 4433}
    end
  end
end
