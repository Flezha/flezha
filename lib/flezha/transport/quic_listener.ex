defmodule Flezha.Transport.QuicListener do
  @moduledoc """
  GenServer that listens for incoming QUIC connections from SDK compute engines.

  Accepts QUIC connections, performs handshake, reads the control init message
  from the first bidi stream, and routes to the FleetManager.
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
    CertGenerator.ensure_certs()

    {address, port} = parse_listen_address()
    server_cert_path = Application.get_env(:flezha, :server_cert_path, "app_data/certs/server.pem")
    server_key_path = Application.get_env(:flezha, :server_key_path, "app_data/certs/server.key")

    listen_opts = [
      certfile: to_charlist(server_cert_path),
      keyfile: to_charlist(server_key_path),
      alpn: [~c"flezha"],
      idle_timeout_ms: 30_000
    ]

    case :quicer.listen(port, listen_opts) do
      {:ok, listener} ->
        Logger.info("QUIC listener started on #{address}:#{port}")
        send(self(), :accept)
        {:ok, %{listener: listener}}

      {:error, reason} ->
        Logger.error("Failed to start QUIC listener: #{inspect(reason)}")
        {:stop, reason}
    end
  end

  @impl true
  def handle_info(:accept, state) do
    case :quicer.accept(state.listener, [], 30_000) do
      {:ok, conn} ->
        handle_new_connection(conn)

      {:error, :timeout} ->
        :ok

      {:error, reason} ->
        Logger.error("QUIC accept error: #{inspect(reason)}")
    end

    send(self(), :accept)
    {:noreply, state}
  end

  @impl true
  def handle_info(msg, state) do
    Logger.warning("QuicListener received unexpected message: #{inspect(msg)}")
    {:noreply, state}
  end

  defp handle_new_connection(conn) do
    case :quicer.handshake(conn) do
      {:ok, conn} ->
        # Accept the first bidi stream as control channel
        case :quicer.accept_stream(conn, []) do
          {:ok, control_stream} ->
            FleetManager.connect_handler_quic(conn, control_stream)

          {:error, reason} ->
            Logger.error("Failed to accept control stream: #{inspect(reason)}")
            :quicer.close_connection(conn)
        end

      {:error, reason} ->
        Logger.error("QUIC handshake failed: #{inspect(reason)}")
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
