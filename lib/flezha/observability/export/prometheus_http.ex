defmodule Flezha.Observability.Export.PrometheusHttp do
  @moduledoc """
  Hand-rolled `:gen_tcp` HTTP/1.1 listener serving the Prometheus
  text exposition format on a configurable port.

  Accepts exactly two request shapes:

    * `GET /metrics` — body is `PrometheusRegistry.render/0` with
      content-type `text/plain; version=0.0.4; charset=utf-8`.
    * `GET /` — body is a tiny static HTML link to `/metrics`.

  Everything else returns `404` (unknown path), `405` (method) or
  `400` (malformed request). Per-connection idle timeout default is
  `5_000` ms — a slow request is closed without producing a
  response. Default bind is `127.0.0.1`; non-loopback exposure
  requires explicit `:prometheus_bind` config.
  """

  use GenServer

  alias Flezha.Observability.Bus
  alias Flezha.Observability.Export.PrometheusRegistry
  alias Flezha.Observability.Signal

  @idle_timeout_ms 5_000
  @max_request_line_bytes 8_192

  # ---------- Public API ----------

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :worker,
      restart: :permanent
    }
  end

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Return the listening port — useful for tests that bind to port 0."
  @spec listen_port() :: {:ok, :inet.port_number()} | :error
  def listen_port() do
    case Process.whereis(__MODULE__) do
      pid when is_pid(pid) -> GenServer.call(pid, :listen_port)
      _ -> :error
    end
  end

  # ---------- GenServer ----------

  @impl true
  def init(_opts) do
    config = Application.get_env(:flezha, :observability_export, [])
    port = Keyword.get(config, :prometheus_port, 9568)
    bind = Keyword.get(config, :prometheus_bind, "127.0.0.1")

    {:ok, ip} = :inet.parse_address(String.to_charlist(bind))

    listen_opts = [
      :binary,
      packet: :raw,
      active: false,
      reuseaddr: true,
      ip: ip,
      backlog: 128
    ]

    case :gen_tcp.listen(port, listen_opts) do
      {:ok, listen_socket} ->
        {:ok, actual_port} = :inet.port(listen_socket)
        send(self(), :accept_loop)
        {:ok, %{listen_socket: listen_socket, port: actual_port}}

      {:error, reason} ->
        {:stop, {:listen_failed, reason}}
    end
  end

  @impl true
  def handle_call(:listen_port, _from, %{port: port} = state) do
    {:reply, {:ok, port}, state}
  end

  @impl true
  def handle_info(:accept_loop, %{listen_socket: listen_socket} = state) do
    parent = self()

    # Acceptor runs in its own process; we don't link it because the
    # acceptor exiting on a closed listen socket is the normal
    # shutdown sequence and should not propagate to the GenServer.
    spawn(fn -> accept_loop(parent, listen_socket) end)

    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, %{listen_socket: ls}) do
    _ = :gen_tcp.close(ls)
    :ok
  end

  # ---------- Acceptor / handler ----------

  defp accept_loop(parent, listen_socket) do
    case :gen_tcp.accept(listen_socket, 1_000) do
      {:ok, socket} ->
        spawn(fn ->
          try do
            serve(socket)
          catch
            _, _ -> :gen_tcp.close(socket)
          end
        end)

        accept_loop(parent, listen_socket)

      {:error, :timeout} ->
        # Heartbeat: re-check parent liveness so the loop terminates if the
        # supervisor stops us.
        if Process.alive?(parent), do: accept_loop(parent, listen_socket), else: :ok

      {:error, :closed} ->
        :ok

      {:error, _other} ->
        :ok
    end
  end

  defp serve(socket) do
    case read_request_line(socket) do
      {:ok, line} ->
        # Drain any pending headers (with the same timeout) before
        # responding so the client sees a clean response.
        _ = drain_headers(socket, 5)
        handle_request(socket, line)

      {:error, :timeout} ->
        # Slow client — close without response.
        :gen_tcp.close(socket)

      {:error, _} ->
        _ = send_response(socket, 400, "text/plain", "bad request\n")
        :gen_tcp.close(socket)
    end
  end

  defp read_request_line(socket) do
    :inet.setopts(socket, packet: :line)

    case :gen_tcp.recv(socket, 0, @idle_timeout_ms) do
      {:ok, line} when byte_size(line) > @max_request_line_bytes ->
        {:error, :too_long}

      {:ok, line} ->
        {:ok, line}

      err ->
        err
    end
  end

  defp drain_headers(socket, attempts) when attempts > 0 do
    case :gen_tcp.recv(socket, 0, 500) do
      {:ok, "\r\n"} -> :ok
      {:ok, "\n"} -> :ok
      {:ok, _other} -> drain_headers(socket, attempts - 1)
      _ -> :ok
    end
  end

  defp drain_headers(_socket, _), do: :ok

  defp handle_request(socket, line) do
    case parse_request_line(line) do
      {:get, "/metrics"} -> respond_metrics(socket)
      {:get, "/"} -> respond_index(socket)
      {:get, _} -> send_response(socket, 404, "text/plain", "not found\n")
      {:method_not_allowed, _} -> send_response(socket, 405, "text/plain", "method not allowed\n")
      :bad -> send_response(socket, 400, "text/plain", "bad request\n")
    end

    :gen_tcp.close(socket)
  end

  defp parse_request_line(line) do
    case String.split(String.trim(line)) do
      [method, path, "HTTP/" <> _] ->
        case String.upcase(method) do
          "GET" -> {:get, path}
          _ -> {:method_not_allowed, path}
        end

      _ ->
        :bad
    end
  end

  defp respond_metrics(socket) do
    start = System.monotonic_time(:microsecond)
    {body, series_count} = PrometheusRegistry.render()

    send_response(socket, 200, "text/plain; version=0.0.4; charset=utf-8", body)

    duration_us = System.monotonic_time(:microsecond) - start

    Bus.emit(
      Signal.new(%{
        kind: :event,
        source: :observability_export,
        name: :prometheus_scrape,
        measurements: %{duration_us: duration_us, series_count: series_count}
      })
    )
  end

  defp respond_index(socket) do
    body = """
    <!doctype html>
    <html><head><title>flezha metrics</title></head>
    <body><h1>Flezha</h1><p><a href="/metrics">/metrics</a></p></body></html>
    """

    send_response(socket, 200, "text/html; charset=utf-8", body)
  end

  defp send_response(socket, status, content_type, body) do
    body_iolist = IO.iodata_to_binary(body)
    size = byte_size(body_iolist)

    response = [
      "HTTP/1.1 ",
      Integer.to_string(status),
      " ",
      status_text(status),
      "\r\n",
      "Content-Type: ",
      content_type,
      "\r\n",
      "Content-Length: ",
      Integer.to_string(size),
      "\r\n",
      "Connection: close\r\n",
      "\r\n",
      body_iolist
    ]

    _ = :gen_tcp.send(socket, response)
    :ok
  end

  defp status_text(200), do: "OK"
  defp status_text(400), do: "Bad Request"
  defp status_text(404), do: "Not Found"
  defp status_text(405), do: "Method Not Allowed"
  defp status_text(_), do: "OK"
end
