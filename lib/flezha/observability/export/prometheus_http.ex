defmodule Flezha.Observability.Export.PrometheusHttp do
  @moduledoc """
  Prometheus text-exposition endpoint, built on `Flezha.Http.Listener`.

  Accepts exactly two request shapes:

    * `GET /metrics` — body is `PrometheusRegistry.render/0` with
      content-type `text/plain; version=0.0.4; charset=utf-8`.
    * `GET /` — body is a tiny static HTML link to `/metrics`.

  Everything else returns `404` / `405` / `400`. Default bind is
  `127.0.0.1`; non-loopback exposure requires explicit `:prometheus_bind`
  config.
  """

  use GenServer

  alias Flezha.Config
  alias Flezha.Http.Listener
  alias Flezha.Observability.Bus
  alias Flezha.Observability.Export.PrometheusRegistry
  alias Flezha.Observability.Signal

  # ---------- Public API ----------

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
    port = Config.get(:prometheus_port)
    bind = Config.get(:prometheus_bind)

    case Listener.listen(port, bind) do
      {:ok, listen_socket, actual_port} ->
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
    spawn(fn -> Listener.accept_loop(parent, listen_socket, &route/2) end)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, %{listen_socket: ls}) do
    _ = :gen_tcp.close(ls)
    :ok
  end

  # ---------- routing ----------

  defp route(socket, {:get, "/metrics"}), do: respond_metrics(socket)
  defp route(socket, {:get, "/"}), do: respond_index(socket)

  defp route(socket, {:get, _}),
    do: Listener.send_response(socket, 404, "text/plain", "not found\n")

  defp route(socket, {:method_not_allowed, _}),
    do: Listener.send_response(socket, 405, "text/plain", "method not allowed\n")

  defp route(socket, :bad), do: Listener.send_response(socket, 400, "text/plain", "bad request\n")

  defp respond_metrics(socket) do
    start = System.monotonic_time(:microsecond)
    {body, series_count} = PrometheusRegistry.render()

    Listener.send_response(socket, 200, "text/plain; version=0.0.4; charset=utf-8", body)

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

    Listener.send_response(socket, 200, "text/html; charset=utf-8", body)
  end
end
