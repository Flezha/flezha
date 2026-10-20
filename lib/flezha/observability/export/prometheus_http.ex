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

  use Flezha.Http.Listener, port: :prometheus_port, bind: :prometheus_bind

  alias Flezha.Http.Listener
  alias Flezha.Observability.Bus
  alias Flezha.Observability.Export.PrometheusRegistry
  alias Flezha.Observability.Signal

  defp route(socket, "/metrics"), do: respond_metrics(socket)
  defp route(socket, "/"), do: respond_index(socket)
  defp route(_socket, _path), do: :not_found

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
