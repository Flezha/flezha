defmodule Flezha.Observability.Export.Supervisor do
  @moduledoc """
  Supervisor for the observability exporters (Prometheus, OTLP).

  Sits under `Flezha.Observability.Supervisor`, started AFTER
  `OtpBackend.Supervisor` so built-in sources exist before exporters
  register their sink interest. `:one_for_one` so a crash in the
  Prometheus subtree leaves OTLP running and vice-versa.

  Each exporter has its own enable flag:

    * `:observability_export, :prometheus_enabled` — gates the
      Prometheus aggregation registry, sink, and HTTP listener.
    * `:observability_export, :otlp_enabled` — gates the OTLP codec,
      client, and sink.

  When both flags are `false`, the supervisor starts with zero
  children. When the master `:observability, :enabled` flag is
  `false`, the whole foundation tree is suppressed, transitively
  suppressing this subtree too.

  Also registers the declarative `:observability_export` source on
  the foundation Registry so the exporters' operational events
  (`:otlp_batch_flushed`, `:prometheus_scrape`, etc.) participate
  in the bus fan-out.
  """

  use Supervisor

  alias Flezha.Observability.Export.{
    OtlpClient,
    OtlpSink,
    PrometheusHttp,
    PrometheusRegistry,
    PrometheusSink
  }

  alias Flezha.Observability.Registry, as: ObsRegistry

  @export_source :observability_export

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    register_export_source()

    children =
      List.flatten([
        if(prometheus_enabled?(), do: prometheus_children(), else: []),
        if(otlp_enabled?(), do: otlp_children(), else: [])
      ])

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp register_export_source() do
    ObsRegistry.register_source(%{
      name: @export_source,
      kinds: [:event],
      flavour: :declarative,
      cardinality_budget: 200
    })
  end

  defp prometheus_children() do
    [
      PrometheusRegistry,
      PrometheusSink,
      PrometheusHttp
    ]
  end

  defp otlp_children() do
    [
      OtlpClient,
      OtlpSink
    ]
  end

  defp prometheus_enabled?() do
    Application.get_env(:flezha, :observability_export, [])
    |> Keyword.get(:prometheus_enabled, false)
  end

  defp otlp_enabled?() do
    Application.get_env(:flezha, :observability_export, [])
    |> Keyword.get(:otlp_enabled, false)
  end

  @doc false
  @spec export_source() :: atom()
  def export_source(), do: @export_source
end
