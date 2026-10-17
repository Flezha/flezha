defmodule Flezha.Observability.Export.Supervisor do
  @moduledoc """
  Supervisor for the observability exporters (Prometheus, OTLP).

  Started AFTER `OtpBackend.Supervisor`, so the built-in sources exist
  before an exporter registers its sink interest. `:one_for_one`, so a
  crash in the Prometheus subtree leaves OTLP running and vice versa;
  each exporter has its own enable flag and both off means zero
  children.

  Also registers the declarative `:observability_export` source, so the
  exporters' own operational events ride the bus like any other.
  """

  use Supervisor

  alias Flezha.Config

  alias Flezha.Observability.Export.{
    OtlpClient,
    OtlpSink,
    PrometheusHttp,
    PrometheusRegistry,
    PrometheusSink
  }

  alias Flezha.Observability.Registry, as: ObsRegistry
  alias Flezha.Observability.Source

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

  defp register_export_source(), do: ObsRegistry.register_source(registration())

  @doc "The export source's registration."
  @spec registration() :: map()
  def registration() do
    %{
      name: @export_source,
      kinds: [:event],
      flavour: :declarative,
      cardinality_budget: 200,
      area: :controller,
      description: "Exporting signals to Prometheus and OTLP.",
      signals:
        Source.signals(
          prometheus_scrape: {:event, "Prometheus scraped the metrics."},
          prometheus_label_truncated:
            {:event, "A label value was too long for Prometheus and was cut."},
          otlp_batch_flushed: {:event, "A batch was sent to the OTLP collector."},
          otlp_batch_dropped:
            {:event, "A batch could not be sent to the OTLP collector and was dropped."}
        )
    }
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
    Config.get(:prometheus_enabled)
  end

  defp otlp_enabled?() do
    Config.get(:otlp_enabled)
  end

  @doc false
  @spec export_source() :: atom()
  def export_source(), do: @export_source
end
