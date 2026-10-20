defmodule Flezha.Observability.Export.Supervisor do
  @moduledoc """
  Supervisor for the observability exporters (Prometheus, OTLP).

  Started AFTER `OtpBackend.Supervisor`, so the built-in sources exist
  before an exporter registers its sink interest. `:one_for_one`, so a
  crash in the Prometheus subtree leaves OTLP running and vice versa;
  each exporter has its own enable flag and both off means zero
  children.

  The exporters' own operational events ride the bus like any other, on the
  `:observability_export` source `Flezha.Observability.Export.Source` declares.
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

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    children =
      List.flatten([
        if(prometheus_enabled?(), do: prometheus_children(), else: []),
        if(otlp_enabled?(), do: otlp_children(), else: [])
      ])

    Supervisor.init(children, strategy: :one_for_one)
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
end
