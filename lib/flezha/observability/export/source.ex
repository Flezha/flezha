defmodule Flezha.Observability.Export.Source do
  @moduledoc """
  The `:observability_export` source: the exporters' own operational events.
  Declared apart from `Flezha.Observability.Export.Supervisor`, which starts
  the exporters that emit on it.
  """

  use Flezha.Observability.Source,
    name: :observability_export,
    cardinality_budget: 200,
    area: :controller,
    title: "Metric export",
    description: "Signals handed to Prometheus and to an OTLP collector.",
    signals: [
      prometheus_scrape:
        {:event, label: "Scraped", meaning: "Prometheus collected the current metrics."},
      prometheus_label_truncated:
        {:event,
         label: "Label cut short",
         outcome: :failure,
         meaning: "A label value was too long for Prometheus, so it was cut short."},
      otlp_batch_flushed:
        {:event,
         label: "Batch sent", meaning: "A batch of signals was sent to the OTLP collector."},
      otlp_batch_dropped:
        {:event,
         label: "Batch dropped",
         outcome: :failure,
         meaning: "A batch of signals could not be sent to the OTLP collector and was dropped."}
    ]
end
