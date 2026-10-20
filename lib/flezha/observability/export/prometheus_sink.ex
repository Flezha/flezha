defmodule Flezha.Observability.Export.PrometheusSink do
  @moduledoc """
  Sink that aggregates `:metric` signals into the
  `PrometheusRegistry`. Filter is `:metric`-only; overflow policy is
  `:drop` (scrape-pull is resilient to a missed sample). Mailbox
  high-water default `50_000`.

  Registers itself with the foundation Registry on `init/1` so the
  Bus knows about it without a separate boot step. Restart is
  `:permanent` — losing the sink under load would silently zero the
  Prometheus exposition.
  """

  use GenServer
  use Flezha.Observability.Sink, name: :prometheus, overflow: :drop, watermark: 50_000

  alias Flezha.Observability.Export.PrometheusRegistry

  # ---------- Sink behaviour ----------

  @impl Flezha.Observability.Sink
  def config_schema(), do: %{}

  @impl Flezha.Observability.Sink
  def filter(), do: %{kinds: [:metric], sources: :any}

  # ---------- GenServer ----------

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl GenServer
  def init(_opts) do
    :ok = register_self()

    {:ok, %{}}
  end

  @impl GenServer
  def handle_info({:signal, signal}, state) do
    PrometheusRegistry.record(signal)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl GenServer
  def terminate(_reason, _state) do
    _ = unregister()
    :ok
  end
end
