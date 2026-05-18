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

  @behaviour Flezha.Observability.Sink

  alias Flezha.Observability.Export.PrometheusRegistry
  alias Flezha.Observability.Registry, as: ObsRegistry

  @sink_name :prometheus
  @default_high_watermark 50_000

  # ---------- Sink behaviour ----------

  @impl Flezha.Observability.Sink
  def handle_signal(signal, state) do
    PrometheusRegistry.record(signal)
    {:ok, state}
  end

  @impl Flezha.Observability.Sink
  def config_schema(), do: %{}

  @impl Flezha.Observability.Sink
  def filter(), do: %{kinds: [:metric], sources: :any}

  # ---------- GenServer ----------

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

  @impl GenServer
  def init(_opts) do
    # The foundation Registry's `register_sink/1` starts the sink
    # under SinkSupervisor; here we're already a child of Export.
    # Insert ourselves into the sink ETS table directly so the Bus
    # routes to us, mirroring what `do_register_sink/1` would have
    # done.
    {:ok, _} = register_self()

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
    _ = ObsRegistry.unregister_sink(@sink_name)
    :ok
  end

  # ---------- Internals ----------

  defp register_self() do
    pid = self()

    :ok =
      ObsRegistry.register_external_sink(%{
        name: @sink_name,
        module: __MODULE__,
        config: %{},
        filter: filter(),
        overflow: :drop,
        mailbox_high_watermark: @default_high_watermark,
        block_timeout_ms: 5_000,
        pid: pid
      })

    {:ok, pid}
  end
end
