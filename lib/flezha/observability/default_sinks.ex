defmodule Flezha.Observability.DefaultSinks do
  @moduledoc false
  # One-shot worker that registers the foundation-default sinks
  # (`RingBufferSink`, `PubSubSink`, and `LogSink` for the sources
  # `:observability_log_sources` names) with the Registry once the rest
  # of the observability supervision tree is up. Exits `:normal` once
  # the work is done — restart policy is `:temporary` so the
  # supervisor does not bring it back.

  use GenServer, restart: :temporary

  alias Flezha.Config
  alias Flezha.Observability.{LogSink, PubSubSink, Registry, RingBufferSink}

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl true
  def init(_opts) do
    _ =
      Registry.register_sink(%{
        name: :ring_buffer,
        module: RingBufferSink,
        config: %{}
      })

    if pubsub_sink_enabled?() do
      _ =
        Registry.register_sink(%{
          name: :pubsub,
          module: PubSubSink,
          config: %{}
        })
    end

    case Config.get(:observability_log_sources) do
      [] ->
        :ok

      sources ->
        _ =
          Registry.register_sink(%{
            name: :log,
            module: LogSink,
            filter: %{kinds: :any, sources: sources}
          })
    end

    {:ok, :registered, {:continue, :stop}}
  end

  @impl true
  def handle_continue(:stop, state), do: {:stop, :normal, state}

  defp pubsub_sink_enabled?() do
    Config.get(:observability_pubsub_sink_enabled)
  end
end
