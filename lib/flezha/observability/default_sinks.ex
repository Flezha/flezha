defmodule Flezha.Observability.DefaultSinks do
  @moduledoc false
  # One-shot worker that registers the two foundation-default sinks
  # (`RingBufferSink`, `PubSubSink`) with the Registry once the rest
  # of the observability supervision tree is up. Exits `:normal` once
  # the work is done — restart policy is `:temporary` so the
  # supervisor does not bring it back.

  use GenServer

  alias Flezha.Observability.{PubSubSink, Registry, RingBufferSink}

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      restart: :temporary,
      type: :worker
    }
  end

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

    {:ok, :registered, {:continue, :stop}}
  end

  @impl true
  def handle_continue(:stop, state), do: {:stop, :normal, state}

  defp pubsub_sink_enabled?() do
    Application.get_env(:flezha, :observability, [])
    |> Keyword.get(:pubsub_sink_enabled, true)
  end
end
