defmodule Flezha.Observability.LogSink do
  @moduledoc """
  Opt-in sink writing each signal it accepts as one `Logger` line at info,
  rendered by `Flezha.Observability.Signal.line/1`: the log view of the sources
  its registration names. `FLEZHA_OBSERVABILITY_LOG_SOURCES=beam_vm` registers
  it for the BEAM's memory and load; nothing registers it by default.

  Its lines carry `flezha_event: true`, so the logger handler does not put a
  signal back on the bus as a `:log` one.
  """

  use GenServer, restart: :transient

  @behaviour Flezha.Observability.Sink

  require Logger

  alias Flezha.Observability.Signal

  @impl Flezha.Observability.Sink
  def config_schema(), do: %{}

  # Nothing unless the registration names its sources.
  @impl Flezha.Observability.Sink
  def filter(), do: %{kinds: :any, sources: []}

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @impl true
  def init(_opts), do: {:ok, nil}

  @impl true
  def handle_info({:signal, %Signal{} = signal}, state) do
    Logger.info(Signal.line(signal), flezha_event: true)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}
end
