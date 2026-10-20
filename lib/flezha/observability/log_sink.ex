defmodule Flezha.Observability.LogSink do
  @moduledoc """
  Opt-in sink writing each signal it accepts as one `Logger` line at info,
  rendered by `Flezha.Observability.Signal.line/1`: the log view of the sources
  its registration names. `FLEZHA_OBSERVABILITY_LOG_SOURCES=beam_vm` registers
  it for the BEAM's memory and load; nothing registers it by default.

  Its lines carry `flezha_event: true`, so the logger handler does not put a
  signal back on the bus as a `:log` one. A signal the telemetry bridge already
  writes at its level (`TelemetryBridge.logged/1`) is not written again.
  """

  use GenServer, restart: :transient

  @behaviour Flezha.Observability.Sink

  require Logger

  alias Flezha.Observability.{Signal, TelemetryBridge}

  @impl Flezha.Observability.Sink
  def config_schema(), do: %{}

  # Nothing unless the registration names its sources.
  @impl Flezha.Observability.Sink
  def filter(), do: %{kinds: :any, sources: []}

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @impl true
  def init(_opts),
    do: {:ok, MapSet.new(Map.values(TelemetryBridge.logged()), &{elem(&1, 0), elem(&1, 1)})}

  @impl true
  def handle_info({:signal, %Signal{source: source, name: name} = signal}, logged) do
    if not MapSet.member?(logged, {source, name}),
      do: Logger.info(Signal.line(signal), flezha_event: true)

    {:noreply, logged}
  end

  def handle_info(_msg, state), do: {:noreply, state}
end
