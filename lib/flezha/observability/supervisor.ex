defmodule Flezha.Observability.Supervisor do
  @moduledoc """
  Top-level supervisor for the observability foundation.

  Children, in start order:

    1. `Flezha.Observability.Registry` — owns ETS state and the
       cardinality tracker.
    2. `Phoenix.PubSub` (name `Flezha.Observability.PubSub`) — used by
       the PubSub sink and by the Registry to broadcast registration
       events.
    3. `Flezha.Observability.SinkSupervisor` — `DynamicSupervisor`
       (`:one_for_one`) holding live sink processes.
    4. `Flezha.Observability.SourceSupervisor` — `DynamicSupervisor`
       (`:one_for_one`) holding process-backed sources.
    5. `Flezha.Observability.LoggerHandler` — installs the `:logger`
       handler.
    6. `Flezha.Observability.TelemetryBridge` — attaches the
       `:telemetry` bridge handler.
    7. `Flezha.Observability.DefaultSinks` — one-shot worker that
       registers the two default sinks with the Registry.

  When the master flag `:observability` → `enabled: false`, the
  supervisor starts with no children at all (no sinks, no logger
  handler, no telemetry bridge). Service-API calls return
  `{:error, :observability_disabled}`.
  """

  use Supervisor

  alias Flezha.Observability.{
    Counters,
    DefaultSinks,
    LoggerHandler,
    Registry,
    SinkSupervisor,
    SourceSupervisor,
    TelemetryBridge
  }

  alias Flezha.Observability.Export.Supervisor, as: ExportSupervisor
  alias Flezha.Observability.OtpBackend.Supervisor, as: OtpBackendSupervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    Counters.init()

    children =
      if enabled?() do
        [
          Registry,
          {Phoenix.PubSub, name: Flezha.Observability.PubSub},
          SinkSupervisor,
          SourceSupervisor,
          LoggerHandler,
          TelemetryBridge,
          DefaultSinks,
          OtpBackendSupervisor,
          ExportSupervisor
        ]
      else
        []
      end

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp enabled?() do
    Application.get_env(:flezha, :observability, [])
    |> Keyword.get(:enabled, true)
  end
end
