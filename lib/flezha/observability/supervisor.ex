defmodule Flezha.Observability.Supervisor do
  @moduledoc """
  Top-level supervisor for the observability foundation.

  Starts, in order: `Registry` (ETS state and the cardinality tracker),
  `Phoenix.PubSub`, `SinkSupervisor`, `SourceSupervisor`,
  `LoggerHandler`, `TelemetryBridge`, and the one-shot `DefaultSinks`
  that registers the two built-in sinks.

  Under `:observability` → `enabled: false` it starts no sinks, no logger
  handler and no bus: only the telemetry bridge's leveled lines, so an event
  that is also a log line is still written. Every service-API call answers
  `{:error, :observability_disabled}`.
  """

  use Supervisor

  alias Flezha.Config
  alias Flezha.Transport.LivenessCounters

  alias Flezha.Observability.{
    Counters,
    DefaultSinks,
    LoggerHandler,
    Registry,
    SinkSupervisor,
    SourceSupervisor,
    TelemetryBridge
  }

  alias Flezha.Observability.AuditLog.Supervisor, as: AuditLogSupervisor
  alias Flezha.Observability.Export.Supervisor, as: ExportSupervisor
  alias Flezha.Observability.OtpBackend.Supervisor, as: OtpBackendSupervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    Counters.init()
    # A named table dies with its owner. Created here, every relay's and
    # channel's counters outlive the one that happened to register first.
    LivenessCounters.ensure_table()

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
          ExportSupervisor,
          AuditLogSupervisor
        ]
      else
        [{TelemetryBridge, lines_only: true}]
      end

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp enabled?() do
    Config.get(:observability_enabled)
  end
end
