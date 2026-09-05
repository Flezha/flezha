defmodule Flezha.Observability.AuditLog.Supervisor do
  @moduledoc """
  Supervisor for the durable audit-log subsystem.

  Gated on `:audit_log, :enabled` (default `false`). When disabled,
  starts with zero children — no disk I/O occurs and no `:audit_log`
  source/sink is registered.

  Children, in start order: Writer → Rotation → Retention → Sink.
  The Sink is last so it subscribes only after the Writer is ready
  to accept records (the bus's `:block` overflow would otherwise
  block producers on a dead writer).
  """

  use Supervisor

  alias Flezha.Config
  alias Flezha.Observability.AuditLog.{Retention, Rotation, Sink, Writer}
  alias Flezha.Observability.Registry, as: ObsRegistry

  @audit_source :audit_log

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    children =
      if enabled?() do
        register_audit_source()

        [
          Writer,
          Rotation,
          Retention,
          Sink
        ]
      else
        []
      end

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc false
  @spec enabled?() :: boolean()
  def enabled?() do
    Config.get(:audit_log_enabled)
  end

  defp register_audit_source() do
    ObsRegistry.register_source(%{
      name: @audit_source,
      kinds: [:event],
      flavour: :declarative,
      cardinality_budget: 200
    })
  end
end
