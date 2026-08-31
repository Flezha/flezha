defmodule Flezha.Observability.Source do
  @moduledoc """
  Behaviour for observability signal producers.

  A source is **declarative** — application code that calls
  `Flezha.Observability.Bus.emit/1` and owns no process — or
  **process-backed**, a GenServer under
  `Flezha.Observability.SourceSupervisor`. Both register through
  `Flezha.Observability.Registry.register_source/1`; registration is
  what makes a source visible, disable-able and budgeted.

  The foundation never calls a source back: a poller arms its own tick
  and emits from it, so this behaviour documents the shape rather than
  driving it. `Flezha.Observability.OtpBackend.Source` is the macro
  that writes that shape for the five sources this foundation ships.

  Worked examples: `docs/operations/observability.md` → Extending.
  """

  @type config :: map()

  @callback init(config()) :: {:ok, state :: term()} | {:error, term()}

  @callback config_schema() :: map()
end
