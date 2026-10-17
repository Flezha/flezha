defmodule Flezha.Observability.Source do
  @moduledoc """
  Behaviour for observability signal producers.

  A source is **declarative** — application code that calls
  `Flezha.Observability.Bus.emit/1` and owns no process — or
  **process-backed**, a GenServer under
  `Flezha.Observability.SourceSupervisor`. Both register through
  `Flezha.Observability.Registry.register_source/1`; registration is
  what makes a source visible, disable-able and budgeted.

  The foundation never calls a source back — a poller arms its own tick,
  and a process-backed source's `init/1` is GenServer's — so the sole
  callback is the registration schema, declared for every polled source
  by `Flezha.Observability.OtpBackend.Poller`.

  Worked examples: `docs/operations/observability.md` → Extending.
  """

  @callback config_schema() :: map()

  @doc """
  Signal declarations for a registration's `signals`, written as
  `[name: {kind, "what it says"}]`.
  """
  @spec signals(keyword({Flezha.Observability.Signal.kind(), String.t()})) :: [map()]
  def signals(declared) do
    for {name, {kind, meaning}} <- declared, do: %{name: name, kind: kind, description: meaning}
  end
end
