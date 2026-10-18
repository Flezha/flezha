defmodule Flezha.Observability.Sink do
  @moduledoc """
  Behaviour for observability signal consumers.

  A Sink is a process, one `:one_for_one` child under
  `Flezha.Observability.SinkSupervisor`, so one crashing or stalling
  reaches no peer and no producer. It declares the kinds and sources it
  wants via `filter/0`, which the bus consults before sending, and the
  configuration shape it accepts via `config_schema/0`, which the
  registry validates before the process is started — an invalid Sink is
  refused at registration, never at start.

  Delivery is a message, not a callback: the bus sends
  `{:signal, signal}` to the pid, asynchronously under the `:drop`
  overflow policy and as a `GenServer.call/3` under `:block`.
  """

  alias Flezha.Observability.Signal

  @type config :: map()
  @type filter :: %{
          required(:kinds) => :any | [Signal.kind()],
          required(:sources) => :any | [atom()]
        }

  @callback config_schema() :: map()

  @callback filter() :: filter()
end
