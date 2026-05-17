defmodule Flezha.Observability.Sink do
  @moduledoc """
  Behaviour for observability signal consumers.

  Every Sink runs as its own GenServer child under
  `Flezha.Observability.SinkSupervisor` (`:one_for_one`) so that one
  Sink crashing or stalling does not affect peer Sinks or producers.

  ## Filter contract

  A Sink declares the kinds and source globs it wants via `filter/0`.
  The bus consults this declaration before sending — Sinks never see
  signals they didn't ask for.

      %{kinds: [:metric, :event], sources: :any}
      %{kinds: [:log], sources: [:logger]}

  `:sources` may be the atom `:any` or a list of explicit source
  atoms. `:kinds` may be `:any` or a list of `Flezha.Observability.Signal.kinds/0`.

  ## Example

      defmodule MyCounterSink do
        @behaviour Flezha.Observability.Sink

        @impl true
        def init(_config), do: {:ok, %{count: 0}}

        @impl true
        def handle_signal(_signal, %{count: c} = state) do
          {:ok, %{state | count: c + 1}}
        end

        @impl true
        def config_schema, do: %{}

        @impl true
        def filter, do: %{kinds: :any, sources: :any}
      end

  ## Configuration validation

  `config_schema/0` returns a map describing the accepted configuration
  shape. Registration validates the supplied config against the schema
  before the Sink process is started — an invalid Sink is rejected at
  registration time, not at start time.
  """

  alias Flezha.Observability.Signal

  @type config :: map()
  @type filter :: %{
          required(:kinds) => :any | [Signal.kind()],
          required(:sources) => :any | [atom()]
        }

  @callback handle_signal(Signal.t(), state :: term()) ::
              {:ok, new_state :: term()} | {:error, term()}

  @callback config_schema() :: map()

  @callback filter() :: filter()
end
