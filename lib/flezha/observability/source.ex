defmodule Flezha.Observability.Source do
  @moduledoc """
  Behaviour for observability signal producers.

  Sources come in two flavours and a producer chooses one at
  registration time:

  ### Declarative source

  No process owned by the foundation. The producer is application code
  that calls `Flezha.Observability.Bus.emit/1` directly. Register it
  with `Flezha.Observability.Registry.register_source/1` so the
  registry knows about it (and so the cardinality budget is tracked).

      iex> Flezha.Observability.Registry.register_source(%{
      ...>   name: :my_metric_source,
      ...>   kinds: [:metric],
      ...>   flavour: :declarative,
      ...>   cardinality_budget: 200
      ...> })
      :ok

      iex> Flezha.Observability.Bus.emit(
      ...>   Flezha.Observability.Signal.new(%{
      ...>     kind: :metric,
      ...>     source: :my_metric_source,
      ...>     name: :requests,
      ...>     measurements: %{count: 1}
      ...>   })
      ...> )
      :ok

  ### Process-backed source

  The producer is a long-lived GenServer managed by the
  `Flezha.Observability.SourceSupervisor`. Used for polled sources
  (`handle_poll/2`) or for sources that need their own state machine.

      defmodule MyPolledSource do
        @behaviour Flezha.Observability.Source

        @impl true
        def init(_opts), do: {:ok, %{count: 0}}

        @impl true
        def handle_poll(_now, state) do
          # Build and emit signals; return `{:ok, new_state}`.
          {:ok, %{state | count: state.count + 1}}
        end

        @impl true
        def config_schema, do: %{poll_interval_ms: :pos_integer}
      end

  At minimum every implementation MUST provide `init/1` and
  `config_schema/0`. `handle_poll/2` and `handle_signal/2` are
  optional — a polled source defines the former, an emit-driven
  process-backed source defines the latter.
  """

  alias Flezha.Observability.Signal

  @type config :: map()

  @callback init(config()) :: {:ok, state :: term()} | {:error, term()}

  @callback handle_poll(now_microseconds :: integer(), state :: term()) ::
              {:ok, new_state :: term()} | {:error, term()}

  @callback handle_signal(Signal.t(), state :: term()) ::
              {:ok, new_state :: term()} | {:error, term()}

  @callback config_schema() :: map()

  @optional_callbacks [handle_poll: 2, handle_signal: 2]
end
