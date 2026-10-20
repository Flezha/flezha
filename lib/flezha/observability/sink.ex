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

  A sink that lives under its own supervisor, outside `SinkSupervisor`,
  declares `use Flezha.Observability.Sink, name: :prometheus` with any of
  `overflow:`, `watermark:` (the mailbox high watermark) and
  `block_timeout_ms:`, each evaluated when it registers. It is given
  `@sink_name`, `sink_name/0`, `register_self/0`, which registers the calling
  process under that name with its `filter/0`, and `unregister/0`; an omitted
  option takes the registry's default.
  """

  alias Flezha.Observability.Signal

  @row_keys [
    overflow: :overflow,
    watermark: :mailbox_high_watermark,
    block_timeout_ms: :block_timeout_ms
  ]

  defmacro __using__(opts) do
    name = Keyword.fetch!(opts, :name)
    row = for {key, row_key} <- @row_keys, Keyword.has_key?(opts, key), do: {row_key, opts[key]}

    quote do
      @behaviour Flezha.Observability.Sink
      @sink_name unquote(name)

      @doc "The name the sink is registered under."
      @spec sink_name() :: atom()
      def sink_name(), do: @sink_name

      @doc false
      @spec register_self() :: :ok
      def register_self() do
        Flezha.Observability.Registry.register_external_sink(
          Map.merge(
            %{name: @sink_name, module: __MODULE__, pid: self(), filter: filter()},
            Map.new(unquote(row))
          )
        )
      end

      @doc false
      @spec unregister() :: :ok
      def unregister(), do: Flezha.Observability.Registry.unregister_sink(@sink_name)
    end
  end

  @type config :: map()
  @type filter :: %{
          required(:kinds) => :any | [Signal.kind()],
          required(:sources) => :any | [atom()]
        }

  @callback config_schema() :: map()

  @callback filter() :: filter()
end
