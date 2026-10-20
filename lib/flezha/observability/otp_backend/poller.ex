defmodule Flezha.Observability.OtpBackend.Poller do
  @moduledoc """
  Shared GenServer plumbing for the periodic observability sources
  (`:beam_vm`, `:host`, `:processhub`, `:flezha_transport`).

  `use Flezha.Observability.OtpBackend.Poller, source: :name,
  cardinality_budget: N, default_poll_ms: N, area: :machine, title: "…",
  description: "…", signals: [name: {:metric, "what it says"}]` (or any form
  `Flezha.Observability.Source.signals/1` takes) declares the source through
  `use Flezha.Observability.Source` (process-backed, so `__source__/0` and
  `emit/3`) and injects the whole lifecycle: `start_link/1`, `init/1`
  (registers `__source__/0` and arms the first tick), `handle_info(:poll, …)`
  with its catch-all and `poll_interval_ms/0` (per-source override under
  `:flezha, :observability, :otp_backend, :poll_intervals`, else the default).

  A source supplies only what varies: `init_state/1` for state beyond
  `:interval`, `poll/1`, which does the work and returns the new state,
  and `config_schema/0`. All three are overridable.
  """

  @doc "State the source needs beyond `:interval`, built at `init/1`."
  @callback init_state(opts :: term()) :: map()

  @doc "One poll. Emits, and returns the state the next tick sees."
  @callback poll(state :: map()) :: map()

  defmacro __using__(opts) do
    {source, opts} = Keyword.pop!(opts, :source)
    {default_poll_ms, opts} = Keyword.pop!(opts, :default_poll_ms)

    quote do
      use GenServer

      @behaviour Flezha.Observability.OtpBackend.Poller
      @behaviour Flezha.Observability.Source

      # The tick is the foundation's, so its key is declared here rather than
      # by every source that uses this template.
      use Flezha.Process.Timer, keys: [:poll]

      use Flezha.Observability.Source,
          unquote([name: source, flavour: :process_backed] ++ opts)

      @source unquote(source)
      @default_poll_ms unquote(default_poll_ms)

      unquote(driven_half())
      unquote(source_half())
    end
  end

  # What the foundation drives: start, register, arm the first tick, and keep
  # re-arming it. A source never writes any of this.
  defp driven_half() do
    quote do
      def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

      @impl GenServer
      def init(opts) do
        Flezha.Observability.Registry.register_source(__source__())

        interval = poll_interval_ms()
        {:ok, opts |> init_state() |> Map.put(:interval, interval) |> arm_poll()}
      end

      @impl GenServer
      def handle_info(:poll, state) do
        {:noreply, state |> poll() |> arm_poll()}
      end

      defp arm_poll(state), do: arm_timer(state, :poll, state.interval, :poll)

      def handle_info(_msg, state), do: {:noreply, state}
    end
  end

  # What a source may supply, with the answer for one that supplies none.
  defp source_half() do
    quote do
      @impl Flezha.Observability.OtpBackend.Poller
      def init_state(_opts), do: %{}

      @impl Flezha.Observability.OtpBackend.Poller
      def poll(state), do: state

      @impl Flezha.Observability.Source
      def config_schema(), do: %{}

      defp poll_interval_ms() do
        Flezha.Config.get(:observability_poll_intervals)
        |> Map.get(@source, @default_poll_ms)
      end

      defoverridable init: 1, handle_info: 2, init_state: 1, poll: 1, config_schema: 0
    end
  end
end
