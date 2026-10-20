defmodule Flezha.Plugin.Provider.Worker do
  @moduledoc """
  What every provider worker that answers a health probe shares:
  `use Flezha.Plugin.Provider.Worker, facts: [...]` makes the module a
  `GenServer` with

    * `start_link/1`, registered under the spec's `:name`;
    * `args/1`, the start arguments from the plugin's prepared options (the
      options themselves unless the worker writes its own);
    * `probe/1` and its answer: the state's `facts`, taken as they are, for the
      provider's `health/1` to judge;
    * with `host: true` (the default), a stop when the relay the worker feeds
      goes down, watched as `:host` in the state's `watch`.

  `fields/1` is the state the kit reads: the host watch, the failure run
  (`consecutive_failures`, `last_error`), the time of the last settled read
  and the timers. `failed/2`, `recovered/1` and `settled/1` move them.
  A `{:DOWN, ...}` that is not the host's, and every other message, reach the
  worker's own `handle_info/2`.
  """

  alias Flezha.Process.Watch

  defmacro __using__(opts) do
    facts = Keyword.fetch!(opts, :facts)
    host? = Keyword.get(opts, :host, true)

    quote do
      use GenServer

      @__worker_facts__ unquote(facts)
      @__worker_host__ unquote(host?)
      @before_compile unquote(__MODULE__)

      @doc "Started by the extension table at activation, from the declared spec."
      @spec start_link(map()) :: GenServer.on_start()
      def start_link(opts) when is_map(opts),
        do: GenServer.start_link(__MODULE__, opts, name: Map.fetch!(opts, :name))

      @doc "The worker's start arguments, from the plugin's prepared options."
      @spec args(map()) :: map()
      def args(opts), do: opts

      @doc "The facts the provider's `health/1` judges; `:timeout` when the worker does not answer."
      @spec probe(pid()) :: {:ok, map()} | :timeout
      def probe(pid), do: Flezha.Plugin.ProviderBehaviour.ask(pid, :health_probe)

      defoverridable args: 1
    end
  end

  defmacro __before_compile__(env) do
    host =
      if Module.get_attribute(env.module, :__worker_host__) do
        quote do
          # The relay this worker feeds is gone.
          def handle_info({:DOWN, ref, :process, _pid, _reason} = message, state) do
            case unquote(Watch).down(state.watch, ref) do
              {[:host], _watch} -> {:stop, :normal, state}
              _other -> super(message, state)
            end
          end
        end
      end

    quote do
      defoverridable handle_info: 2

      @impl GenServer
      def handle_info({:health_probe, from, ref}, state) do
        send(from, {ref, Map.take(state, @__worker_facts__)})
        {:noreply, state}
      end

      unquote(host)

      def handle_info(message, state), do: super(message, state)
    end
  end

  @doc """
  The state fields the kit reads, for a worker's `init/1`: the host the spec
  names watched as `:host`, no failures, settled now (so a worker that has
  never settled is measured from its own start) and no timers.
  """
  @spec fields(map()) :: map()
  def fields(opts) do
    %{
      watch: Watch.watch(Watch.new(), Map.fetch!(opts, :overwatcher_pid), :host),
      consecutive_failures: 0,
      last_error: nil,
      last_settled_at: now(),
      timers: %{}
    }
  end

  @doc "`state` after one more failure in a row, `error` the last."
  @spec failed(map(), term()) :: map()
  def failed(state, error),
    do: %{state | consecutive_failures: state.consecutive_failures + 1, last_error: error}

  @doc "`state` with its failure run cleared."
  @spec recovered(map()) :: map()
  def recovered(state), do: %{state | consecutive_failures: 0, last_error: nil}

  @doc "`state` settled now: the idle window counts from here."
  @spec settled(map()) :: map()
  def settled(state), do: %{state | last_settled_at: now()}

  defp now, do: :erlang.monotonic_time(:millisecond)
end
