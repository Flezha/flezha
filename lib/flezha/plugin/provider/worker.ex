defmodule Flezha.Plugin.Provider.Worker do
  @moduledoc """
  What every provider worker shares: `use Flezha.Plugin.Provider.Worker`
  makes the module a `GenServer` with

    * `start_link/1`, registered under the spec's `:name`;
    * `args/1`, the start arguments from the plugin's prepared options (the
      options themselves unless the worker writes its own);
    * with `facts:`, `probe/1` and its answer: the state's `facts`, taken as
      they are, and `probe_verdict/2`, the provider's verdict over them, for its
      `health/1`;
    * with `host: true` (the default), a stop when the relay the worker feeds
      goes down, watched as `:host` in the state's `watch` (`host/1`).

  `fields/1` is the state the kit reads: the host watch, the failure run
  (`consecutive_failures`, `last_error`), the time of the last settled read
  and the timers. `failed/3`, `recovered/2` and `settled/1` move them.
  A `{:DOWN, ...}` that is not the host's, and every other message, reach the
  worker's own `handle_info/2`.
  """

  alias Flezha.Process.Watch

  require Logger

  defmacro __using__(opts) do
    facts = Keyword.get(opts, :facts)
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

      defoverridable args: 1

      unquote(if facts, do: probe())
    end
  end

  defp probe do
    quote do
      @doc "The facts the provider's `health/1` judges; `:timeout` when the worker does not answer."
      @spec probe(pid()) :: {:ok, map()} | :timeout
      def probe(pid), do: Flezha.Plugin.ProviderBehaviour.ask(pid, :health_probe)

      @doc "`verdict` over the worker's facts; `{:fail_open, :probe_timeout}` when it does not answer."
      @spec probe_verdict(pid(), (map() -> verdict)) :: verdict | {:fail_open, :probe_timeout}
            when verdict: Flezha.Plugin.ProviderBehaviour.probe_result()
      def probe_verdict(pid, verdict) do
        case probe(pid) do
          {:ok, facts} -> verdict.(facts)
          :timeout -> {:fail_open, :probe_timeout}
        end
      end
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

    answer =
      if Module.get_attribute(env.module, :__worker_facts__) do
        quote do
          def handle_info({:health_probe, from, ref}, state) do
            send(from, {ref, Map.take(state, @__worker_facts__)})
            {:noreply, state}
          end
        end
      end

    quote do
      defoverridable handle_info: 2

      @impl GenServer
      unquote(answer)
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
      watch: host(opts),
      consecutive_failures: 0,
      last_error: nil,
      last_settled_at: now(),
      timers: %{}
    }
  end

  @doc "The watch over the host the spec names, as `:host`."
  @spec host(map()) :: Watch.t()
  def host(opts), do: Watch.watch(Watch.new(), Map.fetch!(opts, :overwatcher_pid), :host)

  @doc """
  `state` after one more failure in a row, `error` the last.

  The run is logged under `subject`, the read that failed, when it starts, when
  its error changes and again at every tenfold count, so a source that stays
  down the same way writes a line per order of magnitude, not one per attempt.
  """
  @spec failed(map(), term(), String.t()) :: map()
  def failed(state, error, subject) do
    changed? = state.consecutive_failures > 0 and error != state.last_error
    state = %{state | consecutive_failures: state.consecutive_failures + 1, last_error: error}
    failures = state.consecutive_failures

    if changed? or power_of_ten?(failures) do
      Logger.warning("#{subject} failed consecutive=#{failures} reason=#{reason_text(error)}")
    end

    state
  end

  @doc """
  `state` with its failure run cleared. A run that was under way is logged as
  ended under `subject`, with its length.
  """
  @spec recovered(map(), String.t()) :: map()
  def recovered(state, subject) do
    failures = state.consecutive_failures

    if failures > 0 do
      Logger.info("#{subject} recovered after #{failures} consecutive failures")
    end

    %{state | consecutive_failures: 0, last_error: nil}
  end

  defp power_of_ten?(n), do: n == Integer.pow(10, length(Integer.digits(n)) - 1)

  defp reason_text(reason) when is_binary(reason), do: reason
  defp reason_text(reason), do: inspect(reason)

  @doc "`state` settled now: the idle window counts from here."
  @spec settled(map()) :: map()
  def settled(state), do: %{state | last_settled_at: now()}

  defp now, do: :erlang.monotonic_time(:millisecond)
end
