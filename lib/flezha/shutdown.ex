defmodule Flezha.Shutdown do
  @moduledoc """
  Teardown at every scope as one walk: `run/5` folds the `Flezha.Shutdown.Step`
  implementations of a scope in their declared order, each once, each bounded,
  stopping at the budget and naming the steps that did not run. The user's
  relay stop, the Overwatch's own terminate, the namespace stop and the
  node drain are its teardown callers; a relay start walks the `:start` steps
  that take what it holds.

  It also answers `quiesced?/2` — whether a scope is being torn down — from
  the one table the coordinator and the namespace walk both write, so a
  one-namespace stop refuses that namespace's connects and starts without
  reading as the whole node shutting down.

  This module names no other module of the tree: the declaring modules are
  found at runtime, so the walk sits outside the dependency cycle every
  resource it tears down is in.
  """

  require Logger

  alias Flezha.Config.Durations

  @table Flezha.Shutdown.State
  @steps_key {__MODULE__, :steps}

  # The one budget a relay stop has, and the source the process teardown
  # derives its own from.

  @type scope :: Flezha.Shutdown.Step.scope()
  @type report :: %{ran: [atom()], skipped: [atom()], errors: [{atom(), term()}], results: map()}

  @doc "The budget of a relay stop."
  @spec stop_timeout_ms() :: pos_integer()
  def stop_timeout_ms(), do: Durations.get(:stop_release_timeout_ms)

  @doc "The budget of a process teardown: the relay stop's, less the slack the stop keeps."
  @spec process_budget_ms() :: pos_integer()
  def process_budget_ms(), do: Durations.get(:stop_process_budget_ms)

  @doc """
  Walk `scope`'s steps against `target` under `budget_ms`. A step that answers
  `{:error, _}`, raises or exits halts the walk — a later step must never act
  on a resource an earlier one failed to release — and the report names every
  step that did not run, including the ones the budget left no room for.

  Every step runs **in the caller's process**, because a teardown releases what
  the caller owns: the links its plugin table holds, the ETS tables its marks
  live in, the routes that name `self()`. Each is handed the budget still
  remaining and is responsible for returning inside it — every declared step is
  bounded by a call timeout or by that budget — and the walk re-checks the
  deadline between steps.

  `steps` defaults to the scope's declared steps; passing a list folds exactly
  those, which is how the fold itself is driven apart from what a scope
  happens to declare.

  `opts[:on_error]` is `:halt` (the default) or `:continue`, which records a
  failed step and runs the next while the budget lasts.
  """
  @spec run(scope(), term(), non_neg_integer(), [Flezha.Shutdown.Step.t()] | nil, keyword()) ::
          {:ok, report()} | {:partial, report()}
  def run(scope, target, budget_ms, steps \\ nil, opts \\ []) when is_integer(budget_ms) do
    deadline = now_ms() + budget_ms
    halt_on_error? = Keyword.get(opts, :on_error, :halt) == :halt
    quiesce(scope, target)

    report =
      Enum.reduce(
        steps || steps(scope),
        %{ran: [], skipped: [], errors: [], results: %{}},
        fn step, acc ->
          remaining = deadline - now_ms()

          if (halt_on_error? and acc.errors != []) or acc.skipped != [] or remaining <= 0,
            do: skip(acc, step),
            else: apply_step(acc, step, target, remaining)
        end
      )
      |> finish(scope)

    release(scope, target)

    if report.errors == [] and report.skipped == [], do: {:ok, report}, else: {:partial, report}
  end

  defp apply_step(acc, step, target, remaining) do
    case step.run.(target, remaining) do
      :ok ->
        %{acc | ran: [step.name | acc.ran]}

      {:ok, result} ->
        %{acc | ran: [step.name | acc.ran], results: Map.put(acc.results, step.name, result)}

      {:error, reason} ->
        failed(acc, step, reason)
    end
  catch
    kind, reason -> failed(acc, step, {kind, reason})
  end

  defp failed(acc, step, reason) do
    if step.scope != :start do
      Logger.warning("[shutdown] step #{step.scope}/#{step.name} failed: #{inspect(reason)}")
    end

    %{acc | errors: [{step.name, reason} | acc.errors]}
  end

  defp skip(acc, step), do: %{acc | skipped: [step.name | acc.skipped]}

  # A `:start` walk's caller answers what did not run.
  defp finish(acc, scope) do
    if acc.skipped != [] and scope != :start do
      Logger.warning("[shutdown] steps not run: #{inspect(Enum.reverse(acc.skipped))}")
    end

    %{
      acc
      | ran: Enum.reverse(acc.ran),
        skipped: Enum.reverse(acc.skipped),
        errors: Enum.reverse(acc.errors)
    }
  end

  @doc """
  The steps of `scope`, in order. Every module of the application that
  declares `shutdown_steps/0` is read once; a duplicate `{scope, order}` is a
  declaration error and raises by name.
  """
  @spec steps(scope()) :: [Flezha.Shutdown.Step.t()]
  def steps(scope) do
    Enum.filter(all_steps(), &(&1.scope == scope))
  end

  @doc "Every declared step, ordered by scope and order, with the module that declared it."
  @spec all_steps() :: [map()]
  def all_steps() do
    case :persistent_term.get(@steps_key, nil) do
      nil ->
        steps = discover()
        :persistent_term.put(@steps_key, steps)
        steps

      steps ->
        steps
    end
  end

  defp discover() do
    {:ok, modules} = :application.get_key(:flezha, :modules)

    steps =
      modules
      |> Enum.filter(&(Code.ensure_loaded?(&1) and function_exported?(&1, :shutdown_steps, 0)))
      |> Enum.flat_map(fn mod -> Enum.map(mod.shutdown_steps(), &Map.put(&1, :module, mod)) end)
      |> Enum.sort_by(&{&1.scope, &1.order})

    steps
    |> Enum.group_by(&{&1.scope, &1.order})
    |> Enum.each(fn
      {_key, [_one]} ->
        :ok

      {{scope, order}, many} ->
        raise ArgumentError,
              "shutdown steps #{inspect(Enum.map(many, & &1.name))} share order #{order} in scope #{scope}"
    end)

    steps
  end

  @doc """
  The one admission answer every entry point behind a teardown gives: `:ok`, or
  `{:error, :shutting_down}` once `scope`'s `target` is quiescing. A runner
  connect and a relay start ask it in the same words, so a third caller
  cannot invent a fourth spelling of the same refusal.
  """
  @spec admit(scope(), term()) :: :ok | {:error, :shutting_down}
  def admit(scope, target) do
    if quiesced?(scope, target), do: {:error, :shutting_down}, else: :ok
  end

  @doc """
  Whether `scope`'s `target` is being torn down: the node from the
  coordinator's quiesce until it stops, a namespace while its walk runs (or
  the node is quiesced).
  """
  @spec quiesced?(scope(), term()) :: boolean()
  def quiesced?(:node, _target), do: lookup({:node, :phase}) not in [nil, :idle]

  def quiesced?(:namespace, namespace) do
    quiesced?(:node, nil) or lookup({:namespace, to_string(namespace)}) == true
  end

  def quiesced?(_scope, _target), do: false

  @doc "The coordinator's phase, written by the coordinator as it transitions."
  @spec node_phase(atom()) :: :ok
  def node_phase(phase) when is_atom(phase), do: insert({:node, :phase}, phase)

  @doc "Create the quiesce table unless it exists; the application's start owns it."
  @spec create_table() :: :ok
  def create_table() do
    if :ets.whereis(@table) == :undefined do
      _ = :ets.new(@table, [:set, :public, :named_table, read_concurrency: true])
      insert({:node, :phase}, :idle)
    end

    :ok
  end

  defp quiesce(:namespace, %{namespace: namespace}),
    do: insert({:namespace, to_string(namespace)}, true)

  defp quiesce(_scope, _target), do: :ok

  defp release(:namespace, %{namespace: namespace}) do
    case :ets.whereis(@table) do
      :undefined -> :ok
      tid -> :ets.delete(tid, {:namespace, to_string(namespace)}) && :ok
    end
  end

  defp release(_scope, _target), do: :ok

  defp lookup(key) do
    case :ets.whereis(@table) do
      :undefined -> nil
      tid -> :ets.lookup_element(tid, key, 2, nil)
    end
  end

  defp insert(key, value) do
    case :ets.whereis(@table) do
      :undefined -> :ok
      tid -> :ets.insert(tid, {key, value}) && :ok
    end
  end

  defp now_ms(), do: System.monotonic_time(:millisecond)
end
