defmodule Flezha.Convergence.Probe.Judge do
  @moduledoc """
  The one place a health fact becomes a health state.

  Every detector of `Flezha.Convergence.Probe.Table` reads; this folds what
  they read, per subject, in one fixed rule order, into one
  `%Flezha.Convergence.Verdict{}`. `Flezha.Convergence.Probe.judge/2` is the
  public name.

  The rule order for a relay is the health map's clause order and the
  classifier's deepest-wins, made one list: an unobserved reader wins, then
  suspension, then no binding, then a binding the roster does not hold, then a
  park, then the deepest of a stall, a stuck marker or a plugin that is not
  running, otherwise healthy. A plugin the operator disabled is a pause, not a
  plugin that is not running.

  A reader that raises does not stop the tick: every subject of the kind it
  speaks about is `:unknown`, `:unknown` escalates nothing, and the tick
  publishes.
  """

  alias Flezha.Convergence.Probe.{Fact, Table}
  alias Flezha.Convergence.StallClassifier
  alias Flezha.Convergence.Verdict

  require Logger

  @doc """
  Judge the tick. `context` is what the readers read (see
  `Flezha.Convergence.Probe.Readers`); `previous` is the last tick's record map,
  which is how `since_ms` survives a state that holds.
  """
  @spec judge(Flezha.Convergence.Probe.Readers.context(), %{term() => Verdict.t()}) ::
          %{term() => Verdict.t()}
  def judge(context, previous \\ %{}) do
    {observations, unreadable} = observe(context)
    now = Map.get(context, :now) || System.monotonic_time(:millisecond)

    observations
    |> roll_up_plugins()
    |> Enum.map(fn {subject, facts} ->
      {subject, verdict(subject, facts, unreadable, now, Map.get(previous, subject))}
    end)
    |> Map.new()
  end

  # Run every reader that has one. A reader that raises or throws names its
  # kind in `unreadable`, and every subject of that kind is judged `:unknown`
  # rather than judged wrong.
  defp observe(context) do
    Enum.reduce(Table.detectors(), {%{}, %{}}, fn entry, {acc, bad} ->
      case run(entry, context) do
        {:ok, facts} -> {merge_facts(acc, entry, facts), bad}
        {:error, why} -> {acc, mark_unreadable(bad, entry, why)}
      end
    end)
  end

  # A `:by_scope` reader speaks about drivers and channels ONLY: runner and
  # relay subjects come from readers that answered, and blanking those on
  # this reader's failure would hide facts that were read.
  defp mark_unreadable(bad, %{kind: :by_scope, id: id}, why) do
    Enum.reduce([:driver, :channel], bad, &Map.put(&2, &1, {id, why}))
  end

  defp mark_unreadable(bad, %{kind: kind, id: id}, why), do: Map.put(bad, kind, {id, why})

  defp run(%{facts: nil}, _context), do: {:ok, []}

  defp run(%{facts: reader}, context) do
    {:ok, reader.(context)}
  rescue
    e ->
      Logger.warning("[convergence] fact reader raised: #{Exception.message(e)}")
      {:error, {:raised, Exception.message(e)}}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp merge_facts(acc, _entry, facts) do
    Enum.reduce(facts, acc, fn %Fact{subject: subject, observed: observed}, inner ->
      Map.update(inner, subject, observed, &Map.merge(&1, observed))
    end)
  end

  # A declared plugin that is not running, crashed, or self-reported unhealthy,
  # and was not disabled, is its relay's divergence too — the plugin keeps its
  # own record and the relay carries the deepest one as a fact, so both
  # surfaces agree without either re-reading the other.
  defp roll_up_plugins(observations) do
    issues =
      for {{:plugin, {ns, rid, plugin_id}}, facts} <- observations,
          why = plugin_issue(facts),
          reduce: %{} do
        acc -> Map.put_new(acc, {:relay, {ns, rid}}, {:plugin_unhealthy, plugin_id, why})
      end

    Enum.reduce(issues, observations, fn {subject, issue}, acc ->
      Map.update(acc, subject, %{plugin_issue: issue}, &Map.put(&1, :plugin_issue, issue))
    end)
  end

  # A binding recorded not started is probed for its reason while it is not
  # running, so the reason is read before the phase.
  defp plugin_issue(facts) do
    cond do
      not is_nil(Map.get(facts, :down)) -> {:crashed, Map.get(facts, :down)}
      disabled?(facts) -> nil
      match?({:unhealthy, _}, Map.get(facts, :probe)) -> Map.get(facts, :probe)
      not Map.get(facts, :running?, false) -> :not_running
      true -> nil
    end
  end

  # ---------- the rules ----------

  defp verdict({kind, _id} = subject, facts, unreadable, now, previous) do
    {state, reason} = decide(kind, facts, Map.get(unreadable, kind))

    Verdict.new(subject, state,
      reason: reason,
      phase: phase(kind, facts),
      now: now,
      previous: previous,
      threshold: threshold_for(reason),
      facts: facts
    )
  end

  # The window the verdict was judged by, named — read from the reason, so the
  # threshold on the record is the one that actually decided it rather than
  # whichever reader ran last.
  defp threshold_for({:wedged, layer}), do: named(wedge_threshold(layer))
  defp threshold_for({:stalled, _}), do: named(:output_stall_timeout_ms)
  defp threshold_for({:stuck, _}), do: named(:convergence_output_stall_grace_ms)
  defp threshold_for({:fail_open, _}), do: named(:probe_budget_ms)
  defp threshold_for(:dead_runner), do: named(:control_liveness_window_ms)
  defp threshold_for(_reason), do: nil

  defp named(name), do: {name, Table.threshold(name)}

  defp wedge_threshold(layer) do
    case StallClassifier.layer_scope(layer) do
      :fabric -> :driver_freeze_ms
      :relay -> :cell_stall_ms
      _ -> :stall_ms
    end
  end

  # An uncertain probe is `unknown` for the user and escalates nothing.
  defp decide(_kind, _facts, {id, why}), do: {:unknown, {:unobserved, {id, why}}}

  defp decide(:relay, facts, nil) do
    cond do
      not Map.get(facts, :observed?, true) or Map.get(facts, :bound_runner) == :unknown ->
        {:unknown, {:unobserved, :relay_state}}

      Map.get(facts, :suspended?, false) ->
        {:suspended, :evicted_at_boundary}

      is_nil(Map.get(facts, :bound_runner)) ->
        {:unbound, :no_binding}

      Map.get(facts, :live_runners) == :unknown ->
        {:unknown, {:unobserved, :active_runners}}

      not live?(facts) ->
        {:failed, :dead_runner}

      Map.get(facts, :halted?, false) ->
        {:halted, {:halted, Map.get(facts, :halt_reason)}}

      true ->
        degraded_or_healthy(facts)
    end
  end

  defp decide(:plugin, facts, nil) do
    cond do
      not Map.get(facts, :subscribed?, false) -> {:degraded, :not_running}
      not is_nil(Map.get(facts, :down)) -> {:degraded, {:crashed, Map.get(facts, :down)}}
      disabled?(facts) -> {:healthy, :disabled}
      true -> plugin_probe(Map.get(facts, :probe), Map.get(facts, :running?, false))
    end
  end

  # A roster the population reader did not supply is absent, not "gone": a
  # channel the classifier named and the sample never listed is judged on the
  # wedge alone, because nothing observed its runner's membership.
  defp decide(kind, facts, nil) when kind in [:runner, :channel] do
    roster = Map.get(facts, :roster)

    cond do
      roster == :unknown -> {:unknown, {:unobserved, :active_runners}}
      not is_nil(roster) and roster != :active -> absent_runner(facts)
      layer = Map.get(facts, :wedged_layer) -> {:degraded, {:wedged, layer}}
      true -> {:healthy, :connected}
    end
  end

  defp decide(:driver, facts, nil) do
    case Map.get(facts, :wedged_layer) do
      nil -> {:healthy, :connected}
      layer -> {:degraded, {:wedged, layer}}
    end
  end

  # A runner no roster the tick read holds is dead unless its own control
  # session is still speaking. The rosters cover the running namespaces only,
  # so a runner whose namespace hub is not running is in none of them while its
  # session is open. Silence past `control_liveness_window_ms` is what the
  # connection's own watchdog already treats as a close, and it is the window
  # this verdict already cites: one window, one answer.
  defp absent_runner(facts) do
    window = Table.threshold(:control_liveness_window_ms)
    silent = Map.get(facts, :control_silent_ms)

    if is_integer(silent) and is_integer(window) and silent < window,
      do: {:unknown, {:unobserved, :active_runners}},
      else: {:failed, :dead_runner}
  end

  # A binding whose plugin the operator disabled is a pause, not a fault: it
  # stays recorded not started for that reason until a plugin of its name is
  # enabled again. A plugin missing or failing for any other reason is not.
  defp disabled?(facts), do: match?({:unhealthy, {:plugin_not_found, _}}, Map.get(facts, :probe))

  # A plugin the loop has not probed at all is `unknown`; a prepared one is
  # healthy and its PHASE says it is not running; an uncertain probe is healthy
  # and names why.
  defp plugin_probe(:unprobed, _running?), do: {:unknown, {:unobserved, :plugin_probe}}

  defp plugin_probe({:unhealthy, reason}, _running?),
    do: {:degraded, {:plugin_unhealthy, reason}}

  defp plugin_probe(_probe, false), do: {:healthy, :prepared}
  defp plugin_probe(:healthy, _running?), do: {:healthy, :running}
  defp plugin_probe({:fail_open, reason}, _running?), do: {:healthy, {:fail_open, reason}}

  defp plugin_probe(_other, _running?), do: {:unknown, {:unobserved, :plugin_probe}}

  # Deepest reason first: a data-plane wedge, then a stall, then a stuck
  # marker, then a declared plugin that is not running. A flat marker with no
  # input-side advance is `:idle`, and `:idle` is healthy.
  defp degraded_or_healthy(facts) do
    cond do
      layer = Map.get(facts, :wedged_layer) -> {:degraded, {:wedged, layer}}
      Map.get(facts, :output_stalled?, false) -> {:degraded, {:stalled, :output_stall}}
      stuck = Map.get(facts, :stuck) -> {:degraded, {:stuck, Map.get(stuck, :detail)}}
      issue = Map.get(facts, :plugin_issue) -> {:degraded, issue}
      true -> {:healthy, :producing}
    end
  end

  defp live?(facts) do
    case {Map.get(facts, :live_runners), Map.get(facts, :bound_runner)} do
      {%MapSet{} = live, runner} -> MapSet.member?(live, runner)
      _ -> false
    end
  end

  defp phase(:relay, facts), do: Verdict.phase_of(Map.put_new(facts, :observed?, false))

  defp phase(:plugin, facts) do
    cond do
      Map.get(facts, :running?, false) -> :running
      Map.get(facts, :subscribed?, false) -> :prepared
      true -> :inactive
    end
  end

  defp phase(_kind, _facts), do: nil
end
