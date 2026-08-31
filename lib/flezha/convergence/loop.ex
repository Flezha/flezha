defmodule Flezha.Convergence.Loop do
  @moduledoc """
  The single convergence GenServer — the four-seam declarative reconciler.
  Every tick (and on declared events) it reads `Intent`, probes `Probe`
  reality, computes a `Diff`, the `Planner` turns the diff into
  backoff-filtered primitive ops, and `Heal` executes them. The resulting
  reality + health summary is published to `Flezha.Convergence.Snapshot` so
  `/healthz`, the CLI, and telemetry all read one source of truth.

  Reality-matches-intent is the *event*, not the default: the loop heals
  every divergence it finds, not only ones triggered by a specific
  signal. Declared events (`:output_stall`, `:runner_connected`) just
  shorten detection latency by triggering an immediate tick.
  """

  use GenServer

  alias Flezha.Convergence.{Backoff, Diff, Heal, Intent, Planner, Probe, RecoveryGate}

  alias Flezha.Convergence.Loop.{Liveness, Progress, State, Summary}
  alias Flezha.Service.{EventManager, PluginManager}

  require Logger

  # A operative must be misbound for this many consecutive ticks before it is
  # migrated, so a brief runner blip cannot trigger a full-namespace
  # migration + WAL replay.
  @misbound_min_ticks 2

  @telemetry_subs [
    [:flezha, :state, :output_stall],
    [:flezha, :boot, :reconcile_round]
  ]
  @handler_id {__MODULE__, :events}

  # ---------- public API ----------

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Run a single tick synchronously; for tests + debugging."
  @spec tick_now() :: :ok
  def tick_now(), do: GenServer.call(__MODULE__, :tick_now, 30_000)

  # ---------- GenServer ----------

  @impl true
  def init(opts) do
    tick_ms =
      Keyword.get(
        opts,
        :tick_ms,
        Application.get_env(:flezha, :convergence_tick_ms, State.default_tick_ms())
      )

    grace_ms =
      Keyword.get(
        opts,
        :output_stall_grace_ms,
        Application.get_env(
          :flezha,
          :convergence_output_stall_grace_ms,
          State.default_output_stall_grace_ms()
        )
      )

    # Detach first: `terminate/2` only runs when the Loop is trapping exits, so
    # a supervisor restart otherwise leaves the previous incarnation's handler
    # registered under this id. `attach_many` would then refuse as
    # `:already_exists` and the new Loop would never hear another sensor event
    # — a silently deaf actor, which is the failure class this loop exists to
    # close.
    _ = :telemetry.detach(@handler_id)

    _ =
      :telemetry.attach_many(@handler_id, @telemetry_subs, &__MODULE__.handle_telemetry/4, self())

    # Shorten runner-takeover detection from `tick_ms` to ~ms.
    _ = EventManager.add_handler(:runner_connected_event)

    # First tick immediately so the recovery gate + first publish happen
    # without waiting `tick_ms`.
    Process.send_after(self(), :tick, 0)

    {:ok,
     %State{
       tick_ms: tick_ms,
       output_stall_grace_ms: grace_ms
     }}
  end

  @impl true
  def handle_info(:tick, %State{} = state) do
    state = run_tick(state)
    Process.send_after(self(), :tick, state.tick_ms)
    {:noreply, state}
  end

  def handle_info({:runner_connected_event, {_namespace, _runner_id}}, %State{} = state) do
    {:noreply, run_tick(state)}
  end

  # An already-recorded stall only refreshes its timestamp: the Watchdog
  # re-flags a frozen Entity every sweep, and the ladder for that operative is
  # already walking on the periodic tick. Ticking per re-flag instead would make
  # a handful of stalled Entities drive the whole loop at sweep cadence.
  def handle_info({:output_stall, operative_id}, %State{} = state)
      when is_binary(operative_id) do
    known? = Map.has_key?(state.stalled_operatives, operative_id)
    now = System.monotonic_time(:millisecond)
    state = %{state | stalled_operatives: Map.put(state.stalled_operatives, operative_id, now)}

    if known?, do: {:noreply, state}, else: {:noreply, run_tick(state)}
  end

  # A reconcile round that started orphans: converge against them now rather
  # than at the next tick, so the plan adopts what the round just started
  # instead of a later tick finding them half-bound.
  def handle_info(:reconcile_started, %State{} = state) do
    {:noreply, run_tick(state)}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def handle_call(:tick_now, _from, %State{} = state) do
    {:reply, :ok, run_tick(state)}
  end

  @impl true
  def terminate(_reason, _state) do
    _ = :telemetry.detach(@handler_id)
    :ok
  end

  # ---------- telemetry callback ----------

  @doc false
  def handle_telemetry([:flezha, :state, :output_stall], _measurements, metadata, loop_pid) do
    operative_id = Map.get(metadata, :operative_instance)

    if is_binary(operative_id) and is_pid(loop_pid),
      do: send(loop_pid, {:output_stall, operative_id})

    :ok
  catch
    _, _ -> :ok
  end

  # The hook fires at round COMPLETION, so this is an adopt-and-converge
  # trigger, not a hold. A quiet round (`started = 0`) changed nothing and
  # costs nothing.
  def handle_telemetry([:flezha, :boot, :reconcile_round], measurements, _metadata, loop_pid) do
    if Map.get(measurements, :started, 0) > 0 and is_pid(loop_pid),
      do: send(loop_pid, :reconcile_started)

    :ok
  catch
    _, _ -> :ok
  end

  def handle_telemetry(_event, _measurements, _metadata, _config), do: :ok

  # ---------- tick body ----------

  # A raise/throw inside the tick must NOT crash the Loop: it runs inline in
  # the single self-healing GenServer under default restart intensity, so an
  # uncaught error would cascade to the controller's top supervisor. Skip the
  # tick and carry the prior state instead.
  defp run_tick(%State{} = state) do
    do_run_tick(state)
  rescue
    e ->
      Logger.error("[convergence] tick crashed, skipping: #{Exception.message(e)}")
      tick_failed_telemetry()
      state
  catch
    kind, reason ->
      Logger.error("[convergence] tick aborted (#{kind}): #{inspect(reason)}")
      tick_failed_telemetry()
      state
  end

  defp do_run_tick(%State{} = state) do
    started_at = System.monotonic_time(:millisecond)
    {state, intent, reality, issued, elapsed} = heal_pass(state, started_at)
    state = report_pass(state, intent, reality, started_at)

    :telemetry.execute(
      [:flezha, :convergence, :tick],
      %{
        operatives_total: length(intent.operatives),
        actions_issued: issued,
        duration_ms: elapsed
      },
      %{state: :ticking}
    )

    state
  end

  # Everything whose result a heal decision consumes, in the order it is
  # consumed. Returns the tick's intent and reality so the report pass reads the
  # same two snapshots the heals were planned against.
  defp heal_pass(%State{} = state, started_at) do
    # Age out stalled entries the Watchdog has stopped re-flagging: a
    # operative that resumed producing leaves the set without a manual clear,
    # and its ladder cooldown is released with it.
    state = expire_stalled(state, started_at)
    stalled = state.stalled_operatives

    # The data-plane verdict is computed BEFORE the plan because the Diff
    # consumes it: a wedged layer is a divergence like any other.
    state = Liveness.classify(state)

    intent = Intent.declared()
    reality = Probe.snapshot()

    # Per-namespace recovery gate: heal only operatives whose hub has finished
    # recovery. A namespace mid-replay is skipped (no heal against a
    # half-restored registry) without blocking healing for ready namespaces.
    ready = RecoveryGate.ready_namespaces(intent)
    heal_operatives = Enum.filter(intent.operatives, &MapSet.member?(ready, &1.namespace))

    delta =
      Diff.compute(%Intent{operatives: heal_operatives}, reality, %{
        verdict: state.stall_verdict,
        stalled: Map.keys(stalled)
      })

    {streaks, delta} = apply_misbound_streak(state.misbound_streaks, delta)
    state = %{state | misbound_streaks: streaks}

    delta = without_pending_enables(delta)
    plugin_degraded = plugin_degraded_operatives(delta)
    maybe_log_plugin_degraded(plugin_degraded, state.plugin_degraded)
    state = %{state | plugin_degraded: plugin_degraded}

    {backoff, issued} = execute_plan(Planner.plan(delta, state.backoff), state.backoff)

    elapsed = System.monotonic_time(:millisecond) - started_at

    {%{state | backoff: backoff}, intent, reality, issued, elapsed}
  end

  # Report-only /healthz inputs: producing-liveness verdicts from durable
  # progress markers and the durable-evidence-vs-intent diff. Neither
  # feeds a heal decision.
  defp report_pass(%State{} = state, intent, reality, started_at) do
    state = Progress.classify(state, started_at)

    {mismatch, cache} =
      Intent.detect_mismatch(intent, state.intent_mismatch, state.evidence_cache)

    state = %{state | intent_mismatch: mismatch, evidence_cache: cache}

    # Publish health over the FULL intent so /healthz reports the true
    # intended count, not just the recovery-ready subset. Returns state with
    # the refreshed unmet-intent deadline.
    Summary.publish(intent, reality, state)
  end

  defp tick_failed_telemetry() do
    :telemetry.execute(
      [:flezha, :convergence, :tick],
      %{operatives_total: 0, actions_issued: 0},
      %{state: :error}
    )
  end

  # A operative must be misbound for @misbound_min_ticks consecutive ticks before
  # it is migrated. Operatives not misbound this tick drop out of the streak map
  # (reset), so a operative seen bound again clears immediately.
  @doc false
  def apply_misbound_streak(streaks, %Diff{misbound_operatives: misbound} = delta) do
    new_streaks =
      Enum.reduce(misbound, %{}, fn {ref, _dead}, acc ->
        Map.put(acc, ref, Map.get(streaks, ref, 0) + 1)
      end)

    ready =
      Enum.filter(misbound, fn {ref, _dead} ->
        Map.get(new_streaks, ref) >= @misbound_min_ticks
      end)

    {new_streaks, %{delta | misbound_operatives: ready}}
  end

  # Drop stalled entries past the grace and release their ladder cooldown with
  # them: the cooldown records how far up the ladder a LIVE stall has walked, so
  # a operative that resumed producing must start from the cheap rung again.
  defp expire_stalled(%State{} = state, now) do
    {kept, expired} =
      Enum.split_with(state.stalled_operatives, fn {_operative, since} ->
        now - since < state.output_stall_grace_ms
      end)

    backoff =
      Enum.reduce(expired, state.backoff, fn {operative_id, _since}, acc ->
        Backoff.update(acc, Planner.ladder_key(operative_id), :ok, now)
      end)

    %{state | stalled_operatives: Map.new(kept), backoff: backoff}
  end

  # An enable in flight has written its cspec and not yet subscribed — that is
  # the cspec-first order, not a degradation — so its plugin is not healed
  # (a heal would subscribe it a second time, racing the enable).
  defp without_pending_enables(%Diff{missing_plugins: []} = delta), do: delta

  defp without_pending_enables(%Diff{} = delta) do
    pending =
      if Process.whereis(Flezha.Service.PluginManager),
        do: PluginManager.pending_ids(),
        else: MapSet.new()

    %{
      delta
      | missing_plugins:
          Enum.reject(delta.missing_plugins, fn {_ref, plugin_id, _plugin} ->
            MapSet.member?(pending, plugin_id)
          end)
    }
  end

  # Operatives whose declared plugins diverge from production reality this tick:
  # missing (not subscribed) or self-reported unhealthy. Sorted for a stable
  # transition compare.
  defp plugin_degraded_operatives(%Diff{} = delta) do
    missing = Enum.map(delta.missing_plugins, fn {{_ns, rid}, _id, _plugin} -> rid end)
    unhealthy = Enum.map(delta.unhealthy_plugins, fn {{_ns, rid}, _id, _plugin, _r} -> rid end)
    Enum.uniq(missing ++ unhealthy) |> Enum.sort()
  end

  defp maybe_log_plugin_degraded(same, same), do: :ok

  defp maybe_log_plugin_degraded(new, old) do
    fresh = new -- old
    cleared = old -- new

    if fresh != [] do
      Logger.warning(
        "[convergence] plugin-degraded operatives #{inspect(fresh)} — declared plugins missing or unhealthy; healing"
      )
    end

    if cleared != [] do
      Logger.info("[convergence] plugin-degraded cleared for #{inspect(cleared)}")
    end
  end

  defp execute_plan(plan, backoff) do
    now = System.monotonic_time(:millisecond)

    Enum.reduce(plan, {backoff, 0}, fn op, {acc_backoff, count} ->
      result = Heal.execute(op)
      {Backoff.update(acc_backoff, Planner.key(op), ladder_outcome(op, result), now), count + 1}
    end)
  end

  # Only the Watchdog going quiet proves a stall cleared, and `expire_stalled/2`
  # is where that resets the entry — so a ladder rung advances its cooldown on
  # either outcome (see `Planner.ladder?/1`).
  defp ladder_outcome(op, result), do: if(Planner.ladder?(op), do: :error, else: result)
end
