defmodule Flezha.Convergence.Loop do
  @moduledoc """
  The single convergence GenServer — the four-seam declarative reconciler.
  Every tick (and on a declared fact) it reads `Intent`, probes `Probe`
  reality, computes a `Diff` over reality and the held facts, the `Planner`
  turns the diff into backoff-filtered primitive ops, and `Heal` executes
  them. The resulting reality + health summary is published to
  `Flezha.Convergence.Snapshot` so `/healthz`, the CLI, and telemetry all read
  one source of truth.

  Reality-matches-intent is the *event*, not the default: the loop heals
  every divergence it finds, not only ones triggered by a specific signal. A
  declared fact (`Flezha.Convergence.Facts.declare/1`) shortens detection
  latency by ticking at once when its key is new, and carries the reason the
  Planner keys the remedy on; every site that observes something declares it
  here and decides nothing.
  """

  use GenServer
  use Flezha.Process.Timer, keys: [:tick]

  alias Flezha.Config
  alias Flezha.Config.Backoff
  alias Flezha.Config.Durations
  alias Flezha.Convergence.{Diff, Facts, Heal, Intent, Planner, Probe, RecoveryGate}
  alias Flezha.Convergence.Verdict

  alias Flezha.Convergence.Loop.{Liveness, Progress, State, Summary}
  alias Flezha.Identificator
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.HubManager

  require Logger

  # A relay must be misbound for this many consecutive ticks before it is
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
  def tick_now(),
    do: GenServer.call(__MODULE__, :tick_now, Durations.get(:convergence_tick_call_ms))

  # ---------- GenServer ----------

  @impl true
  def init(opts) do
    tick_ms =
      Keyword.get(
        opts,
        :tick_ms,
        Config.get(:convergence_tick_ms)
      )

    grace_ms =
      Keyword.get(opts, :output_stall_grace_ms, Config.get(:convergence_output_stall_grace_ms))

    # Detach first: `terminate/2` only runs when the Loop is trapping exits, so
    # a supervisor restart otherwise leaves the previous incarnation's handler
    # registered under this id. `attach_many` would then refuse as
    # `:already_exists` and the new Loop would never hear another sensor event
    # — a silently deaf actor, which is the failure class this loop exists to
    # close.
    _ = :telemetry.detach(@handler_id)

    _ =
      :telemetry.attach_many(@handler_id, @telemetry_subs, &__MODULE__.handle_telemetry/4, self())

    # First tick immediately so the recovery gate + first publish happen
    # without waiting `tick_ms`.
    {:ok, request_tick(%State{tick_ms: tick_ms, fact_grace_ms: grace_ms})}
  end

  @impl true
  def handle_info(:tick, %State{} = state) do
    state = run_tick(timer_fired(%{state | tick_pending?: false}, :tick))
    {:noreply, schedule_tick(state)}
  end

  def handle_info({:declare, kind, target, args}, %State{} = state) do
    {:noreply, declared(state, kind, target, args)}
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
  def handle_telemetry([:flezha, :state, :output_stall], _measurements, metadata, _loop_pid) do
    relay_id = Map.get(metadata, :relay_instance)

    if is_binary(relay_id),
      do: Facts.declare({:output_stall, relay_id, Map.take(metadata, [:runner_id])})

    :ok
  catch
    _, _ -> :ok
  end

  # The hook fires at round COMPLETION, so this is an adopt-and-converge
  # trigger, not a hold. A quiet round (`started = 0`) changed nothing and
  # costs nothing.
  def handle_telemetry([:flezha, :boot, :reconcile_round], measurements, metadata, _loop_pid) do
    if Map.get(measurements, :started, 0) > 0,
      do: Facts.declare({:reconcile_started, Map.get(metadata, :hub_id), %{}})

    :ok
  catch
    _, _ -> :ok
  end

  def handle_telemetry(_event, _measurements, _metadata, _config), do: :ok

  # ---------- the intake ----------

  # One declaration: a known key refreshes its time and costs no tick, a new
  # key ticks at once when its kind says so, a counter-fact clears its fact,
  # and a runner arriving in a namespace releases the ladders of the relays
  # it could take — the world changed, so they are retried at once.
  defp declared(%State{} = state, kind, target, args) do
    now = System.monotonic_time(:millisecond)
    {facts, what} = Facts.put(state.facts, kind, target, args, now)
    state = %{state | facts: facts}
    state = release(state, kind, target, what, now)
    if Facts.ticks?(kind, what), do: request_tick(state), else: state
  end

  # A counter-fact is the evidence its fact's ladder was waiting for, so it
  # releases the key as well as dropping the fact — `settle_facts/3` never sees
  # a fact cleared this way. A runner connecting releases the keys of every
  # relay in its namespace waiting for a runner it could serve: the world
  # changed, which is what the deleted per-attempt runner re-resolution used to
  # buy.
  defp release(%State{} = state, kind, target, :cleared, now) do
    case Facts.cleared_key(kind, target) do
      nil -> state
      key -> %{state | backoff: Backoff.update(state.backoff, key, :ok, now)}
    end
  end

  defp release(%State{} = state, :runner_connected, {namespace, runner_id}, _what, now) do
    waiting = waiting_relays(state, namespace)
    sleeping = sleeping_relays(state, namespace)
    servable = servable_factories(namespace, runner_id, Map.merge(waiting, sleeping))
    wake_sleepers(namespace, sleeping, servable)

    backoff =
      Enum.reduce(waiting, state.backoff, fn {id, factory}, acc ->
        if release?(factory, servable) do
          {namespace, id}
          |> Planner.waiting_keys()
          |> Enum.reduce(acc, &Backoff.update(&2, &1, :ok, now))
        else
          acc
        end
      end)

    %{state | backoff: backoff}
  end

  defp release(%State{} = state, _kind, _target, _what, _now), do: state

  # Every suspended relay in `namespace`, with its factory. One sent a message
  # while no runner could take it keeps the message journaled, and only the
  # next message or its timer would wake it otherwise.
  defp sleeping_relays(%State{} = state, namespace) do
    for {{:relay, {ns, id}}, %Verdict{state: :suspended, facts: facts}} <- state.verdicts,
        ns == namespace,
        into: %{},
        do: {id, Map.get(facts, :factory_id)}
  end

  # A cast, so a slow Overwatch never holds the loop; the Overwatch wakes only
  # when its log owes input or its timer has passed. Only a runner known to
  # serve the factory wakes a sleeper: the timer and the next message still do.
  defp wake_sleepers(namespace, sleeping, servable) do
    for {id, factory} <- sleeping,
        is_struct(servable, MapSet) and MapSet.member?(servable, factory),
        {:ok, pid} <- [HubManager.get_pid(namespace, id)],
        do: GenServer.cast(pid, :rehydrate_if_pending)
  end

  # Every relay in `namespace` whose next step is a binding — halted,
  # unbound, or misbound to a runner that is gone — with the factory the judge
  # read for it, so the release path and the Diff agree on who is waiting.
  #
  # The last tick's records are the whole answer: a key only reaches a rung
  # through `execute_plan/2`, which runs inside a tick after `judge/4` has
  # written that tick's records, so a relay with a cooldown to release
  # always has a record here.
  defp waiting_relays(%State{} = state, namespace) do
    for {{:relay, {ns, id}}, %Verdict{state: judged, facts: facts}} <- state.verdicts,
        ns == namespace,
        judged in Diff.awaiting_binding(),
        into: %{},
        do: {id, Map.get(facts, :factory_id)}
  end

  # A runner releases a relay it could take, and one whose factory nobody
  # could resolve. This guard stops the needless work; it is not what makes the
  # release safe — an advertisement can change between here and the plan, so
  # the rebind is what refuses to charge a rung for an attempt that had no
  # runner to bind to.
  defp release?(_factory, :unknown), do: true
  defp release?(nil, _servable), do: true
  defp release?(factory, servable), do: MapSet.member?(servable, factory)

  # Which of the waiting relays' factories this runner advertises, in one
  # bounded read. A refusal answers `:unknown`, and so does a runner that has
  # advertised nothing yet — an SDK may send its factories on a later frame,
  # and "has told us nothing" must not read as "serves nothing". Both release
  # everything: a needless release costs a plan, a missed one costs a rung.
  defp servable_factories(namespace, runner_id, waiting) when map_size(waiting) > 0 do
    case FleetManager.factories_for_runner(namespace, runner_id) do
      %MapSet{} = factories -> if MapSet.size(factories) == 0, do: :unknown, else: factories
      _unreadable -> :unknown
    end
  end

  defp servable_factories(_namespace, _runner_id, _waiting), do: :unknown

  # An immediate tick, folded: declarations that land while one is pending
  # ride the tick already on its way, so a burst of facts costs one probe.
  defp request_tick(%State{tick_pending?: true} = state), do: state

  defp request_tick(%State{} = state) do
    state = arm_timer(state, :tick, 0, :tick)
    %{state | tick_pending?: true}
  end

  defp schedule_tick(%State{tick_pending?: true} = state), do: state

  defp schedule_tick(%State{} = state), do: arm_timer(state, :tick, state.tick_ms, :tick)

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
        relays_total: length(intent.relays),
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
    # The data-plane verdict and the producing-liveness markers are read BEFORE
    # the plan because the judge folds both into the records the Diff consumes:
    # one sample, one judgement, one set of records every surface then reads.
    state = Progress.classify(Liveness.classify(state), started_at)

    intent = Intent.declared()
    {unplaced, unplaced_since} = Intent.detect_unplaced(intent, state.unplaced_since, started_at)
    reality = Map.put(Probe.snapshot(), :unplaced, unplaced)
    state = %{state | no_quorum: no_quorum_namespaces(), unplaced_since: unplaced_since}

    # Facts the probe shows repaired leave the map and release their ladder
    # BEFORE the plan, so a rung never fires for a fault already gone.
    state = settle_facts(state, reality, started_at)
    state = judge(state, intent, reality, started_at)

    {state, delta} = plan_delta(state, intent, reality)

    plan = Planner.plan(delta, state.backoff, unmet_last: state.unmet)
    {backoff, issued, unmet} = execute_plan(plan, state.backoff)
    state = %{state | backoff: backoff, unmet: unmet}

    reclaim_vacant_hubs(reality)

    # A plan that filled the per-tick cap may have been cut: re-tick without
    # waiting `tick_ms`, so the cap bounds one tick's blast radius and never
    # becomes a rate.
    state = if issued >= Planner.max_ops(), do: request_tick(state), else: state

    {state, intent, reality, issued, System.monotonic_time(:millisecond) - started_at}
  end

  # One verdict record per subject, judged once from the tick's facts. Every
  # reader after this — the Diff, the snapshot, `/healthz` and the CLI — reads
  # these and never re-derives a health word of its own.
  defp judge(%State{} = state, intent, reality, now) do
    verdicts =
      Probe.judge(Probe.context(intent, reality, signals(state), now), state.verdicts)

    Verdict.emit_transitions(state.verdicts, verdicts)
    plugin_degraded = Verdict.plugin_degraded(verdicts)
    maybe_log_plugin_degraded(plugin_degraded, state.plugin_degraded)

    %{state | verdicts: verdicts, plugin_degraded: plugin_degraded}
  end

  # The divergence this tick will act on: diffed over the recovery-ready
  # relays only — a namespace mid-replay is skipped, so no heal runs
  # against a half-restored registry, and so is one whose hub has no quorum
  # here, whose relays the other side of the split runs — then misbound
  # relays streak-gated.
  defp plan_delta(%State{} = state, intent, reality) do
    ready = RecoveryGate.ready_namespaces(intent)

    heal_relays =
      Enum.filter(
        intent.relays,
        &(MapSet.member?(ready, &1.namespace) and &1.namespace not in state.no_quorum)
      )

    delta =
      Diff.compute(%Intent{relays: heal_relays}, reality, state.verdicts, state.facts)

    {streaks, delta} = apply_misbound_streak(state.misbound_streaks, delta)

    {%{state | misbound_streaks: streaks}, delta}
  end

  # The signals the fact readers name, lifted out of the Loop's struct: the
  # declared facts, the producing-liveness markers and the data-plane sample.
  defp signals(%State{} = state) do
    %{
      facts: state.facts,
      stuck: state.stuck_relays,
      stall_verdict: state.stall_verdict,
      runner_facts: state.runner_facts,
      liveness: state.prev_liveness
    }
  end

  # Report-only /healthz inputs: the durable-evidence-vs-intent diff, which
  # feeds no heal decision.
  defp report_pass(%State{} = state, intent, reality, started_at) do
    {{mismatch, since}, cache} =
      Intent.detect_mismatch(
        intent,
        {state.intent_mismatch, state.intent_mismatch_since},
        state.evidence_cache,
        started_at
      )

    state = %{
      state
      | intent_mismatch: mismatch,
        intent_mismatch_since: since,
        evidence_cache: cache
    }

    # Publish health over the FULL intent so /healthz reports the true
    # intended count, not just the recovery-ready subset. Returns state with
    # the refreshed unmet-intent deadline.
    Summary.publish(intent, reality, state)
  end

  defp no_quorum_namespaces() do
    Enum.filter(
      HubManager.get_running_namespaces(),
      &(HubManager.quorum(Identificator.hub(&1)) != :ok)
    )
  end

  defp tick_failed_telemetry() do
    :telemetry.execute(
      [:flezha, :convergence, :tick],
      %{relays_total: 0, actions_issued: 0},
      %{state: :error}
    )
  end

  # A hub on a node whose fleet does not serve its namespace is left when it
  # holds nothing another member does not hold (`HubManager.holds_anything?/1`):
  # the node leaves that namespace's cluster rather than being handed children
  # it cannot host. Idempotent, and it stops nothing while a local relay is
  # still there, which is what makes it safe to run beside the plan that is
  # still moving them off. The fleet manager decides, since a runner may have
  # connected since the snapshot.
  defp reclaim_vacant_hubs(%{active_runners: active}) do
    for {namespace, runners} <- active,
        runners != :unknown,
        MapSet.size(runners) == 0,
        do: FleetManager.reclaim_vacant_hub(namespace)

    :ok
  end

  # A relay must be misbound for @misbound_min_ticks consecutive ticks before
  # it is migrated. Relays not misbound this tick drop out of the streak map
  # (reset), so a relay seen bound again clears immediately. A declared
  # disconnect is a closed session the fleet manager observed, not a probe
  # miss, so it is not gated by the streak.
  @doc false
  def apply_misbound_streak(streaks, %Diff{misbound_relays: misbound} = delta) do
    new_streaks =
      Enum.reduce(misbound, %{}, fn {ref, _dead, _reason}, acc ->
        Map.put(acc, ref, Map.get(streaks, ref, 0) + 1)
      end)

    ready =
      Enum.filter(misbound, fn {ref, _dead, reason} ->
        reason != :probe or Map.get(new_streaks, ref) >= @misbound_min_ticks
      end)

    {new_streaks, %{delta | misbound_relays: ready}}
  end

  # Drop the facts reality shows repaired or that outlived their grace, and
  # release the ladder each one walked: the cooldown records how far up the
  # ladder a LIVE fault has walked, so a repaired target starts from the cheap
  # rung again.
  defp settle_facts(%State{} = state, reality, now) do
    {facts, released} = Facts.settle(state.facts, reality, state.fact_grace_ms, now)
    backoff = Enum.reduce(released, state.backoff, &Backoff.update(&2, &1, :ok, now))
    %{state | facts: facts, backoff: backoff}
  end

  # The plugin-degraded set changed: log the transition, never the steady
  # state — the set itself is on the snapshot for `/healthz` to read.
  defp maybe_log_plugin_degraded(same, same), do: :ok

  defp maybe_log_plugin_degraded(new, old) do
    fresh = new -- old
    cleared = old -- new

    if fresh != [] do
      Logger.warning(
        "[convergence] plugin-degraded relays #{inspect(fresh)} — declared plugins missing or unhealthy; healing"
      )
    end

    if cleared != [] do
      Logger.info("[convergence] plugin-degraded cleared for #{inspect(cleared)}")
    end
  end

  # `issued` counts the ops that APPLIED something: an op whose precondition was
  # unmet did nothing, so it neither advances its key nor argues that the tick's
  # cap cut a longer plan short. Counting it would re-tick immediately on a
  # namespace with no capable runner and turn a cap into a spin.
  defp execute_plan(plan, backoff) do
    now = System.monotonic_time(:millisecond)

    Enum.reduce(plan, {backoff, 0, MapSet.new()}, fn op, {acc_backoff, count, unmet} ->
      case Heal.execute(op) do
        :unmet ->
          {acc_backoff, count, MapSet.put(unmet, Planner.key(op))}

        result ->
          {Backoff.update(acc_backoff, Planner.key(op), ladder_outcome(op, result), now),
           count + 1, unmet}
      end
    end)
  end

  # A ladder rung's `:ok` means the remedy was APPLIED, not that the fault
  # cleared — only the declared evidence proves that, and `settle_facts/3` is
  # where it resets the key — so a rung advances its cooldown on either outcome.
  defp ladder_outcome(op, result), do: if(Planner.ladder?(op), do: :error, else: result)
end
