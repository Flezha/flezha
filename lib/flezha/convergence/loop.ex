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

  alias Flezha.Convergence.{
    Backoff,
    Diff,
    Heal,
    Intent,
    Planner,
    Probe,
    RecoveryGate,
    Snapshot,
    StallClassifier
  }

  alias Flezha.Service.{EventManager, HubManager}
  alias Flezha.Transport.LivenessCounters
  alias Flezha.Transport.Server, as: TransportServer

  require Logger

  @default_tick_ms 5_000
  @default_output_stall_grace_ms 60_000
  # A operative must be misbound for this many consecutive ticks before it is
  # migrated, so a brief runner blip cannot trigger a full-namespace
  # migration + WAL replay.
  @misbound_min_ticks 2

  @telemetry_subs [
    [:flezha, :state, :output_stall],
    [:flezha, :boot, :reconcile_round]
  ]
  @handler_id {__MODULE__, :events}

  defstruct tick_ms: @default_tick_ms,
            output_stall_grace_ms: @default_output_stall_grace_ms,
            backoff: Backoff.new(),
            # {namespace, operative_id} => consecutive ticks observed misbound.
            misbound_streaks: %{},
            # operative_instance => monotonic_ms when last flagged stalled.
            # Reported to /healthz as degraded and healed by the output-stall
            # ladder (resync, then rebind). Entries age out after the grace.
            stalled_operatives: %{},
            # operative_ids whose declared plugins were missing or self-reported
            # unhealthy on the last tick. Reported to /healthz as degraded;
            # kept only to log set transitions, not steady state.
            plugin_degraded: [],
            # Previous data-plane liveness snapshot, this tick's classifier
            # verdict, and how many consecutive polls have reported that same
            # layer. The verdict feeds the Diff (layers a heal primitive can
            # reach) and the Snapshot (fabric layers, which only a process
            # restart clears — /healthz turns those into a 503 past the streak).
            prev_liveness: nil,
            stall_verdict: :ok,
            stall_streak: 0,
            # operative_id => %{detail:, entity:} producing-liveness STUCK
            # verdicts (fed but progress markers flat past grace). Report-only:
            # healing stays on the output-stall ladder.
            stuck_operatives: %{},
            # [{taken_at_ms, tier-cursor markers}] — grace-window baselines for
            # the classifier's cursor-delta checks. Holds only Entities with
            # effectively-once tier cursors (empty until that tier lands).
            progress_history: [],
            # Operatives with durable state evidence but no live intent — the
            # wiped-registry witness /healthz degrades on.
            intent_mismatch: [],
            # {state-root mtime, first-level dir listing} cache.
            evidence_cache: nil,
            # Monotonic ms when `bound < intended` first held; nil while met or
            # recovering. healthz escalates recovering → degraded past its grace.
            unmet_intent_since: nil

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
        Application.get_env(:flezha, :convergence_tick_ms, @default_tick_ms)
      )

    grace_ms =
      Keyword.get(
        opts,
        :output_stall_grace_ms,
        Application.get_env(
          :flezha,
          :convergence_output_stall_grace_ms,
          @default_output_stall_grace_ms
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
     %__MODULE__{
       tick_ms: tick_ms,
       output_stall_grace_ms: grace_ms
     }}
  end

  @impl true
  def handle_info(:tick, %__MODULE__{} = state) do
    state = run_tick(state)
    Process.send_after(self(), :tick, state.tick_ms)
    {:noreply, state}
  end

  def handle_info({:runner_connected_event, {_namespace, _runner_id}}, %__MODULE__{} = state) do
    {:noreply, run_tick(state)}
  end

  # An already-recorded stall only refreshes its timestamp: the Watchdog
  # re-flags a frozen Entity every sweep, and the ladder for that operative is
  # already walking on the periodic tick. Ticking per re-flag instead would make
  # a handful of stalled Entities drive the whole loop at sweep cadence.
  def handle_info({:output_stall, operative_id}, %__MODULE__{} = state)
      when is_binary(operative_id) do
    known? = Map.has_key?(state.stalled_operatives, operative_id)
    now = System.monotonic_time(:millisecond)
    state = %{state | stalled_operatives: Map.put(state.stalled_operatives, operative_id, now)}

    if known?, do: {:noreply, state}, else: {:noreply, run_tick(state)}
  end

  # A reconcile round that started orphans: converge against them now rather
  # than at the next tick, so the plan adopts what the round just started
  # instead of a later tick finding them half-bound.
  def handle_info(:reconcile_started, %__MODULE__{} = state) do
    {:noreply, run_tick(state)}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def handle_call(:tick_now, _from, %__MODULE__{} = state) do
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
  defp run_tick(%__MODULE__{} = state) do
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

  defp do_run_tick(%__MODULE__{} = state) do
    started_at = System.monotonic_time(:millisecond)

    # Age out stalled entries the Watchdog has stopped re-flagging: a
    # operative that resumed producing leaves the set without a manual clear,
    # and its ladder cooldown is released with it.
    state = expire_stalled(state, started_at)
    stalled = state.stalled_operatives

    # The data-plane verdict is computed BEFORE the plan because the Diff
    # consumes it: a wedged layer is a divergence like any other.
    state = classify_data_plane(state)

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

    state = %{state | backoff: backoff}

    # Report-only /healthz inputs: producing-liveness verdicts from durable
    # progress markers and the durable-evidence-vs-intent diff. Neither
    # feeds a heal decision.
    state = classify_progress(state, started_at)
    state = detect_intent_mismatch(state, intent)

    # Publish health over the FULL intent so /healthz reports the true
    # intended count, not just the recovery-ready subset. Returns state with
    # the refreshed unmet-intent deadline.
    state = publish(intent, reality, state)

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

  # Localise a silent data-plane stall to one layer, emit a single event when
  # the verdict changes, and carry the layer + consecutive-poll streak forward:
  # the Diff heals the layers a primitive can reach, /healthz reports the
  # fabric ones. Any error here is swallowed — the classifier is instrumentation
  # feeding a decision, and must never be able to skip the tick that decides.
  defp classify_data_plane(%__MODULE__{} = state) do
    if LivenessCounters.poll_enabled?() do
      current = build_liveness_snapshot()
      verdict = StallClassifier.classify(current, state.prev_liveness, classifier_opts())
      maybe_emit_stall(verdict, StallClassifier.layer(state.stall_verdict))

      %{
        state
        | prev_liveness: current,
          stall_verdict: verdict,
          stall_streak: stall_streak(state, verdict)
      }
    else
      state
    end
  rescue
    e ->
      Logger.warning("[convergence] data-plane classifier skipped: #{Exception.message(e)}")
      state
  catch
    _, _ -> state
  end

  # Consecutive polls reporting the SAME layer. A different layer (or `:ok`)
  # restarts the count, so the persistence window /healthz reads can never be
  # satisfied by a run of unrelated one-poll verdicts.
  defp stall_streak(_state, :ok), do: 0

  defp stall_streak(%__MODULE__{} = state, verdict) do
    if StallClassifier.layer(verdict) == StallClassifier.layer(state.stall_verdict),
      do: state.stall_streak + 1,
      else: 1
  end

  # The classifier's own defaults, with the one knob an operator whose
  # Procedures legitimately run long has to raise.
  defp classifier_opts() do
    case Application.get_env(:flezha, :convergence_entity_stall_ms) do
      ms when is_integer(ms) and ms > 0 -> %{entity_stall_ms: ms}
      _ -> %{}
    end
  end

  defp build_liveness_snapshot() do
    control_drivers =
      TransportServer.list_active_transports()
      |> Enum.flat_map(fn t ->
        case TransportServer.get_stats(t) do
          {:ok, %{driver: d}} when is_map(d) -> [Map.put(d, :transport, t)]
          _ -> []
        end
      end)

    # Per-runner data-stream servers (StreamAcceptor-owned) — the layer
    # that actually carries operative traffic, invisible to get_stats/1.
    data_drivers =
      LivenessCounters.driver_refs()
      |> Enum.flat_map(fn {{runner_id, type}, ref} ->
        case Flezha.Transport.Native.driver_stats(ref) do
          d when is_map(d) -> [Map.put(d, :transport, {:uds_data, runner_id, type})]
          _ -> []
        end
      end)

    %{channels: channels, operatives: operatives} = LivenessCounters.snapshot_all()
    %{drivers: control_drivers ++ data_drivers, channels: channels, operatives: operatives}
  end

  defp maybe_emit_stall(:ok, last) when last != nil do
    Logger.info("[convergence] data-plane stall cleared (was #{inspect(last)})")
  end

  defp maybe_emit_stall(:ok, nil), do: :ok

  defp maybe_emit_stall({layer, ctx}, last) when layer != last do
    # Telemetry feeds the bus; the log line is what an operator greps during
    # an incident — the 2026-06-10 freeze emitted no log at onset.
    Logger.warning(
      "[convergence] data-plane stall localized: layer=#{inspect(layer)} #{inspect(ctx)}"
    )

    :telemetry.execute(
      [:flezha, :data_plane, :stall_localized],
      %{count: 1},
      Map.put(ctx, :layer, layer)
    )
  end

  defp maybe_emit_stall(_verdict, _last), do: :ok

  # Producing-liveness from durable progress markers (report-only; the
  # output-stall ladder stays the heal). Reads the
  # existing WalWriter state at tick cadence; a failure here must never
  # skip the tick's heals or publish.
  defp classify_progress(%__MODULE__{} = state, now) do
    markers = progress_markers()

    {baseline, history} =
      rotate_progress_history(
        state.progress_history,
        Enum.filter(markers, &tier_cursors?/1),
        now,
        state.output_stall_grace_ms
      )

    verdicts =
      StallClassifier.classify_producing(markers, baseline, %{
        now: now,
        grace_ms: state.output_stall_grace_ms
      })

    stuck =
      for {id, {:stuck, detail, ctx}} <- verdicts, into: %{} do
        {sh, pk} = ctx.entity
        {id, %{detail: detail, entity: "#{sh}/#{pk}"}}
      end

    note_stuck_transitions(state.stuck_operatives, stuck)
    %{state | progress_history: history, stuck_operatives: stuck}
  rescue
    e ->
      Logger.warning("[convergence] producing-liveness skipped: #{Exception.message(e)}")
      state
  catch
    _, _ -> state
  end

  # Per-Entity fed/advance evidence from the existing Watchdog/WalWriter state,
  # plus the tier cursors when the operative runs the effectively-once tier. The
  # input-log read is per OPERATIVE, not per Entity — an operative with 200
  # Entities costs one lookup per tick, not 200. A tier-off operative yields no
  # cursor fields, and classification degrades to WAL-seq-only.
  @doc false
  def progress_markers() do
    Flezha.State.Watchdog.entity_statuses()
    |> Enum.group_by(fn {{rc, _sh, _pk}, _s} -> rc end)
    |> Enum.flat_map(fn {rc, statuses} ->
      cursors = Flezha.State.InputLog.cursors(rc)

      Enum.map(statuses, fn {{_rc, sh, pk} = entity_id, s} ->
        %{
          operative_id: rc,
          entity: {sh, pk},
          last_change_seen_at: s.last_change_seen_at,
          last_advance_at: s.last_advance_at,
          out_pending_seq: s.out_pending_seq,
          outbox_flushed_seq: s.outbox_flushed_seq,
          outbox_unconfirmed_ms: s.outbox_unconfirmed_ms
        }
        |> merge_input_cursors(cursors, entity_id)
      end)
    end)
  end

  # An Entity that has never reported a consumed offset has no entry, so no key is
  # merged and the watermark guard stays false — an Entity with no durable progress
  # has nothing to freeze.
  defp merge_input_cursors(marker, nil, _entity_id), do: marker

  defp merge_input_cursors(marker, %{tail: tail, consumed: consumed}, entity_id) do
    marker = Map.put(marker, :input_log_tail, tail)

    case Map.fetch(consumed, entity_id) do
      {:ok, offset} -> Map.put(marker, :consumed_offset, offset)
      :error -> marker
    end
  end

  defp tier_cursors?(m),
    do: is_integer(m[:consumed_offset]) or is_integer(m[:outbox_flushed_seq])

  # Keep the newest snapshot at least one grace window old as the
  # cursor-delta baseline (plus everything younger); nil until one has aged
  # enough, so cursor checks stay off during the first grace after boot.
  @doc false
  def rotate_progress_history(history, snapshot, now, grace_ms) do
    {aged, young} =
      Enum.split_with(history ++ [{now, snapshot}], fn {at, _} -> now - at >= grace_ms end)

    case List.last(aged) do
      nil -> {nil, young}
      {_at, baseline} = entry -> {baseline, [entry | young]}
    end
  end

  # Telemetry + operator log on stuck-set transitions only; steady state is
  # visible in /healthz, not re-emitted every tick.
  @doc false
  def note_stuck_transitions(same, same), do: :ok

  def note_stuck_transitions(prev, new) do
    fresh = Map.drop(new, Map.keys(prev))
    cleared = Map.keys(prev) -- Map.keys(new)

    Enum.each(fresh, fn {id, %{detail: detail, entity: entity}} ->
      Logger.warning("[convergence] operative stuck: #{id} #{detail} entity=#{entity}")

      :telemetry.execute(
        [:flezha, :convergence, :operative_stuck],
        %{count: 1},
        %{operative_id: id, detail: detail, entity: entity}
      )
    end)

    if cleared != [], do: Logger.info("[convergence] stuck cleared for #{inspect(cleared)}")
    :ok
  end

  # Durable evidence of prior intent (state dirs + durable-registry rows)
  # contradicting the live intended set — the wiped-registry witness.
  # Report-only; an operator resumes the operative or deletes its state
  # (`flz state delete` / `flz reset`), which clears the entry.
  defp detect_intent_mismatch(%__MODULE__{} = state, intent) do
    {mismatch, cache} = intent_mismatch(intent, state.evidence_cache)
    note_intent_mismatch_transition(state.intent_mismatch, mismatch)
    %{state | intent_mismatch: mismatch, evidence_cache: cache}
  rescue
    e ->
      Logger.warning("[convergence] intent-mismatch check skipped: #{Exception.message(e)}")
      state
  catch
    _, _ -> state
  end

  @doc false
  @spec intent_mismatch(Intent.t(), term()) :: {[binary()], term()}
  def intent_mismatch(%Intent{operatives: operatives}, cache) do
    live = MapSet.new(operatives, & &1.operative_id)
    {dirs, cache} = state_root_dirs(cache)
    evidence = MapSet.union(MapSet.new(dirs), HubManager.durable_operative_ids())

    # A finished workflow's state is retained for inspection on purpose
    # (`Flezha.State.Completion`): intent fulfilled, not intent lost.
    mismatch =
      evidence
      |> MapSet.difference(live)
      |> Enum.reject(&Flezha.State.Completion.exists?/1)
      |> Enum.sort()

    {mismatch, cache}
  end

  @doc false
  def note_intent_mismatch_transition(same, same), do: :ok

  def note_intent_mismatch_transition(_prev, []) do
    Logger.info("[convergence] intent mismatch cleared")
  end

  def note_intent_mismatch_transition(_prev, mismatch) do
    Logger.warning(
      "[convergence] intent mismatch — durable state evidence without live intent: #{inspect(mismatch)}"
    )

    :telemetry.execute(
      [:flezha, :convergence, :intent_mismatch],
      %{count: length(mismatch)},
      %{operatives: mismatch}
    )
  end

  # Operative-class listing under the namespaced `<root>/<ns>/<class>` tree,
  # re-read only when the newest relevant mtime changes — the root's (a
  # namespace added/removed) or any namespace dir's (a class added/removed);
  # deeper writes touch neither. A cache entry younger than 2 s is never
  # trusted — POSIX mtime has 1 s granularity, so a same-second add+remove
  # would otherwise pin a stale listing forever.
  defp state_root_dirs(cache) do
    root = Flezha.State.Registry.state_root()

    case File.stat(root, time: :posix) do
      {:ok, %File.Stat{mtime: root_mtime}} ->
        mtime = Enum.max([root_mtime | namespace_dir_mtimes()])
        now_s = System.os_time(:second)

        case cache do
          {^mtime, dirs} when now_s - mtime > 1 ->
            {dirs, cache}

          _ ->
            dirs = Flezha.State.Registry.operative_classes()
            {dirs, {mtime, dirs}}
        end

      _ ->
        {[], nil}
    end
  end

  defp namespace_dir_mtimes() do
    for namespace <- Flezha.State.Registry.namespaces(),
        {:ok, %File.Stat{mtime: mtime}} <-
          [File.stat(Flezha.State.Registry.namespace_dir(namespace), time: :posix)],
        do: mtime
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
  defp expire_stalled(%__MODULE__{} = state, now) do
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
        do: Flezha.Service.PluginManager.pending_ids(),
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

  defp publish(%Intent{operatives: operatives}, reality, %__MODULE__{} = state) do
    # `intended` = declared operatives (what should be bound); `bound` = operatives
    # actually observed running on runners. After a restart the controller is
    # up but its operatives are not yet rebound (bound < intended) — that is a
    # recovery window, NOT healthy, even though nothing is "stalled".
    intended = length(operatives)
    bound = map_size(Map.get(reality, :operatives, %{}))
    stalled = Map.keys(state.stalled_operatives)
    stuck = Map.keys(state.stuck_operatives)
    non_producing = Enum.uniq(stalled ++ stuck ++ state.plugin_degraded)
    producing = max(bound - length(non_producing), 0)

    # After a restart the durable registry may hold operatives that are not yet
    # bound to a live pid — either the hub has not started (lazy on runner
    # reconnect) or it has replayed its DETS rows but the stale pids are dead.
    # In that window `Intent` is short of the durable set, so `bound < intended`
    # cannot catch it (it compares two already-shrunken counts). Surfacing the
    # registry-vs-loaded signal lets `/healthz` report `recovering` instead of
    # vacuously `healthy` until every durable operative is reloaded and bound.
    pending_restore = HubManager.durable_registry_pending?()

    # Unmet-intent clock: runs while `bound < intended` and not recovering.
    now = System.monotonic_time(:millisecond)

    unmet_intent_since =
      cond do
        bound >= intended or any_recovering?(reality) -> nil
        is_integer(state.unmet_intent_since) -> state.unmet_intent_since
        true -> now
      end

    Snapshot.publish(%{
      last_tick_at: now,
      stalled_operatives: stalled,
      stuck_operatives: state.stuck_operatives,
      plugin_degraded_operatives: state.plugin_degraded,
      intent_mismatch: state.intent_mismatch,
      intended_operatives: intended,
      bound_operatives: bound,
      producing_operatives: producing,
      pending_restore: pending_restore,
      unmet_intent_ms: unmet_intent_since && now - unmet_intent_since,
      operative_health: operative_health_map(operatives, reality, state),
      data_plane_layer: StallClassifier.layer(state.stall_verdict),
      data_plane_streak: state.stall_streak
    })

    %{state | unmet_intent_since: unmet_intent_since}
  end

  # Per-operative health for the operator surfaces (`flz operative list`,
  # `flz cluster status`), keyed `{namespace, operative_id}`. Derives the same
  # authoritative reality the Diff classifies against, so the CLI sees exactly
  # what the loop sees. A operative only appears `:unknown` here while a tick has
  # not yet observed it or its read failed — never permanently. Mirrors the
  # bound/misbound cond in `Diff.compute/2`, extended with the produce signals.
  @doc false
  @spec operative_health_map([Intent.operative()], map(), %__MODULE__{}) :: %{
          {atom(), binary()} => atom()
        }
  def operative_health_map(operatives, reality, %__MODULE__{} = state) do
    observed = Map.get(reality, :operatives, %{})
    active = Map.get(reality, :active_runners, %{})
    plugin_degraded = MapSet.new(state.plugin_degraded)

    Map.new(operatives, fn %{namespace: ns, operative_id: rid} ->
      obs = Map.get(observed, {ns, rid}, %{})
      bound = Map.get(obs, :bound_runner)
      live = Map.get(active, ns, MapSet.new())

      health =
        cond do
          # An observation failed this tick (operative state or live-runner read);
          # no authoritative signal, so report `:unknown` for this tick only.
          bound == :unknown -> :unknown
          # Evicted at a committed iteration boundary, not broken.
          Map.get(obs, :suspended?, false) -> :suspended
          is_nil(bound) -> :unbound
          live == :unknown -> :unknown
          # Bound to a runner that is no longer live — the loop will rebind.
          not MapSet.member?(live, bound) -> :failed
          Map.get(obs, :parked?, false) -> :degraded
          Map.has_key?(state.stalled_operatives, rid) -> :degraded
          Map.has_key?(state.stuck_operatives, rid) -> :degraded
          MapSet.member?(plugin_degraded, rid) -> :degraded
          true -> :healthy
        end

      {{ns, rid}, health}
    end)
  end

  defp any_recovering?(reality) do
    reality
    |> Map.get(:recovery, %{})
    |> Map.values()
    |> Enum.any?(&(&1 in [:recovery_pending, :recovering, :timeout]))
  end
end
