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
  signal. Declared events (`:output_stall`, `:executor_connected`) just
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
  # migrated, so a brief executor blip cannot trigger a full-namespace
  # migration + WAL replay.
  @misbound_min_ticks 2

  @telemetry_subs [[:flezha, :state, :output_stall]]
  @handler_id {__MODULE__, :events}

  defstruct tick_ms: @default_tick_ms,
            output_stall_grace_ms: @default_output_stall_grace_ms,
            backoff: Backoff.new(),
            # {namespace, operative_id} => consecutive ticks observed misbound.
            misbound_streaks: %{},
            # operative_instance => monotonic_ms when last flagged stalled.
            # Reported to /healthz as degraded; the Watchdog resyncs the
            # operative in place. Entries age out after the output-stall grace.
            stalled_operatives: %{},
            # operative_ids whose declared plugins were missing or self-reported
            # unhealthy on the last tick. Reported to /healthz as degraded;
            # kept only to log set transitions, not steady state.
            plugin_degraded: [],
            # Previous data-plane liveness snapshot + last emitted stall layer.
            # Detection only — the classifier never feeds a heal decision.
            prev_liveness: nil,
            last_stall_verdict: :ok,
            # operative_id => %{detail:, segment:} producing-liveness STUCK
            # verdicts (fed but progress markers flat past grace). Report-only:
            # healing stays on the :output_stall → rebind path.
            stuck_operatives: %{},
            # [{taken_at_ms, tier-cursor markers}] — grace-window baselines for
            # the classifier's cursor-delta checks. Holds only Segments with
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

    _ =
      :telemetry.attach_many(@handler_id, @telemetry_subs, &__MODULE__.handle_telemetry/4, self())

    # Shorten executor-takeover detection from `tick_ms` to ~ms.
    _ = EventManager.add_handler(:executor_connected_event)

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

  def handle_info({:executor_connected_event, {_namespace, _executor_id}}, %__MODULE__{} = state) do
    {:noreply, run_tick(state)}
  end

  def handle_info({:output_stall, operative_id}, %__MODULE__{} = state)
      when is_binary(operative_id) do
    now = System.monotonic_time(:millisecond)
    state = %{state | stalled_operatives: Map.put(state.stalled_operatives, operative_id, now)}
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
    # operative that resumed producing leaves the set without a manual clear.
    stalled = expire_stalled(state.stalled_operatives, started_at, state.output_stall_grace_ms)
    state = %{state | stalled_operatives: stalled}

    intent = Intent.declared()
    reality = Probe.snapshot()

    # Per-namespace recovery gate: heal only operatives whose hub has finished
    # recovery. A namespace mid-replay is skipped (no heal against a
    # half-restored registry) without blocking healing for ready namespaces.
    ready = RecoveryGate.ready_namespaces(intent)
    heal_operatives = Enum.filter(intent.operatives, &MapSet.member?(ready, &1.namespace))
    delta = Diff.compute(%Intent{operatives: heal_operatives}, reality)

    {streaks, delta} = apply_misbound_streak(state.misbound_streaks, delta)
    state = %{state | misbound_streaks: streaks}

    plugin_degraded = plugin_degraded_operatives(delta)
    maybe_log_plugin_degraded(plugin_degraded, state.plugin_degraded)
    state = %{state | plugin_degraded: plugin_degraded}

    plan = Planner.plan(delta, state.backoff)
    plan = inject_stall_rebinds(plan, stalled, intent, ready, state.backoff)

    {backoff, issued} = execute_plan(plan, state.backoff)

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

    classify_data_plane(state)
  end

  # Detection-only: localise a silent data-plane stall to one layer and emit a
  # single event when the verdict changes. This adds NO heal action — the
  # existing `:output_stall → rebind` path is untouched — and any error here is
  # swallowed so instrumentation can never break convergence.
  defp classify_data_plane(%__MODULE__{} = state) do
    if LivenessCounters.poll_enabled?() do
      current = build_liveness_snapshot()
      verdict = StallClassifier.classify(current, state.prev_liveness, %{})
      maybe_emit_stall(verdict, state.last_stall_verdict)
      %{state | prev_liveness: current, last_stall_verdict: verdict_key(verdict)}
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

  defp build_liveness_snapshot() do
    control_drivers =
      TransportServer.list_active_transports()
      |> Enum.flat_map(fn t ->
        case TransportServer.get_stats(t) do
          {:ok, %{driver: d}} when is_map(d) -> [Map.put(d, :transport, t)]
          _ -> []
        end
      end)

    # Per-executor data-stream servers (StreamAcceptor-owned) — the layer
    # that actually carries operative traffic, invisible to get_stats/1.
    data_drivers =
      LivenessCounters.driver_refs()
      |> Enum.flat_map(fn {{executor_id, type}, ref} ->
        case Flezha.Transport.Native.driver_stats(ref) do
          d when is_map(d) -> [Map.put(d, :transport, {:uds_data, executor_id, type})]
          _ -> []
        end
      end)

    %{channels: channels, operatives: operatives} = LivenessCounters.snapshot_all()
    %{drivers: control_drivers ++ data_drivers, channels: channels, operatives: operatives}
  end

  defp maybe_emit_stall(:ok, last) when last not in [nil, :ok] do
    Logger.info("[convergence] data-plane stall cleared (was #{inspect(last)})")
  end

  defp maybe_emit_stall(:ok, _last), do: :ok

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

  defp verdict_key(:ok), do: :ok
  defp verdict_key({layer, _ctx}), do: layer

  # Producing-liveness from durable progress markers (report-only; the
  # Watchdog's :output_stall → rebind path stays the heal). Reads the
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
        {sh, pk} = ctx.segment
        {id, %{detail: detail, segment: "#{sh}/#{pk}"}}
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

  # Per-Segment fed/advance evidence from the existing Watchdog/WalWriter
  # state. Cursor fields (:input_log_tail, :min_watermark, :out_pending_seq,
  # :outbox_flushed_seq) join these maps when the effectively-once tier
  # lands; their absence degrades classification to WAL-seq-only.
  defp progress_markers() do
    Flezha.State.Watchdog.segment_statuses()
    |> Enum.map(fn {{rc, sh, pk}, s} ->
      %{
        operative_id: rc,
        segment: {sh, pk},
        last_change_seen_at: s.last_change_seen_at,
        last_advance_at: s.last_advance_at
      }
    end)
  end

  defp tier_cursors?(m),
    do: is_integer(m[:min_watermark]) or is_integer(m[:outbox_flushed_seq])

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

    Enum.each(fresh, fn {id, %{detail: detail, segment: segment}} ->
      Logger.warning("[convergence] operative stuck: #{id} #{detail} segment=#{segment}")

      :telemetry.execute(
        [:flezha, :convergence, :operative_stuck],
        %{count: 1},
        %{operative_id: id, detail: detail, segment: segment}
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

    {evidence |> MapSet.difference(live) |> Enum.sort(), cache}
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

  # First-level state-root listing, re-read only when the root dir's mtime
  # changes (adding/removing an operative dir touches it; deeper writes do
  # not). A cache entry younger than 2 s is never trusted — POSIX mtime has
  # 1 s granularity, so a same-second add+remove would otherwise pin a stale
  # listing forever.
  defp state_root_dirs(cache) do
    root = Flezha.State.Registry.state_root()

    case File.stat(root, time: :posix) do
      {:ok, %File.Stat{mtime: mtime}} ->
        now_s = System.os_time(:second)

        case cache do
          {^mtime, dirs} when now_s - mtime > 1 ->
            {dirs, cache}

          _ ->
            dirs =
              case File.ls(root) do
                {:ok, entries} -> Enum.filter(entries, &File.dir?(Path.join(root, &1)))
                _ -> []
              end

            {dirs, {mtime, dirs}}
        end

      _ ->
        {[], nil}
    end
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

  defp expire_stalled(stalled, now, grace_ms) do
    :maps.filter(fn _operative, since -> now - since < grace_ms end, stalled)
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

  # Inject backoff-filtered rebind_operative ops for stalled operatives not already
  # covered by the plan. Uses args.reason :output_stall so Heal skips the
  # active-executor guard that would block a healthy-but-frozen operative.
  @doc false
  def inject_stall_rebinds_for_test(plan, stalled, intent, ready, backoff) do
    inject_stall_rebinds(plan, stalled, intent, ready, backoff)
  end

  defp inject_stall_rebinds(plan, stalled, %Intent{operatives: intent_operatives}, ready, backoff) do
    covered = MapSet.new(plan, fn op -> op.target end)
    ns_by_operative = Map.new(intent_operatives, fn r -> {r.operative_id, r.namespace} end)
    now = System.monotonic_time(:millisecond)

    stall_ops =
      stalled
      |> Map.keys()
      |> Enum.reject(&MapSet.member?(covered, &1))
      |> Enum.flat_map(fn operative_id ->
        case Map.get(ns_by_operative, operative_id) do
          nil ->
            []

          ns ->
            if MapSet.member?(ready, ns) do
              [
                %{
                  primitive: :rebind_operative,
                  namespace: ns,
                  target: operative_id,
                  args: %{reason: :output_stall}
                }
              ]
            else
              []
            end
        end
      end)
      |> Backoff.filter(backoff, &Planner.key/1, now)

    plan ++ stall_ops
  end

  defp execute_plan(plan, backoff) do
    now = System.monotonic_time(:millisecond)

    Enum.reduce(plan, {backoff, 0}, fn op, {acc_backoff, count} ->
      result = Heal.execute(op)
      {Backoff.update(acc_backoff, Planner.key(op), result, now), count + 1}
    end)
  end

  defp publish(%Intent{operatives: operatives}, reality, %__MODULE__{} = state) do
    # `intended` = declared operatives (what should be bound); `bound` = operatives
    # actually observed running on executors. After a restart the controller is
    # up but its operatives are not yet rebound (bound < intended) — that is a
    # recovery window, NOT healthy, even though nothing is "stalled".
    intended = length(operatives)
    bound = map_size(Map.get(reality, :operatives, %{}))
    stalled = Map.keys(state.stalled_operatives)
    stuck = Map.keys(state.stuck_operatives)
    non_producing = Enum.uniq(stalled ++ stuck ++ state.plugin_degraded)
    producing = max(bound - length(non_producing), 0)

    # After a restart the durable registry may hold operatives that are not yet
    # bound to a live pid — either the hub has not started (lazy on executor
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
      operative_health: operative_health_map(operatives, reality, state)
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
    active = Map.get(reality, :active_executors, %{})
    plugin_degraded = MapSet.new(state.plugin_degraded)

    Map.new(operatives, fn %{namespace: ns, operative_id: rid} ->
      obs = Map.get(observed, {ns, rid}, %{})
      bound = Map.get(obs, :bound_executor)
      live = Map.get(active, ns, MapSet.new())

      health =
        cond do
          # An observation failed this tick (operative state or live-executor read);
          # no authoritative signal, so report `:unknown` for this tick only.
          bound == :unknown -> :unknown
          is_nil(bound) -> :unbound
          live == :unknown -> :unknown
          # Bound to an executor that is no longer live — the loop will rebind.
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
