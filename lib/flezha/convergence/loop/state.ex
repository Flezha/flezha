defmodule Flezha.Convergence.Loop.State do
  @moduledoc """
  The convergence Loop's struct, and the two tick defaults its fields carry.

  Split out of `Flezha.Convergence.Loop` so the `Liveness`, `Progress` and
  `Summary` siblings can thread the same struct without naming the GenServer
  that owns it — a sibling naming its parent is a dependency cycle, and the
  struct is the one thing all four genuinely share.

  Field groups, in order: the two resolved intervals and the tick timer; the
  heal backoff; the declared facts and the per-relay streak the tick
  carries between ticks; the data-plane and producing-liveness verdicts; and
  the intent-mismatch evidence cache.
  """

  alias Flezha.Config.Durations
  alias Flezha.Config.Backoff

  defstruct tick_ms: Durations.get(:convergence_tick_ms),
            # Grace after which a declared fact nothing re-declares ages out,
            # and the STUCK grace of the producing-liveness classifier.
            fact_grace_ms: Durations.get(:convergence_output_stall_grace_ms),
            # The periodic tick's timer, and whether an immediate one is
            # already on its way — declarations arriving before it runs fold
            # into that one tick.
            timers: %{},
            tick_pending?: false,
            backoff: Backoff.new(:heal),
            # The backoff keys of the ops last tick that found their
            # precondition unmet: planned after every other op this tick, so
            # ops that keep waiting cannot hold the per-tick cap for ever.
            unmet: MapSet.new(),
            # {namespace, relay_id} => the reason the safety-net start found
            # no runner for it, kept only to log each reason's transitions.
            unstartable: %{},
            # {kind, target} => %{at, args}: everything a site declared through
            # `Flezha.Convergence.Facts.declare/1` that is not yet repaired.
            facts: %{},
            # {namespace, relay_id} => consecutive ticks observed misbound.
            misbound_streaks: %{},
            # relay_ids whose declared plugins were missing or self-reported
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
            # Per-runner liveness facts read from the same two samples the
            # classifier compares: the two silences side by side, and whether
            # heartbeats and dispatches are still advancing.
            runner_facts: %{},
            # subject => %Flezha.Convergence.Verdict{} from the last tick. The
            # one health answer every surface reads, and what lets a state that
            # holds keep its `since_ms` instead of being re-stamped every tick.
            verdicts: %{},
            # Namespaces whose hub has no quorum on this node this tick: the
            # other side of the split runs their relays, so the loop heals none
            # of them and /healthz reports `no_quorum`.
            no_quorum: [],
            # relay_id => %{detail:, cell:} producing-liveness STUCK
            # verdicts (fed but progress markers flat past grace). Report-only:
            # healing stays on the output-stall ladder.
            stuck_relays: %{},
            # [{taken_at_ms, tier-cursor markers}] — grace-window baselines for
            # the classifier's cursor-delta checks. Holds only Cells with
            # effectively-once tier cursors (empty until that tier lands).
            progress_history: [],
            # Relays with durable state evidence but no live intent — the
            # wiped-registry witness /healthz degrades on.
            intent_mismatch: [],
            # relay_id => monotonic ms it was first seen with evidence and no
            # intent; it joins `intent_mismatch` past the unmet-intent grace.
            intent_mismatch_since: %{},
            # {namespace, relay_id} => monotonic ms a declared relay was first
            # seen running nowhere; past the unmet-intent grace the Diff names it.
            unplaced_since: %{},
            # Whether one placement scan has read every namespace since the
            # loop started; later ones read only those a fact names.
            unplaced_scanned?: false,
            # {state-root mtime, first-level dir listing} cache.
            evidence_cache: nil,
            # Monotonic ms when `bound < intended` first held; nil while met or
            # recovering. healthz escalates recovering → degraded past its grace.
            unmet_intent_since: nil

  @type t :: %__MODULE__{}

  @doc "Tick interval when neither an option nor app config sets one."
  @spec default_tick_ms() :: pos_integer()
  def default_tick_ms, do: Durations.get(:convergence_tick_ms)
end
