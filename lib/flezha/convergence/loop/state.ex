defmodule Flezha.Convergence.Loop.State do
  @moduledoc """
  The convergence Loop's struct, and the two tick defaults its fields carry.

  Split out of `Flezha.Convergence.Loop` so the `Liveness`, `Progress` and
  `Summary` siblings can thread the same struct without naming the GenServer
  that owns it — a sibling naming its parent is a dependency cycle, and the
  struct is the one thing all four genuinely share.

  Field groups, in order: the two resolved intervals; the heal backoff; the
  per-operative streak and stall bookkeeping the tick carries between ticks;
  the data-plane and producing-liveness verdicts; and the intent-mismatch
  evidence cache.
  """

  alias Flezha.Convergence.Backoff

  @default_tick_ms 5_000
  @default_output_stall_grace_ms 60_000

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

  @type t :: %__MODULE__{}

  @doc "Tick interval when neither an option nor app config sets one."
  @spec default_tick_ms() :: pos_integer()
  def default_tick_ms, do: @default_tick_ms

  @doc "Output-stall grace when neither an option nor app config sets one."
  @spec default_output_stall_grace_ms() :: pos_integer()
  def default_output_stall_grace_ms, do: @default_output_stall_grace_ms
end
