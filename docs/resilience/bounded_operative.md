# Bounded Operative — structure, lifecycle, saga & crash-resume

A **Bounded Operative** (kind `bounded`) is an on-demand, terminating,
possibly-interactive process — the counterpart to a Continuous Operative's
always-on streaming DAG. Where a continuous operative folds an input stream
forever, a bounded operative runs an **ordered workflow** to an exit trigger and
is then torn down.

This is the resilience-focused reference for the bounded model shipped by the
`bounded-operative-functionality` change. It covers the sequential structure,
the lifecycle + inbox, saga compensation, durable-by-default crash-resume, and
finished-job retention. Read it with [`data_durability.md`](data_durability.md)
(where state is lost vs. not) and [`state_persistence.md`](state_persistence.md)
(the shared WAL substrate both kinds use).

## Structure: sequential operations, parallel segments

A bounded operative authors the **same** Operations, Channels, and Segments a
continuous operative does — the same per-segment state engine — but its
operations are scheduled **sequentially**:

- **Linear chain** `Op1 → Op2 → … → OpN` (no branching in v1). Each operation's
  collected output is the next operation's input, handed over at a **count-based
  barrier**: an operation is *done* only when every one of its segments has
  finished. Because each stage chews a finite, known batch, "done" is a count,
  not a fuzzy idle guess.
- **Within one operation, segments run in parallel** exactly as in continuous
  (single / static segmenting, per-segment state + WAL).
- The first operation is **seeded from the spawn `args`** (`init_values`); later
  operations consume the previous stage's output.
- `run(ctx, args)` is **sugar** for a single-Operation, single-Segment bounded
  operative — the simplest case, and the only form with an interactive inbox
  loop (below).

The `{operative_class, operation_id, segment_key}` identity is the same one a
continuous operative uses — a bounded operative genuinely has operations and
segments, so there are **no synthetic sentinels**.

## Lifecycle: exit triggers, inbox, one teardown

A bounded operative is spawned on demand with `args` and terminates on the first
of:

1. **chain quiescence** — the last operation completed;
2. **`ctx.complete(result)`** — explicit completion with a result payload;
3. a **loop terminal predicate** — `ctx.loop_until(pred)` on the `run`-sugar body
   (the interactive/agent pattern); a loop MUST have a real, non-timeout exit and
   MUST NOT busy-spin (it blocks/sleeps while idle). An external-stop-only loop is
   **rejected at registration**;
4. a declared **`max_duration`** backstop elapsing (SDK cancellation token —
   Rust `Arc<AtomicBool>` + responsive sleep, Java interrupt/atomic);
5. an external **`flz operative stop`** — a cooperative `BoundedStop` (distinct
   from the hard `RemOperative`) that trips the cancellation token.

**Every trigger converges on the single `BoundedCompleted` teardown** (status
`ok | error | cancelled` + result/error payload). There is exactly one teardown
path; the convergence loop never acts on a terminal bounded operative.

**Inbox** — the `run`-sugar body may `recv()` input **after it starts** (a
blocking front door, no busy-spin), so interactive/long-lived patterns (e.g. an
AI session) work. A frame addressed to a running bounded instance is routed to
its inbox instead of being dropped. **1 operative = 1 job/session**; segments are
internal parallelism only, never multi-session hosting. Structured multi-op
chains are seeded by `args` and run once (inbox frames buffer harmlessly during
a structured run).

## Saga: compensation on abnormal termination

An operation MAY declare a **compensation hook** (`compensate(ctx,
completed_output, state)`) describing how to undo its committed effect. On
**abnormal** termination — an operation returns an error, or a cooperative stop
lands after ≥1 stage completed — the driver fires the completed operations'
hooks in **reverse order, once per segment** that had an effect, inside the
**same single teardown** (not a parallel path). Operations with no hook are
skipped; a clean `ok` **never** compensates.

Two author-selected rollback modes (`rollback_mode()` / `rollbackMode()`):

- **all-or-nothing (default)** — any failure undoes *all* completed operations
  in reverse, back to the start (classic saga: `reserve → charge → ship`, undo
  `charge` then `reserve` when `ship` fails).
- **keep-progress** — each finished loop iteration commits; a failure undoes only
  the in-flight iteration. The commit line is the loop iteration, never an
  individual operation. *(v1: the mode is plumbed through and planner-tested; the
  driver-level keep-iterations behaviour rides the deferred structured chain-loop
  and collapses to all-or-nothing for a single pass.)*

**Crash-safe compensation.** For a durable operative each completed compensable
operation records a **compensation-intent** marker and each finished
compensation records a **compensation-done** marker (per-Segment, on the shared
substrate). On respawn mid-compensation the driver **resumes the remaining
hooks** — it reconstructs the completed set from the shipped-back intent markers
and runs the outstanding undos in reverse, **skipping** any Segment whose done
marker already landed. It does **not** re-run the forward chain (which would redo
already-undone work, e.g. re-charge after a refund). Author contract: because a
crash-resumed `compensate` receives *empty* outputs (an operation's collected
inter-op output was never durable) and reports terminal status `error`, a durable
compensable operation must persist whatever its undo needs in **State** (which is
restored), and compensations must be idempotent.

## Durability: durable-by-default, crash-resume, at-least-once

A bounded operative class is **durable by default** and MAY **opt out** per class
(fire-and-forget) via `PERSISTENCE_ENABLED` / `persistenceEnabled()`, resolved at
start through `PersistenceResolver` (operator override available). When durable,
its operations/segments are WAL-backed on the **same substrate** as continuous
(`WalWriter` framing, fsync batching, retention) under the normal identity. When
opted out, the per-Segment flush **drains and discards** — no WAL, snapshot, or
state files.

**Crash-resume = Model A, idempotent re-run.** On an executor/controller restart
a durable bounded operative is respawned, its committed per-Segment state is
restored via the **existing bind-time restore path**, and the chain is **re-run**
— completed operations are idempotent no-ops over their restored state ("a step
may repeat"). The `run`-sugar form (no segments) re-runs from the top. A
**restore-before-run gate** (`AddOperative.resume_segment_count`, the count of
persisted Segments) holds the first operation until that many `RestoreInitState`
frames land, so the chain never runs on empty state before the restore arrives.
Execution is therefore **at-least-once** — steps and compensations must be
**idempotent by author contract**. An operation returning an **error** is
terminal (compensate + status `error`, **no** platform retry); this is distinct
from crash-resume.

The continuous-only seq-gap → resync + epoch-realign machinery stays on
`:continuous`: a resumed bounded Segment re-arms its `change_seq` to
`terminal + 1`, so post-resume appends are contiguous and no realign is needed.

### How a crashed bounded operative reaches a new executor

1. The executor process dies (SIGKILL / OOM / node death). Its WAL — written
   before the crash — survives on the controller.
2. A replacement executor connects. If it re-presents the **same durable stable
   identity** (`FLEZHA_EXECUTOR_ID_PATH`), the controller **swaps** the dead
   session's bindings onto it and evicts the dead session — on both UDS (prompt
   socket EOF) and QUIC (where the dead peer is otherwise invisible until its
   ~45 s idle timeout).
3. The operative's supervisor re-binds onto the replacement with a **fresh
   `Overwatch.complete_init`**, which stamps `resume_segment_count > 0` from the
   on-disk WAL and ships the persisted Segments (and any compensation markers)
   via `RestoreInitState`. In production this is driven by the convergence loop;
   the cross-language e2e drives it deterministically.
4. The SDK driver holds the first operation on the restore gate until the
   restores land, then re-runs the chain (or resumes compensation) to a terminal
   `BoundedCompleted`.

## Finished-job retention

On terminal teardown (`ok | error | cancelled`) a durable bounded operative's
data is **not deleted immediately**. The controller writes a small **completion
record** (`Flezha.State.Completion` — status, result/error, `finished_at`)
alongside the retained final state and starts a retention timer. The existing
`Flezha.State.Retention` sweeper reclaims both once `finished_at + window < now`
(default **3 days**, `:bounded_completed_retention_hours = 72`, per-class
override, env `FLEZHA_BOUNDED_RETENTION_HOURS`). During the window
`flz operative info <id>` surfaces the finished operative's terminal status,
result, and finish time. An opted-out (ephemeral) operative leaves no state to
retain; its completion record is best-effort only.

## Capability split (continuous behaviour preserved)

The coarse `persistent_state?` flag is split so continuous-only behaviours no
longer ride on it:

| Flag | Meaning | `:continuous` | `:bounded` |
|---|---|---|---|
| `persistent_state?` | durable per-Segment WAL + restore (crash-resume, state introspection) | always | durable-by-default (per-class opt-out) |
| `time_travel?` | point-in-time queries + retention beyond the latest snapshot (`flz state at`) | yes | no |
| `dynamic_segments?` | lazy per-key Segment creation + destructive idle eviction | yes | no |
| `output_stall_watch?` | per-Segment output-staleness watchdog → forced rebind | yes | no |

`persistent_state?` is resolved per kind **and class** (in `PersistenceResolver`,
because bounded's durability is a per-class opt-out); the other three are pure
per-kind flags (`Flezha.Operative.Kind`) keyed off `Flezha.Operative.ClassRegistry`
(operative_class → kind). An **unknown class defaults to `:continuous`**, so every
gate is only ever *subtractive* — continuous behaviour is byte-for-byte
unchanged, and a durable bounded operative gets durable state + restore **without**
inheriting time-travel, dynamic-eviction, or output-stall semantics it has no use
for.

## Operator notes

- **Crash-resume is at-least-once.** A non-idempotent fire-and-forget job (e.g.
  "send one email") can double-act on resume. Write it idempotently or **opt out
  of durability** so a crash re-runs from a clean slate rather than a partial one.
- **Deploy executors with a graceful stop, not a kill** — the same rule as
  continuous ([`data_durability.md`](data_durability.md)). A graceful stop lets a
  running bounded operative migrate; a hard kill forces the crash-resume path.
- **Retention grows disk for many small jobs.** Opt out for fire-and-forget, or
  tune `FLEZHA_BOUNDED_RETENTION_HOURS`. Bounded is not the high-throughput path.
- **End-to-end proof:** `test/system/bounded_crash_resume_e2e_test.exs` drives a
  real SIGKILL-mid-run (and SIGKILL-mid-compensation) crash-resume across Rust and
  Java over UDS and QUIC.
