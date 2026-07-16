# Bounded Operative — structure, lifecycle, saga & crash-resume

A **Bounded Operative** (kind `bounded`) is an on-demand, terminating,
possibly-interactive process — the counterpart to an Unbounded Operative's
always-on streaming DAG. Where an unbounded operative folds an input stream
forever, a bounded operative runs an **ordered workflow** to an exit trigger and
is then torn down.

This is the resilience-focused reference for the bounded model shipped by the
`bounded-operative-functionality` change. It covers the sequential structure,
the lifecycle + inbox, saga compensation, durable-by-default crash-resume, and
finished-job retention. Read it with [`data_durability.md`](data_durability.md)
(where state is lost vs. not) and [`state_persistence.md`](state_persistence.md)
(the shared WAL substrate both kinds use).

## Structure: a tree of operations, parallel segments

A bounded operative authors the **same** Operations, Channels, and Segments a
unbounded operative does — the same per-segment state engine — but its
operations are scheduled **topologically**:

- **Operation tree** — the linear chain `Op1 → Op2 → … → OpN` is the
  single-branch case; an operation MAY also **fan out**. Each operation's
  collected output is its downstream operations' input, handed over at a
  **count-based barrier**: an operation is *done* only when every one of its
  segments has finished. Because each stage chews a finite, known batch, "done"
  is a count, not a fuzzy idle guess.
- **Within one operation, segments run in parallel** exactly as in unbounded
  (single / static segmenting, per-segment state + WAL).
- The root operation is **seeded from the spawn `args`** (`init_values`); every
  other operation consumes its parent's output.
- `run(ctx, args)` is **sugar** for a single-Operation, single-Segment bounded
  operative — the simplest case, and the only form with an interactive inbox
  loop (below).

### Branching: fan-out, not fan-in

An **edge** is *(an operation's output channel, one of that channel's
consumers)*. An `Operation` has a single `Output` type and therefore one output
channel, so a fan-out is simply that channel declaring several consumers:

```rust
fn operations(c: &mut ChannelBuilder) {
    c.input::<Job>("in").into::<Extract>();
    // One channel, two consumers: Enrich and Archive are independent branches.
    c.channel::<Record>("records")
        .codec()
        .from::<Extract>()
        .into::<Enrich>()
        .into::<Archive>();
    c.channel::<Enriched>("out").from::<Enrich>().to_plugin("sink");
    c.channel::<Record>("cold").from::<Archive>().to_plugin("glacier");
}
```

Routing is **per channel**: a downstream operation receives exactly the values
released on the channel it consumes. Branches are **independent** by
construction — there is no cross-branch data exchange, and siblings that need
each other's output are one operation. Registration refuses anything that is not
a tree, naming the node:

| Shape | Refusal |
| --- | --- |
| Two channels feed one operation (**fan-in / join**) | `FanIn` |
| One operation produces two channels | `MultipleOutputChannels` |
| The entry channel feeds ≠ 1 operation | `EntryRoot` |
| An operation unreachable from the entry channel | `Disconnected` |
| `LoopMode::Chained` over a branching tree | `BranchingChainedLoop` |

**Scheduling.** A child becomes runnable only once its parent's commit is
acknowledged. Every currently-runnable node is submitted to the work-stealing
pool **together** — sibling branches are independent subtrees, so they interleave
freely — and joined on one barrier before the level commits. The operative is
`ok` exactly when every **leaf** operation has committed; the controller derives
the same fact from the journal, because the SDK declares its leaf set at
`AddOperative`.

A handoff-chained loop (`LoopMode::Chained`) seeds iteration N+1 from iteration
N's terminal batch, which a tree with several leaves does not define — hence the
`BranchingChainedLoop` refusal. `Once` and `PerInput` classes may branch freely;
one iteration is one pass of the whole tree.

The `{operative_class, operation_id, segment_key}` identity is the same one a
unbounded operative uses — a bounded operative genuinely has operations and
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
internal parallelism only, never multi-session hosting. A structured chain is
seeded by `args` and runs once, unless it declares a **loop** — see below, where
input becomes durable and an idle session stops costing anything.

## Durable loop and suspension

A structured class declares its chain a loop with `loop_mode()`:

| Mode | Each iteration is seeded by | Ends when |
|---|---|---|
| `Once` (default) | the spawn `args` | the chain completes — the single pass, unchanged |
| `PerInput` | the next inbox item (a chat turn) | cancellation / `max_duration`; **parks** while idle |
| `Chained` | the previous iteration's terminal handoff (refinement) | an iteration releases an empty batch |

**Iteration = a durable boundary.** Each iteration runs the whole chain under
the existing per-operation `OpCommit` gating and ends with an `IterCommit`: the
controller journals `iter_committed(N, consumed_inbox_offset)` with the
iteration's terminal handoff, snapshots the instance's segments at the boundary,
flushes what the iteration staged, and releases the consumed inbox prefix. A
crash resumes at the iteration **after** the last committed one — never at the
loop start, and never re-running a committed turn. Work of an uncommitted
in-flight iteration may re-run (at-least-once, the existing author contract).

Because committed iterations are journal-backed, `RollbackMode::KeepProgress`
finally keeps something: on an abnormal exit the planner is handed the journaled
iterations as `committed` and undoes only the in-flight one. `AllOrNothing`
unwinds the in-flight iteration and then every committed iteration in reverse.
The planner itself is unchanged.

**Durable inbox.** Post-start input for a durable bounded instance is appended
to a per-instance `inbox.wal` (offset + dedup id) **before** it is forwarded, so
input sent while the instance is suspended, crashed, or migrating is delivered
on rehydrate rather than dropped. See `data_durability.md`.

**Suspension (evict-while-waiting).** When a `PerInput` loop finds its inbox
empty at a committed boundary and stays idle past `FLEZHA_BOUNDED_PARK_IDLE_MS`
(default 5 s — hysteresis, so trickling input does not thrash), it tears its own
run down **without** a terminal completion and ships `IterPark`. The controller
journals `parked(N[, deadline])` and evicts the instance from its runner. The
registry entry and its Overwatch stay — an idle chat session holds **zero
runner resources** but is still addressable, and `flz operative info` reports
it `suspended`. The convergence loop leaves it alone: it is unbound by design,
not broken.

A new inbox append — or the journaled wake deadline, which the controller
delivers as a synthetic empty-payload inbox entry (`flz.wake = timer`, for
classes declaring `wake_after()`) — rehydrates it through the **normal bind
path**: respawn on any live runner, restore journal + iteration-boundary
snapshots, deliver the undelivered inbox tail, enter iteration N+1. Both wake
sources go through the inbox, so two racing wakes collapse into exactly one
rehydration. Because every input is durable (journal, snapshots, inbox), a
suspended instance is rediscovered from disk after a controller redeploy and
rehydrates exactly as if nothing had happened.

Inspect either half with `flz state journal <instance>` (per-op entries tagged
with their iteration, the `iter_committed` records with their consumed offsets,
and the final `parked` entry) and `flz state inbox <instance>` (offsets, and
which are still undelivered).

## Segment retry: the rung before the abort

A segment error inside a **structured** operation is not immediately fatal. The
failing segment is **retried in place** — default **3 attempts** (the first run
plus two retries), exponential backoff with jitter (base `retry_backoff()`,
default 500 ms, capped at 5 s) — while sibling segments that already finished
stay buffered at the barrier and are **not** re-run. A retry that succeeds lets
the stage quiesce and commit normally; only an exhausted ladder fails the
operation, and the original error then takes the unchanged abnormal path below
(no commit, compensation, status `error`).

- **Author contract.** A retried segment re-runs its whole batch for that
  operation, over the state its failed attempt already applied — the same
  at-least-once idempotency contract crash-resume imposes. A failed attempt's
  emits are discarded (they never reach a plugin), so only the successful
  attempt's output is staged.
- **Non-retryable errors.** `OperationError::non_retryable(..)` (Rust) skips the
  ladder: a permanent failure (validation error, 4xx-class response) fails the
  operation on its first occurrence. Every other error — including a panic, which
  the author never got to classify — is retryable.
- **Policy.** `retry_attempts()` / `retry_backoff()` on the class;
  `retry_attempts() = 0` restores fail-fast (the pre-ladder behaviour). A ladder
  declared with a zero backoff base is rejected at registration (it would re-run
  the failing segment in a hot loop).
- **Scope.** Structured segments only: a `run`-sugar body has no segment to scope
  a retry to and stays terminal-on-error. Retries always happen **before** the
  operation's `OpCommit`, so a committed operation is never retried; a crash
  mid-ladder converges on normal crash-resume of the uncommitted operation (the
  in-memory attempt counter resets).
- **Observability.** Each retry logs `instance / operation / segment / attempt /
  delay` and bumps the same retry counter the unbounded ladder feeds (shipped in
  `data_heartbeat` metadata), so flapping is visible before it escalates.

**Bounded vs unbounded escape ladders.** Both profiles retry a failing
invocation first; they differ in what happens when the retries run out. An
unbounded operative **parks** the poison payload to the
[dead-letter queue](data_durability.md#dead-letter-queue) and advances (the
stream must keep flowing). A bounded operative has no stream to advance: its
terminal escape is **AbortAll + saga** — compensate what completed and finish
with status `error`. Nothing bounded is ever written to the DLQ store.

## Rewind: taking a loop back to a committed iteration

A bad turn is not permanent. `flz operative rewind <instance> --to-iteration N`
takes a looping bounded operative back to the committed boundary of iteration
`N`: the operations of every iteration above it are compensated in reverse
(reverse iteration, reverse chain position), the inter-op handoff and the
consumed inbox offset return to `N`, the epoch is bumped, and the loop resumes
at `N + 1`.

The journal is append-only, so the rewind is a **marker**, not a truncation: one
`rewound(N)` entry, written *before* any compensation runs and carrying
`epoch + 1`. Every fold applies it — on `rewound(N)`, entries accumulated so far
with `iter > N` are purged, while entries appended after it (the undo records,
and the re-run) accumulate normally. So a rewound iteration is not merely skipped
on resume: **its unflushed outbox ceases to be owed**, and it can never leak.

Because the marker is durable first, a crash mid-rewind resumes *as a rewind*.
The outstanding undo is derived, not marked: it is the unwound operations minus
those that already have a `compensated` entry. Crash after undoing iteration 5
but before 4, and the resumed rewind undoes only 4.

**What a rewind does and does not undo.**

- **Plugin output** is undone by the operation's `compensate` hook, and the undo
  is journaled and flushed on the same transactional-outbox path as forward
  output.
- **Segment State** is returned to the boundary by those same hooks — the saga
  contract, in which an operation that needs its effect undone reverses its own
  State change. The engine does not truncate the state WAL. An operation that
  declares no hook leaves its State change in place, exactly as under an
  `AllOrNothing` crash rollback.
- **Input is not replayed.** A committed iteration's input is not retained past
  its commit (the durable inbox is truncated to the consumed offset at each
  boundary), so a rewind is *undo-and-continue*, not replay: the loop resumes
  waiting, and the next message it receives becomes iteration `N + 1`. This is
  the chat "rewind and say it differently" shape.

**The coverage guard.** A rewind is refused — before anything durable changes,
naming the channels — when an unwound operation staged plugin output and
declares no `compensate` hook. A bounded operative's staged output carries no
natural key, so whether a later emit would overwrite it depends on the sink
(Postgres upserts on the synthetic `<segment>/<seq>` key; Kafka without a message
key appends). The engine cannot know, so it insists on the one undo it *can*
guarantee: a semantic one. An operation that emits nothing needs no coverage,
which is the common case — turn state is pure.

**Refusals.** A target that has not committed is refused with the committed
range. Neither refusal touches the journal.

## Saga: compensation on abnormal termination

An operation MAY declare a **compensation hook** (`compensate(ctx,
completed_output, state)`) describing how to undo its committed effect. On
**abnormal** termination — an operation returns an error, or a cooperative stop
lands after ≥1 stage completed — the driver fires the completed operations'
hooks in **reverse topological order, once per segment** that had an effect,
inside the **same single teardown** (not a parallel path). Operations with no
hook are skipped; a clean `ok` **never** compensates.

**Reverse topological** means no operation is undone before its compensated
descendants; independent sibling subtrees unwind in an unspecified relative
order (the same rule per-segment compensation already follows). For a linear
chain it degenerates to the exact reverse of the chain. In an `A → (B, C)` tree
where a failure lands under `B` after all three completed, the rollback runs
`C` and `B` — in either order — and only then `A`:

```
        A                          undo C ─┐
      ↙   ↘        failure  ⇒              ├─▶ undo A
     B     C       under B      undo B ─┘
     ↓
   (fails)
```

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
its operations/segments are WAL-backed on the **same substrate** as unbounded
(`WalWriter` framing, fsync batching, retention) under the normal identity. When
opted out, the per-Segment flush **drains and discards** — no WAL, snapshot, or
state files.

**Crash-resume = journal-gated redo-vs-skip.** On a runner/controller restart
a durable bounded operative is respawned, its committed per-Segment state is
restored via the **existing bind-time restore path**, and forward execution
resumes **at the first operation not recorded in the op-journal**
(`bounded-op-journal`): at every operation barrier the Rust driver ships
`OpCommit` (op index, segments that ran, the inter-op handoff serialized through
the channel's byte codec) and blocks until the controller has fsynced the
`op_committed` entry into the instance's `op.journal`. On resume the journal
state rides `RestoreInitState.op_journal`; committed operations are **skipped,
not redone**, and the first uncommitted operation's batch is **seeded from the
journaled handoff** instead of the entry channel. Only the operation that was
mid-flight when the crash landed re-runs (Model A idempotent re-run applies to
it alone). The `run`-sugar form (no segments), opted-out classes, pre-journal
instances, and the Java SDK (parity follow-up) re-run from the top. A
**restore-before-run gate** (`AddOperative.resume_segment_count`, the count of
persisted Segments) holds the first operation until that many `RestoreInitState`
frames land, so the chain never runs on empty state before the restore arrives.
Execution is therefore **at-least-once** — steps and compensations must be
**idempotent by author contract**. An operation returning an **error** is
terminal (compensate + status `error`, **no** platform retry); this is distinct
from crash-resume.

**Inter-op channel codec.** Every inter-operation channel of a durable
structured class must declare a byte codec — `.codec()` for payloads with the
standard serde derives (postcard), `.codec_with(encode, decode)` otherwise —
so the handoff batch is journal-able; registration fails naming the codec-less
channel. An oversized handoff (above the WAL-watermark-scale journal frame
bound) fails the commit with a typed error: keep bulk data in Segment state
(already WAL-durable) and hand off references. Inspect a journal with
`flz state journal <operative/instance>`; the file shares the instance
lifecycle (finished-job retention, `flz state delete`, `flz reset`).

**Staged output + transactional outbox.** A durable structured operation's
plugin-bound output does not reach the wire as it is emitted: each segment
**stages** it, and it travels in the same `OpCommit` request as the handoff, so
`op_committed` carries progress AND output in one durable write. The controller
flushes it to the plugins only after that fsync, stamped with the deterministic
key `(flz.segment, flz.seq = <op_idx>.<seq_in_op>, flz.epoch)`, and records an
`outbox_flushed` entry. Consequences: a failed or aborted operation leaks
**nothing**; a crash between commit and flush re-drives the delivery with
identical keys on restore (at-least-once with stable keys — a dedup-capable sink
absorbs it exactly); an already-flushed op is never re-delivered. A
`compensate` hook's output rides the same path via a `compensated` entry, keyed
with a trailing `.1` seq component so an undo never collides with the forward
output it reverses. Full contract:
`docs/resilience/data_durability.md` "Bounded operatives: staged commit and the
transactional outbox".

**Compensation precedence on resume.** A shipped compensation-**done** marker
proves an undo pass was already in flight before the crash, so compensation
resume wins over journal-gated forward resume; intent markers alone (which
also accumulate during normal forward progress) do not. If a later operation
terminates the resumed run abnormally, the skipped (journaled) operations
still roll back — they are folded into the saga plan from their intent
markers.

The unbounded-only seq-gap → resync + epoch-realign machinery stays on
`:unbounded`: a resumed bounded Segment re-arms its `change_seq` to
`terminal + 1`, so post-resume appends are contiguous and no realign is needed.

### How a crashed bounded operative reaches a new runner

1. The runner process dies (SIGKILL / OOM / node death). Its WAL — written
   before the crash — survives on the controller.
2. A replacement runner connects. If it re-presents the **same durable stable
   identity** (`FLEZHA_RUNNER_ID_PATH`), the controller **swaps** the dead
   session's bindings onto it and evicts the dead session — on both UDS (prompt
   socket EOF) and QUIC (where the dead peer is otherwise invisible until its
   ~45 s idle timeout).
3. The operative's supervisor re-binds onto the replacement with a **fresh
   `Overwatch.complete_init`**, which stamps `resume_segment_count > 0` from the
   on-disk WAL and ships the persisted Segments (and any compensation markers)
   via `RestoreInitState`. In production this is driven by the convergence loop;
   the cross-language e2e drives it deterministically.
4. The SDK driver holds the first operation on the restore gate until the
   restores land, then resumes the chain past the journaled operations (or
   resumes compensation) to a terminal `BoundedCompleted`.

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

## Capability split (unbounded behaviour preserved)

The coarse `persistent_state?` flag is split so unbounded-only behaviours no
longer ride on it:

| Flag | Meaning | `:unbounded` | `:bounded` |
|---|---|---|---|
| `persistent_state?` | durable per-Segment WAL + restore (crash-resume, state introspection) | always | durable-by-default (per-class opt-out) |
| `time_travel?` | point-in-time queries + retention beyond the latest snapshot (`flz state at`) | yes | no |
| `dynamic_segments?` | lazy per-key Segment creation + destructive idle eviction | yes | no |
| `output_stall_watch?` | per-Segment output-staleness watchdog → forced rebind | yes | no |

`persistent_state?` is resolved per kind **and class** (in `PersistenceResolver`,
because bounded's durability is a per-class opt-out); the other three are pure
per-kind flags (`Flezha.Operative.Kind`) keyed off `Flezha.Operative.ClassRegistry`
(operative_class → kind). An **unknown class defaults to `:unbounded`**, so every
gate is only ever *subtractive* — unbounded behaviour is byte-for-byte
unchanged, and a durable bounded operative gets durable state + restore **without**
inheriting time-travel, dynamic-eviction, or output-stall semantics it has no use
for.

## Operator notes

- **Crash-resume is at-least-once.** A non-idempotent fire-and-forget job (e.g.
  "send one email") can double-act on resume. Write it idempotently or **opt out
  of durability** so a crash re-runs from a clean slate rather than a partial one.
- **Deploy runners with a graceful stop, not a kill** — the same rule as
  unbounded ([`data_durability.md`](data_durability.md)). A graceful stop lets a
  running bounded operative migrate; a hard kill forces the crash-resume path.
- **Retention grows disk for many small jobs.** Opt out for fire-and-forget, or
  tune `FLEZHA_BOUNDED_RETENTION_HOURS`. Bounded is not the high-throughput path.
- **End-to-end proof:** `test/system/bounded_crash_resume_e2e_test.exs` drives a
  real SIGKILL-mid-run (and SIGKILL-mid-compensation) crash-resume across Rust and
  Java over UDS and QUIC.
