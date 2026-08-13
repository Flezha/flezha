# Workflow Operative — structure, lifecycle, saga & crash-resume

A **Workflow Operative** (kind `workflow`) is an on-demand, terminating,
possibly-interactive process — the counterpart to a Stream Operative's
always-on streaming DAG. Where a stream operative folds an input stream
forever, a workflow operative runs an **ordered workflow** to an exit trigger and
is then torn down.

This is the resilience-focused reference for the workflow model shipped by the
`workflow-operative-functionality` change. It covers the sequential structure,
the lifecycle + inbox, saga compensation, durable-by-default crash-resume, and
finished-job retention. Read it with [`data_durability.md`](data_durability.md)
(where state is lost vs. not) and [`state_persistence.md`](state_persistence.md)
(the shared WAL substrate both kinds use).

## Structure: a DAG of procedures, parallel entities

A workflow operative authors the **same** Procedures, Channels, and Entities a
stream operative does — the same per-entity state engine — but its
procedures are scheduled **topologically**:

- **Procedure DAG** — the linear chain `Op1 → Op2 → … → ProcN` is the
  single-branch case; a procedure MAY also **fan out**, and several procedures
  MAY feed one (**fan-in**). Each procedure's collected output is its
  downstream procedures' input, handed over at a **count-based barrier**: an
  procedure is *done* only when every one of its entities has finished. Because
  each stage chews a finite, known batch, "done" is a count, not a fuzzy idle
  guess.
- **Within one procedure, entities run in parallel** exactly as in stream
  (single / static entity mode, per-entity state + WAL).
- The root procedure is **seeded from the spawn `args`** (`init_values`); every
  other procedure consumes its producers' output.
- `run(ctx, args)` is **sugar** for a single-Procedure, single-Entity workflow
  operative — the simplest case, and the only form with an interactive inbox
  loop (below).

### Branching: fan-out and fan-in on a static DAG

An **edge** is *(a procedure's output channel, one of that channel's
consumers)*. A `Procedure` has a single `Output` type and therefore one output
channel, so a fan-out is one stream handle wired twice, and a join is two
streams naming the same node (`sdk-graph-authoring`):

```rust
fn procedures(c: &ChannelBuilder) {
    let records = c
        .input::<Job>("in")
        .into::<Extract>()
        .named("records")
        .codec();
    // One channel, two consumers: Enrich and Archive are independent branches.
    records.into::<Enrich>().named("out").output("sink");
    records.into::<Archive>().named("cold").output("glacier");
}
```

Routing is **per channel**: a downstream procedure receives exactly the values
released on the channels it consumes. A **join** — two streams wiring the same
(unnamed) class — receives the **accumulated** contributions of all its
producers: each producer's released batch is appended to the join's input
batch, never overwritten, and the join is dispatched exactly **once**, only
after every one of its producers has committed. Registration refuses anything
outside the shared graph-shape rule, naming the node:

| Shape | Refusal |
| --- | --- |
| A procedure's output reaches its own input (**cycle**) | `Cycle` (via the shared topology validation) |
| One procedure produces two channels | `MultipleOutputChannels` (structurally unreachable on the stream-handle builder; kept as defense) |
| The entry channel feeds ≠ 1 procedure | `EntryRoot` |
| A procedure unreachable from the entry channel | `Disconnected` |
| `LoopMode::Chained` over a graph with several leaves | `BranchingChainedLoop` |

**Scheduling.** A child becomes runnable only once **every** producer feeding
it has its commit acknowledged — the existing rule, of which a join is just the
several-producer case. Validation orders procedures with a Kahn topological
sort (for a tree this is exactly the former BFS numbering, so journaled
`proc_idx` values keep their meaning), every currently-runnable node is submitted
**together** — one step activation per entity partition, sibling branches are
independent, so they interleave freely — and joined on one barrier before the
level commits.

**Where a stage runs.** A step is an activation the SDK's own workers pick up
(`flz-poll-*` threads in Rust, virtual threads in Java), never a core-owned
thread, in both languages: the driver submits on the poll core's direct queue —
served ahead of entity work and exempt from the jobs-underway cap, so the work
a driver is blocked on is never queued behind work that blocks — and collects
the answers at the barrier it already owns. A Java step therefore costs ONE
thread, not the two it used to (a core thread parked on the crossing plus the
Java thread running it).

The operative is `ok` exactly when every **leaf** procedure has committed; the
controller derives the same fact from the journal, because the SDK declares its
leaf set at `AddOperative`.

**Fan-in and crash-resume.** A join with one committed and one uncommitted
producer resumes with the committed producer's **journaled** handoff banked and
the resumed producer's fresh output appended — the committed branch never
re-runs. Compensation stays reverse-topological: a join is undone **before**
any of its producers, and a shared root last.

A handoff-chained loop (`LoopMode::Chained`) seeds iteration N+1 from iteration
N's terminal batch, which a graph with several leaves does not define — hence
the `BranchingChainedLoop` refusal (a diamond converging on one terminal is
fine). `Once` and `PerInput` classes may branch freely; one iteration is one
pass of the whole graph.

The `{operative_class, procedure_id, entity_key}` identity is the same one a
stream operative uses — a workflow operative genuinely has procedures and
entities, so there are **no synthetic sentinels**.

## Lifecycle: exit triggers, inbox, one teardown

A workflow operative is spawned on demand with `args` — from the CLI
(`flz operative start`), from a schedule (`flz schedule`), or from inside the
cluster by a running procedure (`ctx.spawn_workflow`; see
`docs/procedure_requested_spawn.md`, which spawns-or-delivers on one instance id
and inherits everything below unchanged) — and terminates on the first of:

1. **chain quiescence** — the last procedure completed;
2. **`ctx.complete(result)`** — explicit completion with a result payload;
3. a **loop terminal predicate** — `ctx.loop_until(pred)` on the `run`-sugar body
   (the interactive/agent pattern); a loop MUST have a real, non-timeout exit and
   MUST NOT busy-spin (it blocks/sleeps while idle). An external-stop-only loop is
   **rejected at registration**;
4. a declared **`max_duration`** backstop elapsing (SDK cancellation token —
   Rust `Arc<AtomicBool>` + responsive sleep, Java interrupt/atomic);
5. an external **`flz operative stop`** — a cooperative `WorkflowStop` (distinct
   from the hard `RemOperative`) that trips the cancellation token.

**Every trigger converges on the single `WorkflowCompleted` teardown** (status
`ok | error | cancelled` + result/error payload). There is exactly one teardown
path; the convergence loop never acts on a terminal workflow operative.

**Inbox** — the `run`-sugar body may `recv()` input **after it starts** (a
blocking front door, no busy-spin), so interactive/long-lived patterns (e.g. an
AI session) work. A frame addressed to a running workflow instance is routed to
its inbox instead of being dropped. **1 operative = 1 job/session**; entities are
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
the existing per-procedure `ProcCommit` gating and ends with an `IterCommit`: the
controller journals `iter_committed(N, consumed_inbox_offset)` with the
iteration's terminal handoff, snapshots the instance's entities at the boundary,
flushes what the iteration staged, and releases the consumed inbox prefix. A
crash resumes at the iteration **after** the last committed one — never at the
loop start, and never re-running a committed turn. Work of an uncommitted
in-flight iteration may re-run (at-least-once, the existing author contract).

Because committed iterations are journal-backed, `RollbackMode::KeepProgress`
finally keeps something: on an abnormal exit the planner is handed the journaled
iterations as `committed` and undoes only the in-flight one. `AllOrNothing`
unwinds the in-flight iteration and then every committed iteration in reverse.
The planner itself is unchanged.

**Durable inbox.** Post-start input for a durable workflow instance is appended
to a per-instance `inbox.wal` (offset + dedup id) **before** it is forwarded, so
input sent while the instance is suspended, crashed, or migrating is delivered
on rehydrate rather than dropped. See `data_durability.md`.

**Suspension (evict-while-waiting).** When a `PerInput` loop finds its inbox
empty at a committed boundary and stays idle past `FLEZHA_WORKFLOW_PARK_IDLE_MS`
(default 5 s — hysteresis, so trickling input does not thrash), it tears its own
run down **without** a terminal completion and ships `IterPark`. The controller
journals `parked(N[, deadline])` and evicts the instance from its runner. The
registry entry and its Overwatch stay — an idle chat session holds **zero
runner resources** but is still addressable, and `flz operative info` reports
it `suspended`. The convergence loop leaves it alone: it is unbound by design,
not broken.

The namespace hub **outlives its runner** as long as it holds registered
operatives: runner cleanup and cross-node migration skip `stop_hub` while any
suspended instance remains in the registry (only an explicit namespace teardown
tears it down). So killing or restarting the sole runner of a parked fleet does
not strand it — the hub, the Overwatches and the durable registry survive, and
the instances rehydrate onto the next runner that binds.

A new inbox append — or the journaled wake deadline, which the controller
delivers as a synthetic empty-payload inbox entry (`flz.wake = timer`, for
classes declaring `wake_after()`) — rehydrates it through the **normal bind
path**: respawn on any live runner, restore journal + iteration-boundary
snapshots, deliver the undelivered inbox tail, enter iteration N+1. Both wake
sources go through the inbox, so two racing wakes collapse into exactly one
rehydration. Because every input is durable (journal, snapshots, inbox), a
suspended instance is rediscovered from disk after a controller redeploy and
rehydrates exactly as if nothing had happened.

Inspect either half with `flz state journal <instance>` (per-proc entries tagged
with their iteration, the `iter_committed` records with their consumed offsets,
and the final `parked` entry) and `flz state inbox <instance>` (offsets, and
which are still undelivered).

**Known limitation — removing a suspended instance's type.** A parked/suspended
durable workflow instance is evicted from the SDK's live-instance registry, so
`removeWorkflow` / `remove_workflow` for its factory cannot see it: the SDK-side
live-instance check passes and the removal succeeds. The suspended instance
survives on the controller, but its later rehydration fails placement — no
runner advertises the factory anymore — and stays failed until a runner
re-defines the type. FleetManager logs a warning when an `UpdateFactories`
replacement drops a factory that still has controller-known operatives for that
runner; the warning is the tripwire, and convergence keeps surfacing the failed
placement. A blocking policy (refusing the removal while suspended instances
exist) is deliberately future work.

## Entity retry: the rung before the abort

An entity error inside a **structured** procedure is not immediately fatal. The
failing entity is **retried in place** — default **3 attempts** (the first run
plus two retries), exponential backoff with jitter (base `retry_backoff()`,
default 500 ms, capped at 5 s) — while sibling entities that already finished
stay buffered at the barrier and are **not** re-run. A retry that succeeds lets
the stage quiesce and commit normally; only an exhausted ladder fails the
procedure, and the original error then takes the unchanged abnormal path below
(no commit, compensation, status `error`).

- **Author contract.** A retried entity re-runs its whole batch for that
  procedure, over the state its failed attempt already applied — the same
  at-least-once idempotency contract crash-resume imposes. A failed attempt's
  emits are discarded (they never reach a plugin), so only the successful
  attempt's output is staged.
- **Non-retryable errors.** `ProcedureError::non_retryable(..)` (Rust) skips the
  ladder: a permanent failure (validation error, 4xx-class response) fails the
  procedure on its first occurrence. Every other error — including a panic, which
  the author never got to classify — is retryable.
- **Policy.** `retry_attempts()` / `retry_backoff()` on the class;
  `retry_attempts() = 0` restores fail-fast (the pre-ladder behaviour). A ladder
  declared with a zero backoff base is rejected at registration (it would re-run
  the failing entity in a hot loop).
- **Scope.** Structured entities only: a `run`-sugar body has no entity to scope
  a retry to and stays terminal-on-error. Retries always happen **before** the
  procedure's `ProcCommit`, so a committed procedure is never retried; a crash
  mid-ladder converges on normal crash-resume of the uncommitted procedure (the
  in-memory attempt counter resets).
- **The backoff holds no worker.** A step activation runs ONE attempt and
  releases the worker that ran it; the ladder — attempt count, next-attempt
  time, the failure it is climbing away from — lives on the driver, which times
  the re-submission at the barrier it is already blocked on (waking at the same
  ≤200 ms granularity, so an external stop or `max_duration` still cuts a wait
  short and takes the abnormal path). Nothing sleeps on the SDK's threads, so a
  stage whose entities outnumber the workers backs them all off at once.
- **Observability.** Each retry logs `instance / procedure / entity / attempt /
  delay` and bumps the same retry counter the stream ladder feeds (shipped in
  `data_heartbeat` metadata), so flapping is visible before it escalates.

**Workflow vs stream escape ladders.** Both profiles retry a failing
invocation first; they differ in what happens when the retries run out. An
stream operative **parks** the poison payload to the
[dead-letter queue](data_durability.md#dead-letter-queue) and advances (the
stream must keep flowing). A workflow operative has no stream to advance: its
terminal escape is **AbortAll + saga** — compensate what completed and finish
with status `error`. Nothing workflow is ever written to the DLQ store.

## Rewind: taking a loop back to a committed iteration

A bad turn is not permanent. `flz operative rewind <instance> --to-iteration N`
takes a looping workflow operative back to the committed boundary of iteration
`N`: the procedures of every iteration above it are compensated in reverse
(reverse iteration, reverse chain position), the inter-proc handoff and the
consumed inbox offset return to `N`, the epoch is bumped, and the loop resumes
at `N + 1`.

The journal is append-only, so the rewind is a **marker**, not a truncation: one
`rewound(N)` entry, written *before* any compensation runs and carrying
`epoch + 1`. Every fold applies it — on `rewound(N)`, entries accumulated so far
with `iter > N` are purged, while entries appended after it (the undo records,
and the re-run) accumulate normally. So a rewound iteration is not merely skipped
on resume: **its unflushed outbox ceases to be owed**, and it can never leak.

Because the marker is durable first, a crash mid-rewind resumes *as a rewind*.
The outstanding undo is derived, not marked: it is the unwound procedures minus
those that already have a `compensated` entry. Crash after undoing iteration 5
but before 4, and the resumed rewind undoes only 4.

**What a rewind does and does not undo.**

- **Plugin output** is undone by the procedure's `compensate` hook, and the undo
  is journaled and flushed on the same transactional-outbox path as forward
  output.
- **Entity State** is returned to the boundary by those same hooks — the saga
  contract, in which a procedure that needs its effect undone reverses its own
  State change. The engine does not truncate the state WAL. A procedure that
  declares no hook leaves its State change in place, exactly as under an
  `AllOrNothing` crash rollback.
- **Input is not replayed.** A committed iteration's input is not retained past
  its commit (the durable inbox is truncated to the consumed offset at each
  boundary), so a rewind is *undo-and-continue*, not replay: the loop resumes
  waiting, and the next message it receives becomes iteration `N + 1`. This is
  the chat "rewind and say it differently" shape.

**The coverage guard.** A rewind is refused — before anything durable changes,
naming the channels — when an unwound procedure staged plugin output and
declares no `compensate` hook. A workflow operative's staged output carries no
natural key, so whether a later emit would overwrite it depends on the sink
(Postgres upserts on the synthetic `<entity>/<seq>` key; Kafka without a message
key appends). The engine cannot know, so it insists on the one undo it *can*
guarantee: a semantic one. A procedure that emits nothing needs no coverage,
which is the common case — turn state is pure.

**Refusals.** A target that has not committed is refused with the committed
range. Neither refusal touches the journal.

## Saga: compensation on abnormal termination

A procedure MAY declare a **compensation hook** (`compensate(ctx,
completed_output, state)`) describing how to undo its committed effect. On
**abnormal** termination — a procedure returns an error, or a cooperative stop
lands after ≥1 stage completed — the driver fires the completed procedures'
hooks in **reverse topological order, once per entity** that had an effect,
inside the **same single teardown** (not a parallel path). Procedures with no
hook are skipped; a clean `ok` **never** compensates.

**Reverse topological** means no procedure is undone before its compensated
descendants — in particular a **join is undone before any of its producers** —
while independent sibling subtrees unwind in an unspecified relative order (the
same rule per-entity compensation already follows). For a linear chain it
degenerates to the exact reverse of the chain. In an `A → (B, C)` tree where a
failure lands under `B` after all three completed, the rollback runs `C` and
`B` — in either order — and only then `A`:

```
        A                          undo C ─┐
      ↙   ↘        failure  ⇒              ├─▶ undo A
     B     C       under B      undo B ─┘
     ↓
   (fails)
```

Two author-selected rollback modes (`rollback_mode()` / `rollbackMode()`):

- **all-or-nothing (default)** — any failure undoes *all* completed procedures
  in reverse, back to the start (classic saga: `reserve → charge → ship`, undo
  `charge` then `reserve` when `ship` fails).
- **keep-progress** — each finished loop iteration commits; a failure undoes only
  the in-flight iteration. The commit line is the loop iteration, never an
  individual procedure. *(v1: the mode is plumbed through and planner-tested; the
  driver-level keep-iterations behaviour rides the deferred structured chain-loop
  and collapses to all-or-nothing for a single pass.)*

**Crash-safe compensation.** For a durable operative each completed compensable
procedure records a **compensation-intent** marker and each finished
compensation records a **compensation-done** marker (per-Entity, on the shared
substrate). On respawn mid-compensation the driver **resumes the remaining
hooks** — it reconstructs the completed set from the shipped-back intent markers
and runs the outstanding undos in reverse, **skipping** any Entity whose done
marker already landed. It does **not** re-run the forward chain (which would redo
already-undone work, e.g. re-charge after a refund). Author contract: because a
crash-resumed `compensate` receives *empty* outputs (a procedure's collected
inter-proc output was never durable) and reports terminal status `error`, a durable
compensable procedure must persist whatever its undo needs in **State** (which is
restored), and compensations must be idempotent.

## Durability: durable-by-default, crash-resume, at-least-once

A workflow operative class is **durable by default** and MAY **opt out** per class
(fire-and-forget) via `PERSISTENCE_ENABLED` / `persistenceEnabled()`, resolved at
start through `PersistenceResolver` (operator override available). When durable,
its procedures/entities are WAL-backed on the **same substrate** as stream
(`WalWriter` framing, fsync batching, retention) under the normal identity. When
opted out, the per-Entity flush **drains and discards** — no WAL, snapshot, or
state files.

**Crash-resume = journal-gated redo-vs-skip.** On a runner/controller restart
a durable workflow operative is respawned, its committed per-Entity state is
restored via the **existing bind-time restore path**, and forward execution
resumes **at the first procedure not recorded in the proc-journal**
(`workflow-proc-journal`): at every procedure barrier the Rust driver ships
`ProcCommit` (proc index, entities that ran, the inter-proc handoff serialized through
the channel's byte codec) and blocks until the controller has fsynced the
`proc_committed` entry into the instance's `proc.journal`. On resume the journal
state rides `AddOperative.proc_journal` — the **bind**, not a per-Entity
restore, because the journal is instance-scoped: a class whose procedures hold
no per-Entity state has no Entity WAL and so receives no `RestoreInitState`
at all, and a journal delivered there would never arrive (every wake would
resume at iteration 0, and each turn's staged output would collide with
iteration 0's `outbox_flushed` marker and never be delivered). Committed
procedures are **skipped, not redone**, and the first uncommitted procedure's
batch is **seeded from the journaled handoff** instead of the entry channel.
Only the procedure that was mid-flight when the crash landed re-runs (Model A
idempotent re-run applies to it alone). The `run`-sugar form (no entities) and
opted-out classes re-run from the top. A
**restore-before-run gate** (`AddOperative.resume_entity_count`, the count of
persisted Entities) holds the first procedure until that many `RestoreInitState`
frames land, so the chain never runs on empty state before the restore arrives.
Execution is therefore **at-least-once** — steps and compensations must be
**idempotent by author contract**. A procedure returning an **error** is
terminal (compensate + status `error`, **no** platform retry); this is distinct
from crash-resume.

**Inter-proc channel codec.** Every inter-procedure channel of a durable
structured class must declare a byte codec — `.codec()` for payloads with the
standard serde derives (postcard), `.codec_with(encode, decode)` otherwise —
so the handoff batch is journal-able; registration fails naming the codec-less
channel. An oversized handoff (above the WAL-watermark-scale journal frame
bound) fails the commit with a typed error: keep bulk data in Entity state
(already WAL-durable) and hand off references. Inspect a journal with
`flz state journal <operative/instance>`; the file shares the instance
lifecycle (finished-job retention, `flz state delete`, `flz reset`).

**Staged output + transactional outbox.** A durable structured procedure's
plugin-bound output does not reach the wire as it is emitted: each entity
**stages** it, and it travels in the same `ProcCommit` request as the handoff, so
`proc_committed` carries progress AND output in one durable write. The controller
flushes it to the plugins only after that fsync, stamped with the deterministic
key `(flz.entity, flz.seq = <proc_idx>.<seq_in_proc>, flz.epoch)`, and records an
`outbox_flushed` entry. Consequences: a failed or aborted procedure leaks
**nothing**; a crash between commit and flush re-drives the delivery with
identical keys on restore (at-least-once with stable keys — a dedup-capable sink
absorbs it exactly); an already-flushed proc is never re-delivered. A
`compensate` hook's output rides the same path via a `compensated` entry, keyed
with a trailing `.1` seq component so an undo never collides with the forward
output it reverses. Full contract:
`docs/resilience/data_durability.md` "Workflow operatives: staged commit and the
transactional outbox".

**Compensation precedence on resume.** A shipped compensation-**done** marker
proves an undo pass was already in flight before the crash, so compensation
resume wins over journal-gated forward resume; intent markers alone (which
also accumulate during normal forward progress) do not. If a later procedure
terminates the resumed run abnormally, the skipped (journaled) procedures
still roll back — they are folded into the saga plan from their intent
markers.

The stream-only seq-gap → resync + epoch-realign machinery stays on
`:stream`: a resumed workflow Entity re-arms its `change_seq` to
`terminal + 1`, so post-resume appends are contiguous and no realign is needed.

### How a crashed workflow operative reaches a new runner

1. The runner process dies (SIGKILL / OOM / node death). Its WAL — written
   before the crash — survives on the controller.
2. A replacement runner connects. If it re-presents the **same durable stable
   identity** (`FLEZHA_RUNNER_ID_PATH`), the controller **swaps** the dead
   session's bindings onto it and evicts the dead session — on both UDS (prompt
   socket EOF) and QUIC (where the dead peer is otherwise invisible until its
   ~45 s idle timeout).
3. The operative's supervisor re-binds onto the replacement with a **fresh
   `Overwatch.complete_init`**, which stamps `resume_entity_count > 0` from the
   on-disk WAL and ships the persisted Entities (and any compensation markers)
   via `RestoreInitState`. In production this is driven by the convergence loop;
   the cross-language e2e drives it deterministically.
4. The SDK driver holds the first procedure on the restore gate until the
   restores land, then resumes the chain past the journaled procedures (or
   resumes compensation) to a terminal `WorkflowCompleted`.

## Finished-job retention

On terminal teardown (`ok | error | cancelled`) a durable workflow operative's
data is **not deleted immediately**. The controller writes a small **completion
record** (`Flezha.State.Completion` — status, result/error, `finished_at`)
alongside the retained final state and starts a retention timer. The existing
`Flezha.State.Retention` sweeper reclaims both once `finished_at + window < now`
(default **3 days**, `:workflow_completed_retention_hours = 72`, per-class
override, env `FLEZHA_WORKFLOW_RETENTION_HOURS`). During the window
`flz operative info <id>` surfaces the finished operative's terminal status,
result, and finish time. An opted-out (ephemeral) operative leaves no state to
retain; its completion record is best-effort only.

## Capability split (stream behaviour preserved)

The coarse `persistent_state?` flag is split so stream-only behaviours no
longer ride on it:

| Flag | Meaning | `:stream` | `:workflow` |
|---|---|---|---|
| `persistent_state?` | durable per-Entity WAL + restore (crash-resume, state introspection) | always | durable-by-default (per-class opt-out) |
| `time_travel?` | point-in-time queries + retention beyond the latest snapshot (`flz state at`) | yes | no |
| `dynamic_entities?` | lazy per-key Entity creation + destructive idle eviction | yes | no |
| `output_stall_watch?` | per-Entity output-staleness watchdog → forced rebind | yes | no |

`persistent_state?` is resolved per kind **and class** (in `PersistenceResolver`,
because workflow's durability is a per-class opt-out); the other three are pure
per-kind flags (`Flezha.Operative.Kind`) keyed off `Flezha.Operative.ClassRegistry`
(operative_class → kind). An **unknown class defaults to `:stream`**, so every
gate is only ever *subtractive* — stream behaviour is byte-for-byte
unchanged, and a durable workflow operative gets durable state + restore **without**
inheriting time-travel, dynamic-eviction, or output-stall semantics it has no use
for.

## How many instances a runner holds

Every actively-running workflow instance owns one OS driver thread
(`flezha-workflow-<id>`). It holds the chain's position as a call stack and
never runs user code. Measured on 8 cores / 32 GiB, release build, with the
standing probe — re-run it to re-check these numbers on your own hardware:

```
FLEZHA_SCALE_SHAPE=step FLEZHA_SCALE_N=10000 \
  cargo test -p flezha_runner --features bench-internals --release \
    --test workflow_scaling -- --ignored --nocapture
```

| | measured |
|---|---|
| resident per blocked instance | ~73 KiB, flat from 1 000 to 10 000 instances |
| …of which the driver thread is | ~1.4 KiB — the rest is per-instance heap |
| 10 000 blocked instances | ~734–884 MiB resident, start 0.65 s, teardown 0.44 s |
| CPU, 10 000 blocked mid-step | 0.5 % of one core |
| CPU, 10 000 parked in the inbox wait | ~1.6 of 8 cores |

Two things follow for capacity planning.

**Thread count, not instance count, is what interferes.** An agent turn
blocked on a slow call holds two OS threads in Rust (the driver, plus the pool
worker running the step) and one in Java (the driver — Java's worker is a
virtual thread); a class declaring `max_duration()` pays a third, its duration
watcher. At ~10 000 threads in the process, an unrelated instance's
start-to-completion path roughly doubles at the median and halves in
throughput; at ~20 000 it drops to a quarter. Size a runner by threads.

**A driver blocked at a stage barrier is free; a driver polling is not.** The
barrier is a plain blocking receive. Three other waits re-check every 200 ms —
the inbox wait, the commit-ack wait, and the `max_duration` watcher — so a
population sitting in any of them costs real CPU. A *durable* `PerInput` loop
avoids this by parking after `FLEZHA_WORKFLOW_PARK_IDLE_MS`; a
persistence-opted-out one has no park to take and polls for as long as it
lives.

`FLEZHA_WORKFLOW_DRIVER_STACK_KIB` sets the driver stack. It is unset by
default and is an address-space lever only: a blocked driver's stack is not
resident, so shrinking it was measured to move no resident byte. Note that an
author's edge transform (`map`/`filter`/`flat_map`/merge mapping) runs on this
thread, and a stack overflow there aborts the process rather than unwinding.

## Operator notes

- **Crash-resume is at-least-once.** A non-idempotent fire-and-forget job (e.g.
  "send one email") can double-act on resume. Write it idempotently or **opt out
  of durability** so a crash re-runs from a clean slate rather than a partial one.
- **Deploy runners with a graceful stop, not a kill** — the same rule as
  stream ([`data_durability.md`](data_durability.md)). A graceful stop lets a
  running workflow operative migrate; a hard kill forces the crash-resume path.
- **Retention grows disk for many small jobs.** Opt out for fire-and-forget, or
  tune `FLEZHA_WORKFLOW_RETENTION_HOURS`. Workflow is not the high-throughput path.
- **End-to-end proof:** `test/system/workflow_crash_resume_e2e_test.exs` drives a
  real SIGKILL-mid-run (and SIGKILL-mid-compensation) crash-resume across Rust and
  Java over UDS and QUIC.
