# The poll-based SDK core (`flz_core`)

`flz_core` is the language-agnostic runner kernel: transport, ordering,
durability, retries, timers, credit, routing, and the workflow driver live in
one Rust crate. A host language SDK drives it through exactly one invocation
surface:

```text
loop:
  activation = core.poll(timeout)     # blocks inside the core
  result     = run_user_code(...)     # host threads, host semantics
  core.complete(result)
```

The core **never calls into host code and never owns a host thread** (the
FFI log callback is the one permitted exception). Python keeps asyncio, Go
keeps goroutines, Java keeps JVM threads; each SDK writes the loop above
idiomatically (see [the governing rule](#the-core-decides-the-sdk-brings-the-threads)).
The Rust SDK itself is the first host: its typed layer spawns poll workers
and consumes the same engine as a crate — the dogfood rule that keeps the
boundary from eroding.

For the same split drawn rather than described — where it sits in the system,
and how each host's threading model compares — see
[`architecture/runner.md`](architecture/runner.md).

## The core decides, the SDK brings the threads

This is the governing rule of the boundary, and the one an author of the next
SDK (Go, Node, Python) has to read before choosing a threading model.

**The core decides what runs and how much of it.** It picks the next
activation, holds per-entity ordering, exclusivity, the retry ladder and
durability, and caps how many work activations may be underway at once —
`FLEZHA_MAX_INFLIGHT_ACTIVATIONS`, default 1024, enforced at handout: at the
cap `poll` blocks as if nothing were ready, and the completion that drops the
count below it releases the next one. Direct activations (restore folds,
snapshot requests, workflow steps) are exempt — they are barrier-bounded by the
drivers that enqueue them and answer a caller blocked on a rendezvous or a
stage barrier, so holding one behind capped entity work would deadlock that
caller. They are also what a queued direct activation with no thread inside
`poll` reports as starvation: a blocked driver must be able to buy a thread.

**The SDK brings the threads**, with whatever its runtime does best. The
count is a servicing detail, not a limit on concurrency — that limit is the
core's one number. Concretely, the two shipped hosts differ completely and
neither knows about the other:

- **Rust** has no cheap blocking, so it grows its pool. A base pool
  (`FLEZHA_WORKER_POOL_SIZE`, default = available parallelism) plus one
  supervisor thread parked in the core's **starvation wait**. Each
  observation spawns one more worker up to `FLEZHA_WORKER_POOL_MAX` (default
  10× the base); workers above the base retire after an idle streak, so a
  workload that never starves observes exactly the base pool.
- **Java** hands the activation off instead. A small fixed set of platform
  *fetcher* threads (default 2) loop `poll → decode → hand off → poll`, and
  one **virtual thread** per activation runs the dispatch and completes it.
  Waiting then costs a stack, the starvation condition cannot arise by
  construction, and Java never calls `wait_starvation`.

Starvation is exposed as a blocking wait a host thread may park in
(`flz_core_wait_starvation`), never as a callback: the core calls no host
code, and a host that cannot starve simply never calls it. It answers in the
same three-way shape as `poll` — observed / timeout / shutting down — so a
supervisor loop is structurally a worker loop and ends with the runner.

**Starvation means *stuck*, not *busy*.** Queued work counts only once it has
sat for a dwell (100 ms) during which **no thread entered `poll` at all**. A
saturated runner enters `poll` once per batch — microseconds apart — so it
never trips; only a runner whose every thread is inside user code shows no
entry for the window. Answering saturation with more threads would add
contention to a runner that is merely working. A due deferred wake needs no
dwell: every `poll` pass promotes what has come due, so an entry still
sitting there is itself proof that no pass has happened.

Two notes owed to whoever writes the next SDK:

- **Bring your own fetch threads.** Never fetch on the runtime's shared I/O
  pool (libuv's threadpool, a framework's request executor): `poll` blocks
  inside the core, and borrowing a shared pool's thread for it starves
  everything else that pool serves. The same reason Java's fetchers are
  platform threads and not virtual ones — a native frame pins a virtual
  thread's carrier.
- **Completing from a different thread than the one that polled is expected**,
  not merely tolerated. It is what lets a host fetch on one thread and run on
  another, which is the whole point of the split.

## Activation / ActivationResult

The boundary contract lives in `flz_core::schema` and is postcard-encoded;
field order IS the wire contract and `SCHEMA_VERSION` gates it. Every kind
is pinned by hex byte fixtures (`schema.rs fixture_tests`) that each shim
must reproduce byte-identically.

An **Activation** targets exactly one entity and carries:

- `Work(items)` — an ordered per-entity batch of `Input` items (payload
  bytes, metadata, `consumed_offset`, replay flag, core-evaluated `now`)
  and `Timer` firings (kind, `scheduled_at`; its `now` IS `scheduled_at`,
  so a chained `timer_after` anchors replay-stably on the firing that woke
  it). Timer firings are validated against the pending set at assembly —
  a stale entry fires nothing.
- One of the five host-participating command kinds — `RestoreFold`,
  `SnapshotRequest`, `LiveStateRead`, `ApplyState`, `MigrationSerialize` —
  subject to the same per-entity exclusivity. The other 14 control commands
  never surface (pinned: a `Ping` generates no activation).
- `KeyRequest` — batched keying for unkeyed entry payloads bound for a
  keyed edge, answered before anything reaches an entity mailbox.

An **ActivationResult** carries, per processed item, the effect record the
flusher consumes exactly as it consumes a typed drain today: host-encoded
change bytes (core-opaque), outputs `(edge | plugin, bytes, key?)`,
`consumed_offset`, timer ops, checkpoint flag, and a verdict
`ok | retryable_error | non_retryable_error`.

## Per-entity exclusivity and ordering

The core never hands out two activations for the same entity concurrently —
the single-active-drainer CAS gate, expressed at the boundary. An entity
with an outstanding activation reappears in poll results only after its
completion; its inputs are delivered in enqueue order across successive
activations. Batch assembly also owns the consumed-floor skip, replay-slice
buffering and event-time-ordered flush, and late-input admission.

**Event-time-clocked classes get ONE admitted input per activation** (and a
closed replay slice drains one member per round): chained timer
registrations land only at completion, so wider batches would make
due-firing interleave depend on batch timing — and a correction replay must
fold firings at the same positions the live run did. Wall/logical classes
batch freely.

## The retry→park ladder

The engine drives the ladder from completion verdicts: a `retryable_error`
re-hands the same item at the head of the line after the existing backoff
(`base × 2^(n-1)` ±50% jitter, capped), with the unprocessed batch tail
held behind it; a `non_retryable_error` — or a failing final attempt —
parks the payload through the existing DLQ path, and a park closes the
checkpoint interval. Attempt counts and `RETRIES_TOTAL` ride the existing
telemetry.

**The backoff blocks no thread.** Reporting the failure returns the
completing thread immediately; the entity's exclusivity gates stay held
across the window — nothing else could drain it during a backoff anyway —
and the release-then-reclaim runs when the delay comes due. That matters
under correlated failure: with workers sized to the CPU count, a backoff
that slept the completing worker would, when a shared dependency goes down,
put every worker to sleep and stop work that would have succeeded. Shutdown
is not delayed either — there is no sleep left to run out.

## The deferred-wake list

One ordered list answers "do something with this entity at time T": retry
backoffs, and processing-time timer deadlines (`entity-timers`). It owns
**no thread**. Entries are promoted by whichever thread next enters or
wakes inside `poll` — with no lock held, since a firing enqueues under the
`Block` policy and may drain a saturated consumer inline — and the nearest
entry shortens the poller wait. Recording one notifies blocked pollers so
they re-compute that wait.

Promotion needs a thread in `poll`, so a due entry with every worker inside
user code is starvation and reads as such: the starvation wait's deadline
considers the nearest due entry, which is how a host that grows threads gets
one there in time to promote the firing.

## Producer-side transforms and keys (D4)

Consumer-edge transform chains and `key_by` evaluate on the producing side
— for byte-level hosts, in the completion's outputs (`(edge, key)` is all
the core routes on); for the in-process Rust host, at the one routing seam
(`flz_core::routing`) both live delivery and the controller's replay slice
travel, which is what keeps replay determinism free. A transform or key
failure is contained once and enters the ladder.

## The C ABI (`flz_core_ffi`)

Context lifecycle: `flz_core_context_new(shim_schema_version)` →
`flz_core_declare_class` (JSON) → `flz_core_start(namespace)` →
`flz_core_poll` / `flz_core_complete` → `flz_core_shutdown` →
`flz_core_context_free`.

- **Version negotiation happens at creation**: a shim built against a
  different `SCHEMA_VERSION` gets NULL and `flz_core_init_error()` names
  both versions — before any controller connection. Silent runtime skew is
  impossible.
- **Any host thread may poll; any host thread may complete**, including a
  different one than the thread that polled. No affinity.
- `flz_core_poll` blocks up to its timeout and returns a bounded-margin
  timeout result; **shutdown wakes every blocked poller promptly** with a
  distinguishable status. Unpolled work follows the existing durability
  rules (journaled controller-side, re-forwarded on the next bind).
- Ownership: whoever allocates frees. Activation buffers the core hands out
  are freed with `flz_core_buf_free`.

The CI scenario (`flz_core_ffi/tests/abi_scenario.rs`) drives connect,
AddOperative over the real control path, input activations, a timer firing,
a restore fold, a live state read, a keyed dynamic route through the
prepare crossing, an effectively-once floor skip, a workflow run-sugar
lifecycle, and shutdown entirely through the C ABI — a capability reachable
in the crate API but not the ABI is a defect, not a backlog item.

### Host classes (full capability)

`flz_core_declare_class` accepts the complete host-class declaration
(`flz_core::host::HostClassDecl`, JSON with `deny_unknown_fields` — a field
the core never learned is a refusal, not a silent degrade): the channels
graph (entry / inter-proc / plugin, per-channel retry, overflow, plugin
keying), entity modes (single, static with declared keys, dynamic with
idle-TTL eviction and cap), clock kinds (wall, logical, event-time), the
effectively-once tier and checkpoint ack-granularity, the late-input
correction window, the record-header lang/format bytes (`sdk_lang`,
`change_format_id` — a Java host's durable records claim Kryo), and workflow
lifecycle facts (`workflow`: loop mode, rollback mode, stage retry,
max-duration, wake-after; empty `procedures` = run-sugar).

At runtime the full contract crosses the boundary:

- **The five command kinds are real activations.** `RestoreInitState`
  becomes a `RestoreFold` (the core walks the WAL records, folds cursors and
  timer ops itself, and hands the host only its own change bodies — effect
  records unwrapped, plain bodies verbatim); `CompactEntity` a
  `SnapshotRequest` (the core drains, re-states the pending timer set, and
  frames the answer); `EntityState` a `LiveStateRead`; migration a
  `MigrationSerialize` / `ApplyState`. Each runs under the entity's handout
  gate (`PollCore::host_command`), so a fold completes before inputs flow.
- **The prepare crossing** (`KeyRequest`, batched per entry channel) asks
  the host for routing keys and/or extracted event times before anything is
  enqueued; a per-payload failure climbs the entry channel's retry→park
  ladder, and the end-of-slice broadcast defers behind staged slice members.
- **Tiers ride completions.** `ItemEffect` carries changes, outputs,
  timer ops and `checkpoint`; the core builds effect records (offsets are
  core-authoritative), stages checkpoint intervals, holds a
  failed-but-retryable attempt's changes out of the flusher's reach, and
  mints DLQ entries where the typed layer does.
- **Inter-procedure edges route on `(edge, key)`**: outputs name their
  consumer, keys and event times are producer-evaluated (the host holds the
  typed value and the extractor; the core holds neither).
- **Workflow lifecycles run under poll**: a run-sugar body is one
  `WorkflowRun` activation plus the session calls (`flz_core_workflow_emit` /
  `_recv` / `_cancelled`); a structured chain hands out one **step
  activation** per entity of the stage — a `Work` batch, or `CompensateRun` for
  an undo — and collects the answers at its stage barrier. Both language
  paths take that route, so a step costs exactly one thread (the SDK's) and
  no core thread waits on a host: the driver is the only waiter, and its
  retry timing lives there too. Barriers, proc-journal, loop modes,
  suspension and rewind are unchanged driver machinery.

### In-process residuals

Two host-code-on-core-thread seams remain for the crate-consuming Rust host
and are invisible to the C ABI: `TypedBuffer<S>` serializes `S::Change` on
the flusher's linger thread, and erased entry parsers / edge closures
evaluate on the wire-reader/routing thread. Both are the documented
in-process forms of what the byte boundary removes for FFI hosts.

## What stays per-language, forever

The authoring surface (Procedure traits, the graph DSL), State and channel
codecs, the poll loop's integration with the host's concurrency primitives,
idiomatic errors, and the testing harness — the code that *should* feel
native, an estimated 3–5k lines per SDK.
