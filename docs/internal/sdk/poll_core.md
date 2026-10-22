# The Poll Core (`flz_core`)

For SDK implementers and for anyone reading the kernel. `flz_core` is the
language-agnostic runner kernel: transport, ordering, durability, retries,
timers, credit, routing and a steps relay's pass live in one Rust crate. A host
language SDK drives it through exactly one invocation surface:

```text
loop:
  activation = core.poll(timeout)     # blocks inside the core
  result     = run_user_code(...)     # host threads, host semantics
  core.complete(result)
```

The core **never calls into host code and never owns a host thread** (the FFI
log callback is the one permitted exception). The Rust SDK is the first host —
its typed layer spawns poll workers and consumes the engine as a crate, which
is the dogfood rule that keeps the boundary from eroding; the Java SDK is the
second, over the C ABI. The Elixir SDK polls from threads its Rustler library
owns and hands each activation to a BEAM process, so no wait holds a BEAM
scheduler ([`elixir_sdk.md`](elixir_sdk.md#the-runtime-underneath)). This document covers the boundary contract,
the C ABI and where the code lives; the system-level picture is
[`../../public/architecture/runner.md`](../../public/architecture/runner.md).

## Where the code lives

| Path                                   | Purpose |
| -------------------------------------- | ------- |
| `crates/flz_core/src/poll_core/`              | The activation engine (`mod.rs`, `assembly.rs`, `starvation.rs`, `wake.rs`): per-cell handout exclusivity, batch assembly (admission, replay-slice ordering, due-timer generation), the in-flight cap, the deferred-wake list, starvation, `poll` / `complete` |
| `crates/flz_core/src/schema.rs`               | The versioned Activation / ActivationResult boundary contract (postcard; `SCHEMA_VERSION`; bytes pinned in `test/fixtures/postcard_bodies.txt`) |
| `crates/flz_core/src/host/`                   | Erased host classes (`decl.rs`, `register.rs`, `prepare.rs`, `commands.rs`, `activation.rs`, `cell.rs` under `mod.rs`): `HostClassDecl` (JSON), data-declared topologies, completion application — what the C ABI drives |
| `crates/flz_core/src/cell.rs`, `mailbox.rs` | `CellSlot`, bounded mailbox + inflight CAS, shared slot assembly |
| `crates/flz_core/src/routing.rs`              | `enqueue` + overflow policies, consumer-edge delivery, entry-parse and edge ladders, end-of-slice broadcast |
| `crates/flz_core/src/retry.rs`, `park.rs`     | The retry→park ladder: backoff computation, `RETRIES_TOTAL`, DLQ parks |
| `crates/flz_core/src/flusher.rs`, `state_sink.rs`, `credit.rs`, `correction.rs`, `timer.rs`, `restore.rs`, `header.rs` | Durability: WAL framing + linger drain, the reliable output, credit windows, late-input machinery, timers, restore payloads, record headers |
| `crates/flz_core/src/coordinator.rs`, `control_unit.rs`, `runner.rs`, `command/` | RunnerNode, wire readers, control channel + reconnect, one handler module per controller command |
| `crates/flz_core/src/steps/`               | A steps instance's run: `instance.rs` (the pass sheet a journal resume fills), `run.rs` (the events that advance it), `commit.rs` (the barrier frames), `resume.rs` (the journal readers), `compensation.rs` (the undo plan), `spec.rs` (the spec/DAG kernel) |
| `crates/flz_core_ffi/src/lib.rs`, `abi/`      | The context, and the `flz_core_*` C entry points in one `abi/` file per concern (crate types `cdylib`, `staticlib`, `rlib`) |
| `crates/flz_core_ffi/src/jni_glue.rs`         | `Java_com_flezha_sdk_native_1_FlzCore_*` exports behind the `jni` feature |
| `crates/flz_core_ffi/tests/abi_scenario.rs`   | The CI scenario that drives the whole runtime through the C ABI |
| `crates/flz_transport/`                       | The transport tier both the kernel and the controller NIF share: `transport/` (QUIC, TLS, config, liveness, `protocol.rs` stream constants `STREAM_TYPE_PRIMARY 0x01` / `STREAM_TYPE_SECONDARY 0x02`), `service/payloader.rs` (binary framing), `utility/socket_dir.rs` (socket paths), `utility/socket_probe.rs` (reachability) |
| `sdk/rust/src/`                        | The Rust SDK — the typed layer over `flz_core`: `lib.rs` (runner handle), `tasklet/`, `channel/` (builder DSL), `relay/` (typed dispatcher, poll pool), `cell/`, `steps/`, `runner/`, `testing/` (harness), `diag.rs` |
| `sdk/java/`                            | The Java SDK (Maven): `flezha-sdk-api` (authoring surface), `flezha-sdk-runtime` (`PollHost`, `ActivationDispatcher`, codec), `flezha-sdk-native` (`FlzCore`, `NativeLibraryLoader`, bundled binaries), `flezha-sdk-testing` (`TaskletTestHarness`), `flezha-sdk-examples` (the `E01`–`E12` authoring samples), `flezha-sdk` (fat-jar aggregator) |

## The core decides, the SDK brings the threads

This is the governing rule of the boundary, and the one an author of the next
SDK has to read before choosing a threading model.

**The core decides what runs and how much of it.** It picks the next
activation, holds per-cell ordering, exclusivity, the retry ladder and
durability, and caps how many work activations may be underway at once —
`FLEZHA_MAX_INFLIGHT_ACTIVATIONS`, default 1024, enforced at handout: at the
cap `poll` blocks as if nothing were ready, and the completion that drops the
count below it releases the next one. Direct activations (restore folds,
snapshot requests, the other command crossings, a steps relay's undo) are exempt —
a command crossing answers a caller blocked on a rendezvous, so holding one
behind capped cell work would deadlock that caller, and an undo holds its
cell's gate until it is answered. They are also what a queued direct
activation with no thread inside `poll` reports as starvation: a blocked caller
must be able to buy a thread. A steps stage's items are ordinary cell work
and take the cap like a flow relay's.

**The SDK brings the threads**, with whatever its runtime does best. The count
is a servicing detail, not a limit on concurrency — that limit is the core's
one number. **How many is the core's pool governor's** (`flz_core::poll_pool`),
one policy for every host: the base and the ceiling from
`FLEZHA_WORKER_POOL_SIZE` and `FLEZHA_WORKER_POOL_MAX` (ten times the base,
never below it) over the default base the host passes; a poll turn of 250 ms;
a supervisor that parks in the starvation wait for at most 1 s and answers
each observation with one more worker up to the ceiling and a pool below its
base with a replacement; a worker above the base retired after 30 s of
timed-out polls; and a drain bound of 5 s. A Rust host (the Rust SDK, the Node
addon's fetchers, the Elixir NIF's pollers) hands the governor a `Spawner`; a
C ABI host (Python, Ruby, PHP, Java, Go) opens the pool with `flz_core_pool_open`,
once per context (a second open is refused), answers `flz_core_pool_supervise` from a thread of its own and gives a seat
back with `flz_core_pool_left`. A host keeps only how it spawns a worker and
what the worker runs. The hosts differ completely in that and none knows about
the others:

| Host | Threading model | Knobs |
| ---- | --------------- | ----- |
| Rust | A base pool of poll workers plus one supervisor thread parked in the core's **starvation wait**. Each observation spawns one more worker up to the ceiling; workers above the base retire after an idle streak, so a workload that never starves observes exactly the base pool. | `FLEZHA_WORKER_POOL_SIZE` (default: available parallelism, 2 if unknown), `FLEZHA_WORKER_POOL_MAX` (default 10× the base, never below it) |
| Java | A small fixed set of platform *fetcher* threads loop `poll → decode → hand off → poll`; one **virtual thread** per activation runs the dispatch and completes it (`Executors.newVirtualThreadPerTaskExecutor()`), plus one platform **supervisor** thread parked in the starvation wait that starts one more fetcher per observation, up to the ceiling. | `FLEZHA_WORKER_POOL_SIZE` (default 2), `FLEZHA_WORKER_POOL_MAX` (default 10× the count, never below it) |
| Node | Java's shape, with a hard constraint where Java has a choice. Native fetcher threads the SDK owns — never libuv's shared pool — loop `poll → decode → hand off → poll` and hand each activation to one **event loop**: the main one by default, or one of N `worker_threads`. Blocking calls run on a **growable** native pool, because `complete` parks under `DropNothing` and a fixed pool whose every thread parks pushing to one consumer has no thread left for the completion that would drain it. One supervisor parks in the starvation wait. | `FLEZHA_NODE_WORKERS` (default 0 = the main loop), `FLEZHA_WORKER_POOL_SIZE` (default 2), `FLEZHA_WORKER_POOL_MAX` (default 10× the base, never below it) |
| PHP | Two shapes behind one seam, because a stock PHP process runs one thread of PHP code and only a thread-safe build lifts it. `SyncExecutor` (the default) loops `poll → dispatch → complete` on the calling thread — one loop, no supervisor, and cores come from more processes. `SwooleExecutor` runs the same loop on a pool of `Swoole\Thread`s over one shared context, each thread bootstrapping the entry script and *attaching* to the context by its address; the main thread is the supervisor, parked in the starvation wait, and adds a thread per observation. | `FLEZHA_PHP_EXECUTOR` (`sync` — the default — or `swoole`), `FLEZHA_MAX_ACTIVATIONS` (default unlimited); under `swoole`, `FLEZHA_WORKER_POOL_SIZE` (default 1), `FLEZHA_WORKER_POOL_MAX` (default 10× the base, never below it) |
| Ruby | The Rust host's shape, because MRI's situation is the Rust host's: an MRI thread is a real OS thread with a real stack, so hand-off would buy nothing and cost a thread per activation. A base pool of Ruby threads each loop `poll → dispatch → complete` **on themselves**, plus one supervisor parked in the starvation wait. Every blocking core call runs with the **GVL released**, so the lock bounds the handler body and nothing else. An opt-in `Fiber::Scheduler` executor hands off through a `Thread::Queue`, with the fetcher still a real thread. | `FLEZHA_WORKER_POOL_SIZE` (default: the machine's processor count), `FLEZHA_WORKER_POOL_MAX` (default 10× the base, never below it), `FLEZHA_RUBY_EXECUTOR` (`threads` — the default — or `fibers`) |
| Python | The Rust host's shape, for the reason CPython's situation is the Rust host's: a CPython thread is a real OS thread with a real stack, so hand-off would buy nothing and cost a thread per activation. A base pool of Python threads each loop `poll → dispatch → complete` **on themselves**, plus one supervisor parked in the starvation wait. Every blocking core call runs with the **GIL released**, so the lock bounds the handler body and nothing else — and the free-threaded wheel tier lifts even that. An opt-in asyncio executor hands each activation to one shared loop through `run_coroutine_threadsafe`, with the fetcher still a real thread and the completion on the loop thread; the supervisor's answer under *either* executor is one more fetch-and-run thread, because a stalled loop cannot rescue itself. | `FLEZHA_WORKER_POOL_SIZE` (default: the machine's processor count), `FLEZHA_WORKER_POOL_MAX` (default 10× the base, never below it), `FLEZHA_PYTHON_EXECUTOR` (`thread` — the default — or `asyncio`) |
| Go | The Rust host's shape: a base pool of poll goroutines each loop `poll → dispatch → complete` **on themselves**, plus one supervisor goroutine parked in the starvation wait that adds a poller per observation, up to the ceiling. No goroutine is locked to its thread; a blocking core call holds its OS thread and the Go scheduler starts another for the rest. | `FLEZHA_WORKER_POOL_SIZE` (default: the host's core count), `FLEZHA_WORKER_POOL_MAX` (default 10× the base, never below it) |

**No core call blocks a host thread.** The core cannot know how many threads a
host has, whether it can create more, or whether its "threads" pin a shared
carrier — a Node host on the event loop has one, a PHP `SyncExecutor` has one,
a Java virtual thread pins its carrier on a native frame. So the rule is not
"park only when it is safe to": backpressure is expressed by **withholding work
from the producer**, never by holding the producer's caller. A single-threaded
host is a supported shape, and it is supported by the core never blocking, not
by the host promising to bring more threads.

The one park that remains is the **transport reader's**, on the entry path. It
is permitted because that reader is a core-owned thread no host language
observes, it has no producing Cell to withhold work from, and its stalling is
what carries ingress backpressure to the controller. See "Backpressure without
holding a thread" below.

**A poll host brings a supervisor.** Not "may": a host that ships a fixed
worker count and runs no supervisor has no answer at all
when every worker is held — it processes nothing, recovers from nothing, and
reports nothing. Java shipped exactly that shape and measured 0 of 20 payloads
delivered against a consumer nobody was draining; with the supervisor the same
probe recovers. What the supervisor does *not* buy is a thread against user
code that never returns: no ceiling outruns a handler that never comes back,
which is why the conformance `pool_wedge` capability is a separate claim.

**A thread inside `poll` is invisible to the starvation wait.** `pollers_inside`
is incremented across the whole call, and `starving()` answers `false`
unconditionally while it is non-zero — a thread inside `poll` is read as a
thread about to take work. So a poller that blocks anywhere inside `poll` both
stops the runner and hides that it has, which is why nothing on the poll path
may wait for anything (see the deferred-wake list below).

Starvation is exposed as a blocking wait a thread may park in
(`PollCore::wait_starvation`, which the governor's supervisor parks in; a C ABI
host reaches it through `flz_core_pool_supervise`), never as a callback: the
core calls no host code. It answers in the same three-way shape as `poll` —
`Observed` / `Timeout` / `ShuttingDown` — so a supervisor loop is structurally
a worker loop and ends with the runner.

**Starvation means *stuck*, not *busy*.** Queued work counts only once it has
sat for a dwell (100 ms) during which **no thread entered `poll` at all**. A
saturated runner enters `poll` once per batch — microseconds apart — so it
never trips; only a runner whose every thread is inside user code shows no
entry for the window. Answering saturation with more threads would add
contention to a runner that is merely working. A due deferred wake needs no
dwell: every `poll` pass promotes what has come due, so an entry still sitting
there is itself proof that no pass has happened. It is owed one thread at a
time, though: after it is answered, it reads as starvation again only once a
thread has entered `poll` or a dwell has passed, so a supervisor asking back to
back cannot grow the pool before the thread it was given arrives.

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
field order IS the wire contract and `SCHEMA_VERSION` (currently 8) gates it.
Every kind is pinned as bytes in `test/fixtures/postcard_bodies.txt`, which
the core's tests encode and each shim must reproduce byte-identically.

An **Activation** targets one cell (a relay-wide `Leave`, one relay) and carries one `ActivationKind`:

It also carries `cell_identity`, the Cell's wire identity bytes, on the first
activation for that Cell since the host last dropped or rebuilt it (its `Leave`
or `RestoreFold`), again after an activation for it was abandoned or failed
whole (it may never have reached the host), and on no other: the host keeps
them with the Cell, a re-sent copy as the first, and derives `ctx.stable_id()`
from them, and never builds the layout itself.

| Kind                                                                 | What the host does |
| -------------------------------------------------------------------- | ------------------ |
| `Work(items)`                                                        | An ordered per-cell batch of `Input` items (payload bytes, metadata, `consumed_offset`, replay flag, core-evaluated `now`, and `form`: `untransformed` for an entry payload or a steps stage's released value, `transformed` for a stream producer's edge output, `prepared` for a keyed consumer's entry payload, shipping no payload bytes, only the values its prepare answer named for this Cell; the host applies the consuming edge's chain to an `untransformed` one only, and runs a `prepared` one's values, each decoded as an inter-tasklet value, without the entry decoder or the edge) and `Timer` firings (kind, `scheduled_at`; its `now` IS `scheduled_at`, so a chained `timer_after` anchors replay-stably on the firing that woke it). Timer firings are validated against the pending set at assembly — a stale entry fires nothing. |
| `RestoreFold`, `SnapshotRequest`, `LiveStateRead`, `ApplyState`                      | The four host-participating command kinds, under the same per-cell exclusivity; a live read and a migration both read the state as `LiveStateRead`. No other controller command produces an activation (pinned: a `Ping` generates none). |
| `KeyRequest`                                                         | Batched keying for unkeyed entry payloads bound for a keyed edge, answered before anything reaches a cell mailbox: the host decodes each payload once and answers, per keyed consumer edge, every value its chain yields with the Cell keys it names, encoded as an inter-tasklet value. The core enqueues one `prepared` item per Cell carrying that Cell's values; a consumer that needs no keys gets the payload `untransformed`. |
| `CompensateRun`                                                      | An undo step of a steps chain. |
| `Leave { whole_relay }`                                              | The target cell was evicted, or with `whole_relay` the relay stopped, finished or moved away: the host drops what it holds for it, so a cell started again under the same name begins at `init_state`. An eviction, made only once everything the cell settled has shipped, crosses it under the cell's gate and waits for the answer before the key is removed, so a cell minted again under the key is never handed work first, unless the relay stops before a host took it, when it is withdrawn and the relay's own leave covers the cell; a stop sends it once no activation handed out for the relay is still running, and an `AddRelay` for the same id is handled only after the host answered it. Answered `StateApplied`. |

An **ActivationResult** (`ActivationOutcome::Work(Vec<ItemEffect>)`) carries,
per processed item, the effect record the flusher consumes exactly as it
consumes a typed drain: host-encoded change bytes (core-opaque), outputs
`(edge | plugin, bytes, key?)`, `consumed_offset`, timer ops, the checkpoint
flag, and a `Verdict` — `Ok` / `RetryableError` / `NonRetryableError`.

## Per-cell exclusivity and ordering

The core never hands out two activations for the same cell concurrently —
the single-active-drainer CAS gate, expressed at the boundary. A cell with
an outstanding activation reappears in poll results only after its completion;
its inputs are delivered in enqueue order across successive activations. Batch
assembly also owns the consumed-floor skip, replay-slice buffering and
event-time-ordered flush, and late-input admission.

**Event-time-clocked classes get ONE admitted input per activation** (and a
closed replay slice drains one member per round): chained timer registrations
land only at completion, so wider batches would make due-firing interleave
depend on batch timing — and a correction replay must fold firings at the same
positions the live run did. Wall/logical classes batch freely.

### Backpressure without holding a thread

Under `DropNothing` a push into a full consumer mailbox has to wait for
something. What waits differs by who produced the value:

| producer | what waits |
| --- | --- |
| an activation's `ctx.emit`, or a host completion's edge outputs | the **producing Cell**: the push is appended to its push holdback and it is handed no further activation until the holdback drains |
| a due wake (a timer firing, an interval close) | the **wake list**: the entry is re-recorded at a later due time (see "The deferred-wake list") |
| the shared transport reader on an entry channel | the **reader thread**, which parks — the one exception, and the thing that carries ingress backpressure to the controller |

A producing Cell's gates stay held across the holdback exactly as they stay
held across a retry backoff: nothing else could drain that Cell then either,
and producers just enqueue. The consumer's next free slot serves whoever is
owed it — a parked thread, or a gated Cell whose holdback is then drained —
and a bounded fallback wake means a lost signal degrades to a retry rather than
a hold. Because a gated Cell keeps its round claimed, the dynamic-mode sweep
skips it with no check of its own, and a restore or a compaction waits on it
the way it waits on a retry window.

**Ordering is preserved by construction**, by two rules that together reproduce
exactly what a parked producing thread achieved:

1. a Cell with a non-empty push holdback is handed no new activation;
2. a push made while that Cell's holdback is non-empty is **appended** to it
   rather than attempted — this is what stops item 2 of a completion's batch
   from overtaking item 1's held output.

Rule 2 is evaluated on every push, including the overwhelming majority that
succeed, so it reads a lock-free flag and never inspects the holdback under its
lock: measured, the atomic load is free (14.39 ns against 14.79 ns for a bare
`try_push`) and a mutex acquisition costs +64%.

**A held push holds its durable record too.** An inter-tasklet output has no
durable copy, so the only thing that can re-produce it is the producer
re-running its input. While a Cell owes a push, the flusher skips its buffer
— the consumed offset must not become durable ahead of the edge delivery it
derives from, or a crash would leave the replay floor skipping that input with
the consumer never fed.

The holdback is **bounded** (`FLEZHA_PUSH_HOLDBACK_MAX`, default 4096). An
invocation that emits past the bound waits for its own earlier pushes to land
— the one park left on an emit path, and one only the in-process typed host can
reach, because an FFI host's outputs are materialized as bytes before the core
sees them. That host is also the one with a supervised worker pool behind it.

A gated producer holds no handout, so `inflight` is blind to it; it is reported
instead as `pending_pushes` and `pending_push_oldest_ms` on the data heartbeat
([Heartbeat frames](protocol_spec.md#heartbeat-frames)).

### The host answers every activation exactly once

Because the handout is what gates the cell, an activation that is never
answered retires its cell **for the life of the process** — silently, since
the runner-wide dispatch counter keeps climbing on every other cell. There
are exactly three answers:

| Answer | When | Ladder attempt |
| ------ | ---- | -------------- |
| `complete` | the batch ran | — |
| fail (`retry_after_failure`) | an item failed retryably | consumed |
| `abandon` | the host could not finish: an unwound worker, a dead thread, a veneer whose language threw | **none** |

Abandon re-hands the items the host did **not** process and leaves the cell
exactly as if it had never been polled. Only untouched items may be passed:
re-handing one that already ran would execute it twice, which is the one thing
this seam may never do.

**Make the answer structural, not a discipline.** An RAII guard in Rust
(`relay::dispatcher::Handout`, whose `Drop` abandons), a `finally` in Java
(`PollHost.runActivation`), in Elixir a pool that fails the activation of a worker
that exits — so a panic, an exception, an
early return or a `?` cannot skip it. A contract a reviewer has to notice is
not a contract: the discipline version of this rule produced two live leak
paths before it was replaced.

Answering a token the core does not hold — a double answer, or one from a
context that never held it — is **reported**, not ignored: `Answered::UnknownToken`
in the crate API, an error through `flz_core_last_error` at the ABI.

### What the core will never do

**It never reclaims an outstanding activation on a timer.** From the boundary a
ten-minute Tasklet and a handout the host lost are byte-for-byte identical:
an entry in `outstanding` that has not been answered. A deadline that re-handed
would re-execute work the effectively-once tier promises once, and would do it
precisely to the slowest, most expensive activations in the system. There is no
threshold that separates the two cases, so the core does not pretend there is
one.

What it does instead is **report**: `outstanding_count()` and
`outstanding_oldest_age_ms()` ride the data-channel heartbeat
([Heartbeat frames](protocol_spec.md#heartbeat-frames)) as `inflight` and
`inflight_oldest_ms`, the controller's classifier mints
`cell_dispatch_stalled` from them, and the remedy is a relay rebind that
resumes from durable state — the path that is already safe. Detection is the
core's; the judgement and the remedy are the controller's.

**And it never waits inside `poll`.** The engine's own paths — batch assembly,
wake promotion — take no lock a host holds and wait on no mailbox. A poller
that waits is counted as a poller about to take work, so a wait there is a
wedge the starvation supervisor is structurally blind to.

## The retry→park ladder

The engine drives the ladder from completion verdicts: a `RetryableError`
re-hands the same item at the head of the line after the backoff, with the
unprocessed batch tail held behind it; a `NonRetryableError` — or a failing
final attempt — parks the payload through the DLQ path, and a park closes the
checkpoint interval. The backoff is `base × 2^(attempt−1)` (exponent capped at
16) with jitter uniform in `[½·that, 1½·that)`, capped at
`RETRY_BACKOFF_MAX_MS`; attempt counts and `RETRIES_TOTAL` ride the existing
telemetry.

**The backoff blocks no thread.** Reporting the failure returns the completing
thread immediately; the cell's exclusivity gates stay held across the window
— nothing else could drain it during a backoff anyway — and the
release-then-reclaim runs when the delay comes due. Under correlated failure
this is what keeps a runner alive: with workers sized to the CPU count, a
backoff that slept the completing worker would, when a shared dependency goes
down, put every worker to sleep and stop work that would have succeeded.
Shutdown is not delayed either — there is no sleep left to run out.

## The deferred-wake list

One ordered list answers "do something with this cell at time T": retry
backoffs, push re-arms, processing-time timer deadlines
([`cell_timers.md`](cell_timers.md)), checkpoint-interval deadline closes,
an instance's deadlines and a dynamic tasklet's idle sweep. It owns **no
thread**. Entries are promoted by whichever thread next enters or wakes inside
`poll`, and the nearest entry shortens the poller wait. Recording one notifies
blocked pollers so they re-compute that wait.

**Two clocks, one list.** The key leads with a `lane`: a processing-time timer
is due on the wall clock, its `scheduled_at` being an absolute wall time the
controller re-arms across restarts; every other entry is a span from now, due
on the monotonic clock, so a wall-clock step neither delays nor advances it.
The list stays one `BTreeMap` ordered by lane, then due time, with one cached
earliest due per lane; a pass reads a lane's clock only while that lane holds
an entry, so an idle pass reads no clock at all.

**An instance deadline is re-armed in one call.** `PollCore::rearm_instance`
removes the entry armed for the due time it replaces and arms the new one, so a
re-arm leaves one entry. An entry its owner has since re-decided is still a
no-op at promotion: the owner's own record of its current due time (a steps
run's pass sheet, a flow relay's idle watch) is what the promotion is judged by.

**The idle sweep is an entry too.** A dynamic tasklet with an idle TTL arms a
sweep `FLEZHA_REAP_INTERVAL_MS` out (default `min(ttl / 4, 30 s)`); its
promotion sweeps the registry and arms the next one until the relay stops. A
host-run Cell's eviction waits for the host to answer its leave, which a poller
must not, so a sweep with such a Cell to evict runs on a `flezha-leave` thread
of its own, as a relay's leave does. A stop waits out a sweep in flight.

**A wake that cannot be delivered stays on the list.** Promotion attempts a
non-waiting admission; if the target's mailbox is full the entry is re-recorded
at a strictly later due time and the pass continues. The list IS the holdback,
so nothing new was built to hold the firing — and the condition becomes
*reportable*, because `starving()` treats a due wake as starvation with no
dwell. Neither promotion path serves a consumer inline and neither parks: the
promoter is a thread inside `poll`, and a thread inside `poll` is counted as a
thread about to take work.

A firing keeps the timestamp it was **armed** with across every deferral — that
is what the pending set cancels by and what `ctx.now()` reads inside `on_timer`
— while the entry's due time moves. A deferred firing is delayed, never
dropped; and it is never *wrongly* delivered either, because the pending set is
the authority at staging: an entry for a timer that was cancelled or restored
away fires nothing.

Promotion needs a thread in `poll`, so a due entry with every worker inside
user code is starvation and reads as such: the starvation wait's deadline
considers the nearest due entry, which is how a host that grows threads gets
one there in time to promote the firing.

## Producer-side transforms and keys

Consumer-edge transform chains and `key_by` evaluate on the producing side —
for byte-level hosts, in the completion's outputs (`(edge, key)` is all the
core routes on); for the in-process Rust host, at the one routing seam
(`flz_core::routing`) both live delivery and the controller's replay slice
travel, which is what keeps replay determinism free. A transform or key
failure is contained once and enters the ladder.

## The C ABI (`flz_core_ffi`)

Context lifecycle: `flz_core_context_new(shim_schema_version)` →
`flz_core_declare_class(ctx, json, len)` → `flz_core_start(ctx, namespace)` →
`flz_core_poll(ctx, timeout_ms, &buf, &len)` / `flz_core_complete(ctx, buf, len)`
→ `flz_core_shutdown(ctx)` → `flz_core_context_free(ctx)`.

| Export | Role |
| ------ | ---- |
| `flz_core_schema_version`, `flz_core_context_new`, `flz_core_init_error`, `flz_core_last_error` | Version negotiation and error text |
| `flz_core_error_document` | The JSON error document the catalogue renders for a code and its details: how a host renders a refusal it makes itself |
| `flz_core_declare_class`, `flz_core_start`, `flz_core_shutdown`, `flz_core_context_free` | Context lifecycle |
| `flz_core_poll`, `flz_core_complete`, `flz_core_abandon`, `flz_core_buf_free` | The loop |
| `flz_core_pool_open`, `flz_core_pool_supervise`, `flz_core_pool_left` | The pool governor: its policy over the host's default base, one decision per supervisor wait (`FLZ_POOL_IDLE` 0, `FLZ_POOL_SPAWN_BASE` 1, `FLZ_POOL_STOP` 2, `FLZ_POOL_SPAWN_GROWN` 3; a spawn answer has reserved its seat), and a seat given back |
| `flz_core_spawn_request`, `flz_core_spawn_result` | Tasklet-requested spawn: a request that answers a handle at once, and a read of that handle with a timeout (0 never waits) |
| `flz_core_part` | A part of the invocation of work item `item` of the live activation `token`: numbered, stamped and written at once, never waited on. `FLZ_OK` for a part sent or dropped; `FLZ_ERR` with `part.no_output` or `part.outside_invocation` (a token no longer outstanding, an item it does not hold) |
| `flz_core_monotonic_nanos`, `flz_core_entry_last_delivery_nanos` | Context-free reads |
| `flz_core_knob` | Context-free: the text a host looked up for a knob, parsed by the one rule of `test/fixtures/knob_parse_cases.txt` (`positive`, `ms`, `int`, `bool`); `FLZ_KNOB_UNSET` keeps the host's default |
| `flz_core_response_decode` | Context-free: a `Response` channel payload read by `flz_core::call::Envelope` into the caller's `out`, as long as the payload: the target, the request's bytes and the answer's bytes or the error document (the envelope's error object as written, which the core has read), back to back, with their three lengths, `created` and `answered` in the caller's five `parts`; a payload that is no envelope answers `FLZ_ERR` with `call.malformed_response` in `flz_core_init_error` |
| `flz_core_take_notices` | The core's notices held since the last take, as JSON; the first take starts the holding, so a host writes them through its own log on a thread of its own |
| `flz_core_set_log_callback` | The one host callback the core ever invokes, on whichever thread wrote the notice; for a C host that can take it there |

- **Version negotiation happens at creation**: a shim built against a
  different `SCHEMA_VERSION` gets NULL and `flz_core_init_error()` names both
  versions — before any controller connection. Silent runtime skew is
  impossible.
- **Any host thread may poll; any host thread may complete**, including a
  different one than the thread that polled. No affinity.
- **A host whose unit of concurrency is bound to one thread must keep every
  blocking entry point off that thread.** The blocking ones are the poll, the
  starvation wait, the completion, the steps relay receive, the spawn result read
  with a nonzero timeout and the runner start. The spawn request, and the spawn
  result read with a timeout of zero, never wait. An event loop is the case this rule exists for, and
  `flz_core_complete` is what makes it a correctness rule rather than a latency
  one: under the default overflow policy it parks until the consumer's mailbox
  drains, and the consumer drains only when its handler runs. A host that parks
  the one thread able to run that handler cannot recover — the inline-drain
  escape the crate host uses (`routing::install_inline_serve`) takes a host
  function pointer and is by construction unreachable over the ABI. The Node
  veneer draws that boundary once, in the binding: `flz_core_poll` is not
  exported to JavaScript at all, and the four remaining blocking entry points
  are promises resolved from the host's own threads.
- **`flz_core_abandon(ctx, token)` exists because no unwind crosses the ABI.**
  A veneer whose host language throws, or whose thread dies, has no way to
  reach `flz_core_complete`, and per-cell exclusivity would then retire that
  cell for the life of the process. A veneer SHALL answer every polled
  activation exactly once and SHALL make that structural in its own language —
  a `finally`, a destructor, a scope guard. An unknown or already-answered
  token is refused with `FLZ_ERR` and the reason in `flz_core_last_error`.
- `flz_core_poll` blocks up to its timeout and answers `FLZ_POLL_ACTIVATION`
  (1), `FLZ_POLL_TIMEOUT` (0) or `FLZ_POLL_SHUTTING_DOWN` (2); **shutdown wakes
  every blocked poller promptly** with that distinguishable status.
  `flz_core_pool_supervise` answers `FLZ_POOL_STOP` (2) then. Unpolled work
  follows the durability rules (journaled
  controller-side, re-forwarded on the next bind).
- Ownership: whoever allocates frees. Activation buffers the core hands out are
  freed with `flz_core_buf_free`.
- **Context-free reads**: `flz_core_monotonic_nanos()` takes no context: it
  reads a process-wide clock, so every host's `ctx` dates an event the same way
  without keeping a second clock in step.

The CI scenario (`crates/flz_core_ffi/tests/abi_scenario.rs`, `c_abi_full_scenario`)
drives connect, AddRelay over the real control path, input activations, a
timer firing, a restore fold, a live state read, a
keyed dynamic route through the prepare crossing, an effectively-once floor
skip, a steps tasklet completing its instance and shutdown entirely
through the C ABI —
a capability reachable in the crate API but not the ABI is a defect, not a
backlog item.

### Host classes (full capability)

`flz_core_declare_class` accepts the complete host-class declaration
(`flz_core::host::HostClassDecl`, JSON with `deny_unknown_fields` — a field
the core never learned is a refusal, not a silent degrade): the channels graph
(entry / inter-task / plugin, per-channel retry, overflow, plugin keying),
cell keys, idle-TTL eviction and `max_cells` (the core derives each tasklet's
cell mode: static with declared keys, dynamic when a chain reaches it through
`key` then `into` or `merge`, single otherwise), the schema version, its
readable floor and whether the tasklet `migrates` (a floor above the version, or
below it with no migration, is refused), the mailbox capacity (absent takes
`FLEZHA_MAILBOX_CAPACITY`, else 1024), clock kinds (wall, logical, event-time), the
effectively-once tier and checkpoint ack-granularity, the late-input correction
window, the record-header lang/format bytes (`sdk_lang`, `change_format_id` — a
Java host's durable records claim Kryo), and steps lifecycle facts
(`steps`: loop mode, rollback mode, stage retry, max-duration, wake-after;
a steps class declaring no tasklet is refused).

At runtime the full contract crosses the boundary:

- **The four command kinds are real activations.** `RestoreInitState` becomes
  a `RestoreFold` (the core walks the WAL records, folds cursors and timer ops
  itself, and hands the host only its own change bodies — effect records
  unwrapped, plain bodies verbatim); `CompactCell` a `SnapshotRequest` (the
  core drains, re-states the pending timer set, and frames the answer);
  `CellState` a `LiveStateRead`; migration a `LiveStateRead` /
  `ApplyState`. Each runs under the cell's handout gate
  (`PollCore::host_command`), so a fold completes before inputs flow.
- **The prepare crossing** (`KeyRequest`, batched per entry channel) asks the
  host for each keyed consumer's values with their Cell keys and/or extracted
  event times before anything is enqueued, so a keyed payload is decoded and
  transformed once; a per-payload failure climbs the entry channel's retry→park
  ladder, and the end-of-slice broadcast defers behind staged slice members.
- **Tiers ride completions.** `ItemEffect` carries changes, outputs, timer ops,
  `checkpoint` and a tasklet's `complete` request (a flow instance ends once its
  Cells are idle); the core builds effect records (offsets are
  core-authoritative), stages checkpoint intervals, holds a
  failed-but-retryable attempt's changes out of the flusher's reach, and mints
  DLQ entries where the typed layer does.
- **Inter-tasklet edges route on `(edge, key)`**: outputs name their
  consumer; keys and event times are producer-evaluated (the host holds the
  typed value and the extractor; the core holds neither).
- **Steps relay lifecycles run under poll**: a steps stage's items are routed
  into its cells' mailboxes and handed out as ordinary `Work` batches, and an
  undo crosses as a `CompensateRun` on the direct queue. Nothing waits for
  them: a settled item, a commit ack, a deadline and an undo's answer are
  events that advance a pass sheet on the instance, on the thread that brings
  each one. Both language paths take that route, so a stage costs exactly the
  SDK threads that run its items and no core thread belongs to an instance.
  The retry ladder is the flow relay's; barriers, the task journal, loop modes,
  suspension and rewind are the sheet's
  ([`../../public/architecture/steps_profile.md`](../../public/architecture/steps_profile.md)).

### In-process residuals

Two host-code-on-core-thread seams remain for the crate-consuming Rust host
and are invisible to the C ABI: `TypedBuffer<S>` serializes `S::Change` on the
flusher's linger thread, and erased entry parsers / edge closures evaluate on
the wire-reader/routing thread. Both are the in-process forms of what the byte
boundary removes for FFI hosts.

## The Java binding

The Java SDK is a poll host over `flz_core`: its runtime binds to
`flz_core_ffi` built with the `jni` feature through the one-to-one `FlzCore`
class — `schemaVersion()`, `contextNew(shimSchemaVersion)`,
`declareClass(ctx, json)`, `start(ctx, namespace)`, `poll(ctx, timeoutMs)`,
`complete(ctx, result)`, `spawnRequest` / `spawnResult`, `monotonicNanos()`,
`shutdown(ctx)`, `contextFree(ctx)`. `PollHost`'s fetcher threads loop
`poll → decode Activation → hand off → poll`; the kernel (transport, ordering,
durability, retries, timers, credit, routing, a steps relay's pass, reconnect)
lives entirely in the core.

**The shipped binary.** One `libflz_core_ffi.{so,dylib,dll}` per platform
exports both the C ABI (`flz_core_*`) and the JNI symbols
(`Java_com_flezha_sdk_native_1_FlzCore_*`). The `flezha-sdk-native` module's
`native-build` Maven profile drives `scripts/build-native.sh` (host triple by
default; `--targets all` cross-compiles all five triples with cargo-zigbuild);
the binaries land under `src/main/resources/native/<os>-<arch>/` (`linux-x64`,
`linux-arm64`, `macos-x64`, …) and are extracted and `System.load`-ed at SDK
init by `NativeLibraryLoader`. Schema negotiation at `FlzCore.contextNew`
refuses a shim/core version mismatch before any controller connection.

**Buffer ownership at the JNI boundary.**

1. Every `byte[]` the binding returns (a polled activation, a spawn result) is
   copied from the core's buffer into a fresh Java array, and
   the native buffer is freed through `flz_core_buf_free` before the JNI method
   returns. No native buffer escapes.
2. Java-owned arrays handed into native methods (`declareClass`, `complete`,
   …) are copied into Rust-side buffers before the call returns; the JVM
   retains ownership.

## The Node binding

The Node SDK is a poll host over the same crate, built with the `napi` feature
beside the `jni` one — one binary, two veneers, no second copy of the ABI. The
napi layer is one-to-one with the C entry points and its exported surface is
asserted against a declared table (`nativeSurface()`), so an export that named
no C entry point, or a blocking one reachable synchronously, fails a test
rather than a review.

Two things differ from the Java binding, both following from the event loop:

1. **`poll` and the pool entries are not exported to JavaScript.** Native
   fetcher threads inside the binding poll, sized by the governor, which is
   also what keeps them off the runtime's shared I/O threadpool — four threads
   that `fs`, `dns` and `crypto` also use.
2. **The activation is decoded on the Rust side** and reaches JavaScript as a
   structured object rather than bytes, so the SDK carries no second
   implementation of the postcard contract. Payload `Vec`s move into `Buffer`s
   without copying, and every core buffer is freed through `flz_core_buf_free`
   before the fetcher's turn ends.

`docs/internal/sdk/node_sdk.md` states the host's parallelism model as arithmetic.

## The PHP binding

The PHP SDK is the first host to consume the C ABI **as a C ABI** — through
`ext-ffi`, with no compiled veneer of its own. PHP has no stable ABI across
minor versions, so a native extension would be a per-minor × NTS/ZTS ×
5-triple matrix that grows with every release; `ext-ffi` needs one library per
platform, the five that already exist for Java.

Three things follow, and they are the whole of what is PHP-specific:

1. **The activation codec is postcard in interpreted PHP**, pinned against the
   rows of `test/fixtures/postcard_bodies.txt`, which the core encodes from
   its own structs. The test reads the rows rather than copying them, so a
   schema change is a red PHP build in the commit that makes it. Only the two
   directions a host uses exist: activations decode, results encode.
2. **`u64` has no PHP representation above 2^63−1.** A field needing the 64th
   bit raises a decode error naming it rather than silently widening to a
   float, which would corrupt an offset comparison instead of failing.
3. **The FFI handle resolves per interpreter, and so per thread.** `FFI::cdef`
   builds inside the interpreter that calls it and a `Swoole\Thread` is a
   separate interpreter, so a plain static is already per-thread on a ZTS
   build. What does *not* follow the interpreter is the context: a worker
   receives its address as an integer and re-casts it, and never frees what it
   attached to.

`docs/internal/sdk/php_sdk.md` states the host's sizing model as arithmetic, and names
the three requirements the threaded executor checks before the runner connects
— a ZTS build, `Swoole\Thread`, and `ffi.enable=true` (the shipped `preload`
permits FFI from the CLI SAPI, and a worker thread is not it).

## What stays per-language

The authoring surface (Tasklet traits, the graph DSL), State and channel
codecs, the poll loop's integration with the host's concurrency primitives,
idiomatic errors, and the testing harness — the code that *should* feel
native. Everything in that list changes in lockstep across the SDKs; the rule
and the per-SDK matrix live in
[`../../public/architecture/runner.md` → "One change, every SDK"](../../public/architecture/runner.md#one-change-every-sdk).

## See also

- [`authoring_contract.md`](authoring_contract.md) — the sugar that expands onto this kernel
- [`protocol_spec.md`](protocol_spec.md) — the wire protocol the transport tier carries
- [`state_persistence.md`](state_persistence.md) — WAL layout and restore the flusher feeds
- [`rust_relay_architecture.md`](rust_relay_architecture.md) — the Rust typed layer in detail
- [`../../public/architecture/runner.md`](../../public/architecture/runner.md) — the runner drawn, all three hosts
