# Kernel refactor (`refactor-1-kernel`) — implementation notes

Area 1 of the refactor programme: `flz_core`, `flz_transport`, `flz_nif`,
zero behaviour change, run as three worker segments (batches 0–3, 4–7, 8–11)
with a supervisor commit at each checkpoint. What `docs/refactor/charter.md`
says is not repeated; the A/B tables live in the change's `design.md`. What is
here is what a later session would otherwise rediscover, plus the module
narrative the charter moved out of the crates (§5: a module doc is what the
module owns and its one invariant — the proofs and the incident history go
here).

## The seam, the errors, the tests

**`Error` is one enum per crate, and its `Display` IS the old string.** The
controller matches token prefixes off `CommandResponse.errors`
(`entity_not_found:`, `PartitionsWithoutKeyedProcedure:`) and every veneer
suite asserts substrings of `flz_core_last_error`, so every String-carrying
variant renders as `{0}` and `error.rs`'s one test pins each variant against a
producer's real text. Design D3's five `#[from]` nestings (`RestoreError`,
`SendError`, `BuildWorkflowError`, `CommandDecodeError`, `TimerRefusal`) were
NOT written: no site ever turned one of those into a `Result<_, String>` —
each is consumed structurally or prefixed into `errors` — so the variants
would have had no producer. What does nest is the transport (`Error::Transport
(#[from] flz_transport::Error)` plus a `From` for each of its two halves), the
only conversion the control-unit sites actually made (`.map_err(|e|
e.to_string())` on `SocketDirError`, `?` on `quic_addresses()`). Two String
shapes stay on purpose: the DLQ ladder's `attempt_step` (its error IS the
`DlqEntryBody.error` wire string) and `ProcedureDispatcher::snapshot_frame`
(a trait `sdk/rust` implements — area 3); `channel.rs`'s four closure types
are deferred by name in D3. The C ABI renders the enum with `.to_string()` at
its four `ctx.fail` sites until area 2 maps it.

**`flz_transport::Error` is hand-written, not `thiserror`.** The crate has no
`thiserror` dependency and `SocketDirError` already hand-writes `Display`; a
dependency for ten lines is the wrong trade. It does implement
`std::error::Error` because thiserror's `#[error(transparent)]` in `flz_core`
needs the inner type to.

**The refusal strings are a contract, and one of them is a marker.** The
narrative-marker gate counts `no longer` in a comment; the QUIC list's
`FLEZHA_QUIC_ADDRESS is no longer honoured` refusal is a STRING (and the test
that pins it), so the gate does not see it and it must not be rephrased —
it is what the operator reads.

**`register.rs` went over the ceiling by being made typed.** Wrapping 36
refusals in `Error::Declaration(..)` added the lines rustfmt needs, 804 → 818;
the host payload's journal codec and the two host-held-state seams
(`host_serialize_unreachable`, `host_deserialize_unreachable`) belong beside
`HostPayload` in `host/entity.rs` and moved there, 818 → 775.

**Coverage decides a test deletion, and the diff is on function hits.** The
three per-SDK header pair tests and the three per-SDK declaration tests became
two sweeps over `SDK_LANG_ALLOCATED` (one per format byte: Kryo, MessagePack,
JSON) that cover strictly more pairs; the `catch_unwind` sweep in
`command/mod.rs` was deleted. `cargo llvm-cov --lcov` before and after, diffed
per file on `DA:` lines and `FNDA:` functions: every line that lost coverage
sits inside a deleted test's own body (the test modules start at
`command/mod.rs:287`, `header.rs:288`, `frame_buffer.rs:315`; the lost lines
are at 375+, 479+ and 403+), and `decl.rs`, `register.rs`, `registry.rs` lost
nothing. The lcov files and the diff script are in the session scratchpad
(`k1/b10/`); the evidence in the report is what the supervisor re-derives.

**The test helpers that were copies.** `test_support.rs` (`#[cfg(test)]`)
holds `spin_until` (one 5 s deadline — `mailbox.rs` had 2 s, `activation_tests`
5 s), `stream_pair() -> (Arc<FlezhaStream>, UnixStream)` (the shape all seven
`UnixStream::pair()` sites wanted: the SDK end wrapped, the controller end
raw) and `saturate(&mailbox, item)` (push and assert full; the host test's
slot variant is `saturate_slot`, which claims inflight after it).
`shutdown.rs`'s hand-rolled `decode_frame` was a copy of
`payloader::extract_payload` and reads through it now.

## Names and docs

- `TimerSession.now` → `now_ms` (the field and the `session(..)` parameter;
  the `now()` accessor keeps its name — `sdk/rust` calls it),
  `TimerSession::refused` → `note_refused`, `EntitySlot::last_activity_at` →
  `last_activity_ms` (one `sdk/rust` test reads it), `uds_driver`'s `now`
  parameters → `now_ms`, `persistent_id::derive_or_load` → `load_or_derive`.
- `flz_core`'s doc density went 25.0 % → 14.8 % (`doc_lines` 4 429 → 1 980) by
  the charter's rule applied file by file: a `///` states an invariant, a
  constraint or a why; anything that restated the signature, narrated a
  decision ("built here rather than … because") or cited an incident went, and
  the proofs below moved here. Module docs are all ≤ 15 lines; `workflow/
  mod.rs`'s doctest left with them (area 3 owes it a home in `sdk/rust`).
- `frame_buffer::feed` (69 lines) is `feed` + `step`: the loop stays, the
  state transition is one function whose `match` still shows every state.
- **The bench scaffolding lives in `flz_core::bench_support`** behind a
  `bench-internals` feature `flz_core_ffi` enables through its
  dev-dependency, so `cargo bench -p flz_core_ffi --bench host_loop` needs no
  flag: `stream_pair` (the test helper, exported under the feature),
  `ok_effect` (the host's `empty_effect`), `measurement_secs`, `fan_in` (the
  regression twins' dispatch loop, a fresh metadata map per message because
  that allocation IS part of what the row measures) and `Settle`, a
  `Condvar` count the fetchers advance and the bench waits on — the bench's
  one `thread::sleep` was a 1 ms polling `wait_until`. `sdk/rust`'s benches
  still carry their copies; area 3 moves them onto this module.

## Narrative moved out of the crates

### `mailbox.rs` — the wake protocol and its proof

- Producer calls `try_push(item)` → on success, CASes `inflight false→true`.
  On a winning CAS the caller schedules the entity's round on the poll core;
  on a losing CAS another drain is already in flight and will pop the new
  item.
- The drain loop pops items until the queue is empty, stores `inflight =
  false`, then re-checks `is_empty`. If a producer's push raced our store
  (their CAS failed because we still held inflight, but their push made it
  onto the queue), the re-check sees the new item and we re-CAS to resume
  ownership.

```text
T1 (drainer):                    T2 (producer):
  pop() -> None                    push(item)            // succeeds
                                   CAS false→true -> FAILS (still true)
  store(inflight = false)
  pop() -> Some(item)  <-- rescues the missed wake
```

This is the standard "actor dispatch" pattern used in Erlang's run queue and
Akka's Dispatcher; it remains correct under `ArrayQueue`'s lock-free push/pop
because the queue's internal ordering guarantees that a successful push is
observable to a subsequent `is_empty()` / `pop()` on any thread.

`wait_for_room_until` removes its entry on every exit path because an entry
whose thread has moved on still consumes a `serve_one` the next live waiter
was owed, and each consumed wake makes that waiter's next timeout more likely
— a deficit that only ever grows, until every wake is absorbed by the dead
and a live producer advances one 50 ms park at a time.

### `poll_core/mod.rs` — the rationale the module doc carried

- A host that gives none of the three answers (`complete`,
  `retry_after_failure`, `abandon`) retires that entity for the life of the
  process, which is why every host makes the answer structural
  (`docs/sdk/poll_core.md`); the core reports how long a handout has been out
  and never reclaims one on a timer, because from the core a slow Procedure
  and a lost handout are identical.
- The engine took over from the drain loop everything except user code:
  consumed-floor skip, replay-slice buffering and event-time ordering,
  late-input admission and classification, event-time due-timer generation,
  stale-timer validation, and the retry→park ladder's counting, backoff and
  re-hand. The host half runs one attempt per item and reports the
  disposition.
- An event-time class is handed ONE input per activation: a chained timer
  registration lands only at completion, so batching more would make
  due-firing interleaving depend on batch timing, and a correction replay
  must fold firings at the same positions the live run did.
- The one park the core keeps is the transport reader's on the entry path —
  a core-owned thread with no producing entity, whose stalling carries
  ingress backpressure to the controller.
- The deferred-wake list is serviced by whichever poller next enters or
  wakes, and its nearest entry shortens that wait. `max_inflight` is
  decoupled from any host's thread count; `wait_starvation` is the other half
  of that contract, in the same bring-your-own-thread shape as `poll`.
- `starve_cv` is deliberately NOT signalled by `schedule` or by a poller
  leaving: those are the hottest paths in the engine, and one futex wake each
  would cost more than the whole feature is worth.
- `hold_or_release_pushes` publishes `push_gated` on EVERY entry because a
  consumer freeing room can empty the holdback at any instant — including
  between the caller testing `push_deferred` and the callee re-testing it —
  and a claim published only on the deferred branch would leave that
  interleaving with a `swap(false)` of `false` meaning "nobody ever claimed
  it" rather than "somebody else is releasing"; both sides then answer "held"
  and the entity's gates leak: `drain_active` stays true with no handout
  outstanding, and every later push queues behind a round that never runs.

### `entity.rs`, `admission.rs`, `credit.rs` — the 2026-06-10 freeze

`DISPATCHES_COMPLETED` rides every `data_heartbeat` so the controller can tell
"runner alive but dispatch wedged" from "runner healthy" — the 2026-06-10
freeze was invisible to every heartbeat-level signal. `BLOCK_WARN_AFTER`
exists because silence on a full mailbox holding a producer for seconds is how
that freeze hid. The credit window's guardrails come from the same lesson: the
park lives in the data send path only (`arm`/`grant` run on the coordinator
and never block, so heartbeats and grants keep flowing while senders are
parked), it is a `Condvar` wait loop with a 50 ms recheck so a lost grant
degrades to a retry loop with warnings, and `CREDIT_PARKED` advances on every
wait iteration so a credit-parked runner classifies as fed-but-parked.
`push_deferred` is an atomic read rather than a peek under the holdback's
lock because it is read on every push: measured, the load is free (14.39 ns
against 14.79 ns for a bare `try_push`) and a mutex acquisition costs +64 %.
`owe_room` is the ONE registration both stand-down paths use because the
first draft had the recheck on the deferral and not on the re-drain, which is
a lost wake costing a full fallback window.

### `coordinator.rs` — `handle_reconnect` and the May 25 silent freeze

The previous behaviour cleared `shared_io_streams`, `operative_registry` and
the `runner_id`; that is what caused the May 25 silent freeze (the runner sat
empty while the controller still believed the binding was live). The
`runner-controller-connection-healing` capability requires: the registry and
the streams are retained across a reconnect (in-flight per-Entity mailbox
work keeps draining), the existing `runner_id` is kept until the controller
assigns a fresh session token via `AssignRunnerId` on the re-opened stream,
and a `tracing::warn!` line carries the upstream-derived reason so kubectl
logs surface every reconnect.

### `workflow/driver.rs` — the saga ordering and the resume rule

One compensation-intent marker per entity that ran, so a crash mid-compensation
knows what still needs undo — only for compensable procedures (a hookless proc
is never rolled back). The proc-boundary commit journals the stage's
completion + handoff on the controller and blocks until the durable ack — a
child procedure must not race ahead of the committed record — ordered AFTER
the intent markers so a crash between marker and ack resolves as "proc not
committed → re-run" (at-least-once). Without a proc journal a non-empty
reconstruction always means the operative terminated abnormally with some of
its undo outstanding; WITH a journal, intent markers also accumulate during
normal forward progress, so only a shipped compensation-done marker — written
exclusively by an undo pass — still proves compensation was in flight;
otherwise the journal decides where forward execution resumes.

### `workflow/compensation.rs` — why plain reversal is reverse-topological

Completion order is topological over the procedure tree — the driver records
a level of the frontier before releasing its children, and the journal-rebuilt
path sorts by `stage_idx`, which is BFS-numbered — so reversing it is exactly
the reverse-topological order a tree saga requires: no procedure is undone
before its compensated descendants, and independent sibling subtrees unwind
in an unspecified relative order. The rewind undo differs from the
crash-resume pass in two ways: it does not consult the per-entity
compensation-done markers (an entity that ran in two rewound iterations must
be undone once per iteration), and it journals exactly one `compensated`
entry per procedure, even an empty one for a hookless procedure — that entry
is the durable proof the procedure is undone, and without it the journal
would re-plan it on every restore.

### `host/activation.rs` — why the fold comes after the edge pushes

An edge output is the one output kind with no durable copy (it is excluded
from the transactional outbox), so the only thing that can re-produce it is
the producer re-running its input. Fold first and a crash in the ~5 ms until
the flusher runs leaves the offset durable, the replay floor skipping that
input, and the consumer never fed — a permanent loss under a contract that
promises effectively-once. The typed path emits from inside `ctx.emit` during
`run()`, before the effect block exists, and already satisfies this.

### `routing.rs` — why the end of a replayed slice is a broadcast

An input routes to exactly one Entity by key, so a marker on the slice's last
input would reach one Entity and leave every other Entity that buffered slice
members holding them forever.

### `shutdown.rs` — the timing contract

The SDK awaits `RunnerShutdownAck` per operative with a 2 s per-operative
timeout, parallelised so the total wait is bounded by 2 s (Kubernetes' default
`terminationGracePeriodSeconds` is 30 s). The per-operative serializer step is
capped at the controller's `cross_node_request_timeout` ceiling (10 s); a
class with a multi-second serializer overrides `bin_apply_state` to stay
below it. On timeout the SDK exits unconditionally and never blocks its
container's grace period. SIGKILL bypasses the handler by definition; the
controller falls back to its most recent periodic snapshot.

### `state_sink.rs` — the flood-latch regression

Before the latch re-armed, it was cleared only by `install_global_sink`, so
if the `Reconnect` was dropped (the runner was `Stopped`) or the reconnect
failed, no further request was ever made: every subsequent state change was
silently absorbed by the ring for the life of the process while the control
channel stayed healthy — the runner looked alive from every angle and never
produced anything again. `FLEZHA_RECONNECT_REQUEST_RETRY_MS` is the re-arm.

### `flusher.rs` — the frame layout (also in `docs/sdk/state_persistence.md`)

```text
[entity_id_len: u16 BE]
[entity_id_bytes]   // [rc_len:u8][rc][sh_len:u8][sh][pk_len:u16 BE][pk]
[seq: u64 BE]
[header: 16B]
[body_len: u32 BE]
[body]                 // change_bytes (or snapshot_bytes for compactions)
[crc32: u32 BE over body]
```

The structured `entity_id_bytes` carries `(operative_class, procedure_id,
entity_key)` so the controller routes the record to the right per-Entity WAL
file without a side table; the trailing CRC32 covers the body so the
controller validates framing without re-decoding the header.

### `workflow/mod.rs` — the authoring example (area 3 owes it a `sdk/rust` home)

```rust
struct Greet;
impl flezha_runner::WorkflowOperative for Greet {
    fn factory_id() -> &'static str { "greet" }
    fn run(ctx: &mut flezha_runner::WorkflowCtx, args: &str) -> Result<(), flezha_runner::WorkflowError> {
        ctx.complete(format!("hello {args}").into_bytes());
        Ok(())
    }
}
flezha_runner::Flezha::create_runner("my_app")?.define_workflow::<Greet>()?.run()?;
```

The lifecycle it introduced: `run` sugar (one implicit Procedure/Entity);
structured (`procedures` on the same `ChannelBuilder`, a count-based barrier
per procedure); exit triggers (chain quiescence, `WorkflowCtx::complete`, a
loop's terminal predicate, `max_duration`, an external `flz operative stop`),
all converging on one `WorkflowCompleted`; the inbox (`WorkflowCtx::recv`).

## Measurement notes (segment C)

- **Row 1 is behind by a small, consistent amount, and it is real.** The
  typed dispatch bench measured +1.24 % (B5), +0.63 % (B7) and +1.36 % (B11)
  on the median, and arm B was the slower leg in every one of the fifteen
  pairs — inside the 3 % rule, above its 0.90 % floor. Nothing in the three
  segments can be named as the cause from the rows alone; the path is
  `sdk/rust`'s, and area 3 owns finding it.
- **Row 5 measures the erased path and it gained**: 8.09 → 7.09 allocs/elem
  from batch 4's `EntitySlot.instance_id: Arc<str>`, and the A/B medians sit
  within the row's 4.13 % floor at every checkpoint.
- **Row 6 needs both artifacts swapped.** The whole-system round trip runs
  the controller's NIF, so a batch that rebuilt `flz_nif` swaps
  `priv/native/flz_nif.so` per arm beside `target/release/rust_runner`, each
  verified by sha before and after every run; the script restores the tree's
  own copies on exit, whatever happens.
- **Java `mvn verify` after `build-dist.sh` fails at the shade step** with
  `duplicate entry: META-INF/THIRD_PARTY_NOTICES.md` — the shade plugin
  reuses stale `target/` output, which `build-dist.sh` already documents by
  running `clean install`; the SDK suite is `clean verify`.
- **The Ruby veneer needs clang's builtin headers, and this box has gcc's.**
  `libclang1-18` without `libclang-common-18-dev` dies inside cargo as
  `'stdarg.h' file not found`; `BINDGEN_EXTRA_CLANG_ARGS=-I/usr/lib/gcc/
  x86_64-linux-gnu/13/include` is what builds it here.
- **The second system-tier red is the 0b first-input race on the restore's
  re-forward path**, not the kernel: `effectively_once_checkpoint_e2e` passed
  56 times under probes and then failed with three `[:flezha, :stream,
  :drop]` events in one millisecond right after `replay_slice/4 -> :ok` —
  offsets 3..5 re-forwarded onto a data stream the rebound runner had not
  bound yet. The controller never re-drives a dropped re-forward. Same
  defect, one more path; recorded, not fixed.
- **`flz_core` production LOC rose** (11 666 → 12 563 by the corrected
  classifier) while `flz_transport` fell (1 961 → 1 743) and `flz_nif` held
  (2 065 → 2 048): the seam's core half (+315, its typed twin still in
  `sdk/rust` until area 3 ports it), the `error` enum (+56), the
  `bench_support` module (+90), and the owner splits' module heads, imports
  and re-exports. The proposal's "≈ −250" for `flz_core` did not survive
  contact; the supervisor re-baselines at the flip.
