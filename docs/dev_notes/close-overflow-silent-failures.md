# close-overflow-silent-failures — implementation notes

Five silent failures around the overflow path. Four were real at HEAD; the
fifth (the Elixir half of the declare-time refusal) had nothing to refuse.

## The measured probe table, and what it actually isolates

| probe | inline serve | pool base/max | result |
| --- | --- | --- | --- |
| producer emit | no-op hook | 1 / 1 | 0 of 20 delivered, wedged 15 s |
| producer emit | no-op hook | 1 / 16 | 20 of 20 in 0.74 s — the supervisor rescues |
| `promote_due` | no-op hook | 1 / 16 | **1 of 3, wedged 15 s**, 593 blocked pushes |
| `promote_due` | real hook | 1 / 16 | 3 of 3 in 0.65 s, 9 blocked pushes |

Same ceiling, same hook state, opposite outcome. `python-sdk`'s **D9 records
the starvation supervisor as the rescue for this family of wedges: true for the
emit case, FALSE for the wake case** — a promoter parks *inside* `poll`, so
`pollers_inside > 0` makes `starving()` answer `false` unconditionally and the
supervisor never observes anything. Amend D9 when `python-sdk` is next touched;
this change does not edit another change's design.

`close-silent-data-loss-defaults` noted "`pushes_blocked_total` rising where
`drops_newest_total` used to is the observable". **That was false when written**
— neither counter reached a heartbeat, an export or a log — and item 4 makes it
true retroactively.

## D1 — the wake list is the holdback, and `scheduled_at` had to move

The fix is `try_admit` (non-waiting) + re-record at `now + WAKE_RETRY_BACKOFF_MS`
(5 ms). Two things that are not obvious:

- **The key's due time and the firing's timestamp are different facts.**
  `Wake::Timer` gained `scheduled_at`, because re-recording under a new key
  would otherwise make `key.due_at_ms` — which the fire was stamped with — the
  *retry* time. That timestamp is what `PendingTimers` cancels by and what
  `ctx.now()` reads inside `on_timer`; the regression test asserts it survives
  the deferral, and that assertion is the one that fails if the field is
  removed.
- **Re-keying loses the disarm.** `disarm_timer(entity, scheduled_at)` and
  `disarm_interval_close(entity, deadline)` build the key from the ORIGINAL due
  time, so they miss a deferred entry. Harmless in both directions and already
  true before the fix (`take_due` pops before delivering, so a cancel racing a
  promotion always missed): a stale firing is discarded at staging because
  **the pending set is the authority** (`stage_popped_item`'s
  `timers.contains(kind, scheduled_at)` check), and a stale interval close is a
  no-op against a buffer with no open interval. Do not "fix" this by making the
  tiebreak carry the deadline — the BTreeMap is ordered by due time and that is
  the whole point of it.
- **A hot retry was the trap.** Re-recording at the SAME due time spins:
  `park_slice` shortens the poller's wait to the nearest due entry, so a
  still-due entry gives a zero wait. Strictly later is load-bearing, and the
  spec delta says so.

`crate::routing::in_activation` left `poll_core.rs` entirely; it survives only
in `sdk/rust/src/operative/poll_pool.rs` (the emit path, untouched).

## D2 — Java's supervisor and the JNI export

`FlzCore` is JNI, not the plain C ABI, so binding `flz_core_wait_starvation`
meant a new `Java_com_flezha_sdk_native_1_FlzCore_waitStarvation` export in
`flz_core_ffi/src/jni_glue.rs` — and therefore **all five committed native
binaries had to be rebuilt** (`scripts/build-dist.sh --host`, ~1 min, needs
`/home/moon/.local/share/flezha-zig-venv/bin/zig` on `PATH`). This was owed
anyway: every `flz_core` change in this set is compiled into those binaries.

`PollHost` references `FlzCore.STARVATION_*`, which are `static final int`
constant variables — javac inlines them, so `PollHostTest` still runs without
loading the native library. Do not turn them into anything else.

The ceiling refusal is **latched** (`atCeilingLogged`), reset on a successful
growth: a sustained wedge would otherwise print the same WARN every second
forever.

## D3 — the reorder is the whole fix; `pending_attempt` was not needed

`resolve_item`'s edge-routing block moved ahead of the flush-buffer fold. The
task list asked for the item's `InvocationEffects` to be held via the
`pending_attempt` seam; that is unnecessary because `deliver_host_output` is
**synchronous** — it parks until the push is admitted — so the local variables
already are the holdback and the record is assembled only after every push has
landed. A second holding place would have been the bloat RULE #1 forbids.

**The typed Rust path needed no matching edit** and nobody should go looking for
one: `EntityEmitSink::emit` runs inside `ctx.emit` during `run()`, before the
effect block is assembled.

The residual window this does NOT close: between the push landing and the
consumer's own record becoming durable, the value exists only in the consumer's
volatile mailbox, which `discard_queued_input_on_restore` (`entity.rs`) clears
on every tier-class restore. The controller re-forwards to the *entry* channel,
where the producer's own floor skips it. Closing it needs `OutputTarget::Edge`
in the transactional outbox — a separate, larger change.

## D4 — one block counter, not two

The proposal asked for BOTH "move the `BLOCKED_PARK_EPISODES` increment out of
the `blocked_since.is_none()` guard" AND "a new `pushes_blocked` heartbeat key".
Those are the same number: the guard sits in the same loop iteration as the
per-slot `pushes_blocked_total` bump, so a per-iteration global would have been
byte-identical to `blocked_parks`. **Collapsed to one counter**:
`BLOCKED_PARK_EPISODES` was renamed `PUSHES_BLOCKED_TOTAL` (the old name became
a lie), the wire key stays `blocked_parks` as the task required, and the
heartbeat gained exactly two keys, `drops_newest` and `drops_oldest`. The spec
delta was corrected to match.

`fast.rs`'s `send_data_heartbeat` was split: `heartbeat_metadata()` builds the
map, the caller writes it. That is what makes the key set testable without a
socket — the keys are a cross-SDK contract `stream_channel.ex` mirrors slot for
slot, and nothing pinned them before.

**The controller never read `blocked_parks` at all.** `@heartbeat_slots` in
`lib/flezha/runner/stream_channel.ex` carried `dispatches`, `credit_parked`,
`inflight` and `inflight_oldest_ms` only, so the key had been on the wire and
unread since it was added. It is mirrored now, along with the two drop counters.

`runner_pool_wedged` was the right existing verdict — **no new verdict was
added**. It gains one clause: `hb_blocked_parks` must also be flat. A frozen
dispatch count with the block counter climbing is healthy backpressure, and the
wedge remedy (a rebind) would only move the congestion.

Granularity is not identical across SDKs and the difference is deliberate:
`flz_core` counts per wait iteration (a parked producer keeps the number
moving), the Elixir SDK counts per blocked `enqueue` call (its caller is
deferred, not looping). Both advance under active backpressure, which is what
the classifier reads.

### One consequence of counting a force-push

`note_admitted` (the admission bookkeeping `try_admit` and the `DropOldest`
arm now share) also stamps `last_activity_at`, which `DropOldest` never did.
That is the point: the item WAS admitted, so a dynamic-mode Entity kept
saturated under `DropOldest` no longer looks idle to the reaper. The old
omission followed from `pushes_total` not counting the force-push, which is the
same defect.

### The inversion, re-measured

`drop_oldest_stress_admitted_never_below_processed` is the probe, kept: 8
producers x 2 000 sends into a capacity-2 `DropOldest` mailbox against a live
drain, four rounds. With the fix, `admitted == sent == 16 000` and `processed`
lands at 1 226-1 597 every round, the gap accounted for by
`drops_oldest_total`. Revert just the `note_admitted(slot)` in that arm and
round 1 reproduces the proposal's finding exactly: `admitted=989`,
`processed=1105` — **processed exceeding admitted**. A unit assertion cannot
show this; the inversion only exists while producers race a live drain, which
is the only thing that makes `force_push` fire.

## D5 — the correction arm is live in exactly one place

`correction_window` requires `effectively_once` in the Rust builder
(`TopologyIssue::CorrectionWithoutTier`) and in the Java builder, so in both
typed builders the correction arm is **subsumed** by the tier arm — adding it
there would be dead code, and only the tier arm was written. `flz_core`'s
`validate` does NOT enforce correction-without-tier, so both arms are live
there and both are implemented.

**Elixir carries neither half.** `Flezha.SDK.Channels` / `Topology` have no
`effectively_once`, no `persistent`, no `correction_window` — the Elixir SDK is
a v1 SDK with no durability tier at all — so there is nothing for `on_full/2`
to contradict. The proposal's Cross-SDK table ("Elixir | Item 5 only") is
**stale**: the refusal is vacuous, not deferred. When the Elixir SDK gains
`effectively_once`, the refusal ships with it. Elixir did take item 4 (its own
counters and heartbeat keys), which the same table said it needed nothing for —
`tasks.md` 4.5/4.6 were right and the table was wrong.

The replay exemption is one line at the head of `routing::enqueue`, where the
`replay` flag is already inspected to set `replay_open`. No new plumbing
carries it.

## Stale claims found in the proposal

- **Line numbers**: every cited line had shifted; the claims themselves held
  except as noted below.
- **`docs/sdk/poll_core.md` and `rust_operative_architecture.md` both described
  the retired inline-serve-on-promotion behaviour** as current. Rewritten.
- **`sdk/rust/tests/entity_timers.rs::a_firing_into_a_saturated_consumer_drains_it_inline`**
  codified the REMOVED requirement and would have hung forever under the fix
  (it steals `inflight` so nothing else can ever drain the slot). Rewritten as
  `…waits_for_room_without_holding_a_poller`: it now asserts the firing is NOT
  delivered while the mailbox is full, then frees a slot and schedules the
  round.
- **`drop_oldest_evicts_head_and_bumps_oldest_counter`** asserted
  `pushes_total == 2` after four pushes, with a comment stating the wrong
  behaviour as intended. Now 4.

## The archive refused the delta twice — both were staleness

The delta was written before `rename-block-to-drop-nothing` landed and before
anyone checked the main spec's shape. `openspec archive` aborts cleanly (no
files changed) on each, which is how they were found:

1. **`## REMOVED Requirements` named a SCENARIO.** "A due firing into a
   saturated consumer drains it" is a `#### Scenario:` nested inside
   "A DropNothing push drains the consumer inline only from a pool thread" —
   the very requirement the delta already MODIFIES. A `MODIFIED` block replaces
   the whole requirement, scenarios included, so the retirement happens by
   omission there and the `REMOVED` block had to go. Both surviving scenarios
   were already carried across, per the trap `rename-block-to-drop-nothing`
   documented.
2. <!-- overflow-naming-record:begin -->**The `MODIFIED` header still said `A Block-policy push…`.**<!-- overflow-naming-record:end --> The main spec
   says `A DropNothing push…`. A `MODIFIED` header is matched verbatim, so a
   delta written against a pre-rename spec cannot apply.

Check both before archiving: `grep '^### Requirement:'` the target spec and
diff the titles against the delta's.

## Test map

| What | Where |
| --- | --- |
| the wake defers instead of parking (timer + interval close) | `sdk/rust/src/runner/poll_core_tests.rs` — `a_due_firing_into_a_full_mailbox_defers_instead_of_parking`, `a_due_interval_close_…` |
| the deferred firing keeps its armed timestamp | same, the `assemble_timer` assertion |
| end-to-end firing into a saturated consumer | `sdk/rust/tests/entity_timers.rs` |
| the offset never precedes the edge push (single + fan-out) | `flz_core/src/host.rs` `edge_output_ordering_tests` |
| the declare-time refusal (host path) | `flz_core/src/host.rs` `dropping_policy_decl_tests` |
| the declare-time refusal (Rust builder) | `sdk/rust/src/operative/builder.rs` |
| the declare-time refusal (Java builder) | `BoundedMailboxTopologyTest` |
| a replay-slice member is never shed | `sdk/rust/tests/operative_dispatch.rs` |
| the heartbeat carries every overflow counter | `flz_core/src/fast.rs` `heartbeat_tests` |
| the process-wide aggregates move with the per-slot ones | `sdk/rust/tests/operative_dispatch.rs` (DropNewest / DropOldest) |
| `blocked_parks` advances per iteration, not once | `sdk/rust/tests/operative_dispatch.rs` entry-park body |
| backpressure is not a wedge | `test/flezha/convergence/stall_classifier_test.exs` |
| Java's supervisor grows, stops at the ceiling, joins on close | `PollHostTest`, `FlezhaEnvTest` |
| Elixir's drop counters agree with the shed count | `test/flezha/sdk/dispatch_test.exs` |

`sdk/rust/tests/entity_timers.rs` and `operative_dispatch.rs` need
`--features "bench-internals testing"`.

## Two traps in the ordering test

- **The flusher races any assertion about buffer contents.** It drains every
  registered buffer every ~5 ms — which is exactly the window the bug lived in.
  The first draft read `bytes.effects` and passed under a REVERT, because the
  record it should have caught had already been shipped. The test now calls
  `detach_from_flusher` (`EntityFlusher::deregister`) on the producer, so the
  two states either side of the push are actually observable.
- **The wake-deferral tests do not prove the PARK case.** The inline-serve hook
  is a process-wide `OnceLock` and something in the `flezha_runner` test binary
  installs it, so a revert of item 1 serves the consumer inline there rather
  than parking. Both tests still fail on the revert (the firing lands while the
  mailbox is full), which is what the regression pins. The park case — no hook,
  which is every C-ABI host — is what the measured probe covered.

## The controller had to grow a reader, not just a slot

The first pass mirrored `drops_newest` / `drops_oldest` into `hb_drops_newest` /
`hb_drops_oldest` and stopped there — and nothing in `lib/` read them. The
stall classifier reads only `hb_blocked_parks`, and
`OtpBackend.TransportSource` emitted a hand-picked subset that did not include
them, so the counters were carried to the controller and dropped on the floor:
exactly the "written by the kernel and read by nothing" defect item 4 exists to
close, reproduced one layer up. `transport_source.ex` now emits all three
(`mailbox_blocked`, `mailbox_drops_newest`, `mailbox_drops_oldest`) beside
`credit_parked`, which is the path the spec's "surface them on the same path"
names.

## Refactor pass (post-implementation)

- `EntitySlot::note_blocked` / `note_shed` in `entity.rs` are the only places
  an overflow outcome is counted: each moves the per-Entity counter and the
  process-wide aggregate together, so a new drop site cannot bump one and
  forget the other. `note_shed` also owns the drop report — the evicted item's
  identity exists nowhere else once `force_push` hands it back.
- `routing::note_admitted` is what `try_admit` and the `DropOldest` arm share.
- `promote_due` has ONE tail (`admit or defer`) instead of the rule living
  separately in each wake arm.
- `fast.rs::heartbeat_metadata` is one table, so the key set reads as the
  cross-SDK contract it is; `stream_channel.ex` derives each `hb_<key>` slot
  from the key rather than listing both, and `stall_classifier.ex` aggregates
  from two slot lists (summed vs reported) rather than two copies of the field
  set.
- `wait_until` moved into `poll_core_tests::fixtures`, the crate's existing
  shared test-helper home, and `poll_pool`'s copy is gone.
- `PollHost.startFetcher` is shared by the replacement path and the
  supervisor's growth; the supervisor survives a throwing `waitStarvation` the
  way a fetcher survives a throwing `poll` (Exception → one bad turn, Error →
  end the thread), because a supervisor that stopped on a transient failure
  would be the very wedge it exists to close.
