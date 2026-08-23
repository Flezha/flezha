# Per-Entity timers (`entity-timers`)

- **`ctx.timer_at_event_time(ts)` / `ctx.timer_after(d)` + the two cancels
  (Rust) — `timerAtEventTime` / `timerAfter` / `cancelEventTimeTimer` /
  `cancelProcessingTimeTimer` (Java)**, handled by `Procedure::on_timer` /
  `Procedure.onTimer`. Registration returns the ABSOLUTE scheduled timestamp;
  that value IS the cancellation handle. Pending timers are a SET, so a
  duplicate registration is a no-op that produces no second durable op.
- **THREE REUSES carry the whole feature**, which is why it needs no new store,
  no new control message and (almost) no controller change:
  (1) **the Change → WAL → replay fold** makes the pending set durable —
  registration / cancellation / firing are records in the Entity's OWN change
  log, so crash-resume, replay and retention are free;
  (2) **the per-Entity event-time high-water** (from
  `stream-late-input-correction`) is the event-time trigger — no scheduler,
  no watermark propagation;
  (3) **the nil-offset effect record** (`effectively-once-checkpoint-granularity`
  made `consumed_offset: Option<u64>` first-class) carries a firing, because a
  firing consumes no input offset and every controller reader already tolerates
  `nil`.
- **Two record kinds, both stored VERBATIM by the controller.** Tier-off:
  `FORMAT_ID_TIMER_OPS = 3` under the EXISTING `entity_state_change`
  payload-id, body a postcard `Vec<TimerOp>` — the controller stores, truncates
  and re-ships it exactly like a change record and never decodes it. Tier-on:
  `EffectRecordBody.timers` (new trailing field), so a firing's Changes,
  outputs and the fired timer's removal are ONE record. That field is the ONLY
  Elixir change: `Wire.decode_effect_body/1` had a strict trailing-byte check,
  so it and `encode_effect_body/1` learned `timers` (+ the `wire_test.exs`
  fixture, shared with `flusher.rs`; the Java `EffectRecordTest` went with
  the kernel mirror).
- **`fired_at` is the SCHEDULED timestamp and `ctx.now()` EQUALS it inside
  `on_timer`.** A chained `timer_after` therefore schedules off the timer that
  woke it, and a replay observes the same value. `timer_after(d)` anchors on
  `ctx.now()`, not an ambient read — that is what makes "replay registers the
  same timers" true.
- **GOTCHA — a quiet Entity NEVER fires its event-time timers.** Nothing
  advances its clock. Inherent to event time (Flink behaves identically with an
  idle source); the processing-time kind is the answer to "nothing has
  arrived", and it is what a session timeout / idle flush / join expiry wants.
- **Event-time firing rides the dispatch path; processing-time rides the
  MAILBOX.** The per-Operative `TimerWheel` was later replaced by
  `sdk-deferred-wake`: a processing-time timer is an entry on the poll core's
  ONE process-wide deferred-wake list (`flz_core/src/timer.rs` — no timer
  owns a thread), keyed
  `(procedure, entity_key, scheduled_at)`; on expiry it PUSHES onto the target
  Entity's existing mailbox rather than invoking `on_timer`, so the
  single-active-drainer gate that already serializes inputs serializes firings
  too. No second lock. Both SDKs.
- **GOTCHA — `admit_and_track` had to start advancing the high-water for EVERY
  event-time-clocked class**, not only a correcting one: it was gated on
  `correction.is_some()`, and event-time timers fire against that mark. It is
  now gated on `<S::Clock>::IS_EVENT_TIME` instead, which is also strictly more
  correct (a wall/logical value must never be folded into an event-time mark).
- **GOTCHA — compaction would have silently eaten the pending set.** A snapshot
  covers `S::State` alone, and `truncate_up_to(snapshot_seq)` deletes every op
  below it. `snapshot_frame` / the host crossing
  (`flz_core::host::snapshot_frame_crossing`) therefore re-state the
  FULL set as a fresh record AFTER reading the seq, so it lands at
  `snapshot_seq + 1` and survives; a crash between the two leaves the
  untruncated log, which still holds the originals. Ordering the other way
  round would delete the restatement.
- **GOTCHA — a `NoChange` Procedure is not registered with the flusher**, so
  its timer ops would never drain. Rust drains them synchronously instead
  (`Self::flusher_registered()`) rather than registering every Entity and
  spawning the flusher thread for programs that will never produce a record.
  On the Java/host path timer ops are buffered ONLY when the slot has a
  flusher entry (`host.rs`) — a non-persistent (all-`NoChange`) Java class
  gets none, so its timer ops are not buffered at all; there is no
  synchronous-drain fallback there.
- **A registration made during a REPLAY is registered** — the deliberate
  opposite of `ctx.spawn_workflow`'s `SkippedOnReplay`. A timer is internal
  state; skipping it would rebuild a pending set the live run never had. Both
  calls sit on the same `ProcedureCtx`, so it is a trap worth stating.
- **`TimerPolicy` is what a Procedure declares about its timers, resolved
  ONCE** (bound, procedure/clock names for refusals, whether the Clock is
  event-time, and the counter set) instead of four arguments threaded to every
  session. `TimerPolicy::session/firingSession` is the only way to open a
  `TimerSession`, in both SDKs. The arming seam (`apply_ops` /
  `rearm_restored`) now lives ONCE in `flz_core/src/timer.rs`
  (`sdk/rust/src/entity/timer.rs` is a re-export shim) — the old
  Rust-vs-Java arming asymmetry is gone.
- **A failed firing parks like a poison input** (same retry ladder), and the
  removal is committed EITHER WAY — a poison timer parks once instead of firing
  forever, the same reason a park closes a checkpoint interval. `TimerSession`
  works on a COPY of the pending set so a retried attempt leaves no
  registration behind; `commit` is where the counters move.
- **FIXED IN PASSING (Java)** — `HeaderTemplate.decodeHeader` rejected any
  `format_id != KRYO`, so a Java Entity restoring a change log containing its
  OWN effect records (format 2) threw `formatNotSupported`. It now accepts 2
  and 3 and the restore rebuild branches, folding an effect record's
  enclosed changes. (The Java classes named here were later absorbed into
  the core — header decode lives in `flz_core/src/header.rs`.)
- **Refusals**: `EventTimeTimerWithoutEventTimeClock` (an event-time timer
  under a non-event-time clock), `TimerCapExceeded`
  (`max_timers_per_entity()`, default 10 000), `TimersUnavailable` (a workflow
  stage or a `compensate` hook — structural: those contexts carry no session).
- **LIMITS (documented, not fixed)**: dynamic eviction discards an Entity's
  timers with its state (legal, D6); a HOT-SWAP migration does not carry them
  (the handover ships JSON `State` and suppresses the WAL restore — they are
  still on disk, so a later restore rebuilds them); workflow operatives have no
  timers (they have `wake_after`); resolution is milliseconds.
- **Observability**: per-Procedure `TimerCounters` (pending / registered /
  fired / cancelled / refused) plus `timers_armed` + `timers_fired` in every
  `data_heartbeat`, beside `dispatches` / `blocked_parks`.
- **Tests**: `sdk/rust/tests/entity_timers.rs` (own `[[test]]`,
  `bench-internals,testing`; cases that arm real deadlines take
  `timer_test_guard()` because the state sink is process-global; Java timer
  coverage now lives inside `ActivationDispatcherTest`), the
  `TimersUnavailable` unit case in
  `dispatcher.rs`, and e2e `test/system/entity_timers_e2e_test.exs` — fires +
  re-seat-from-disk on BOTH runtimes (fixtures `session_timeout` /
  `java_session_timeout`, timeout from `TIMER_TEST_TIMEOUT_MS`). GOTCHA: the
  e2e's re-seat uses `TierE2EHarness.force_fresh_rebind` (kill the Overwatch),
  because `AddOperative` rebuilds the SDK-side instance — fresh slots, fresh
  timer state, no in-memory pending set — so ONLY the restore can still close the
  session. Killing the runner process instead does NOT rebind within the test's
  budget.
- **FIXED IN PASSING (controller, pre-existing from `entity-state-live-query`)**
  — `await_restore_init_state/2` (now on `Flezha.State.Commands`) still called
  `GenServer.call(control_unit, {:send_control, command}, timeout)` after that
  change reshaped `handle_call` to `{:send_control, command, want_metadata}`.
  EVERY awaited `RestoreInitState` therefore crashed the ControlUnit with a
  `FunctionClauseError`, taking the runner connection with it — invisible to
  `mix test` (the unit suite never drives a real restore) and fatal to
  `late_input_correction_e2e`, `effectively_once_checkpoint_e2e` and the
  workflow branching resume. It now goes through `ControlUnit.send_control_await/3`,
  which is the ONE awaited-round-trip seam and owns the reply-ref bookkeeping.
- **Docs/examples**: `docs/sdk/entity_timers.md`, plus sections in
  `docs/sdk/authoring_contract.md` (3.4), `docs/sdk/rust_operative_architecture.md`,
  `docs/resilience/data_durability.md`, `docs/sdk/protocol_spec.md`,
  `docs/glossary.md`; `sdk/rust/examples/session_timeout.rs`, Java
  `E12_SessionTimeout`; `sdk/rust/public-api.txt` updated.

