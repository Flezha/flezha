# Test-suite refactor (sleeps, refutes, async) — implementation notes

Per-change notes for `refactor-0-tests`. The design (D1–D6) and the flake
population table live in the archived change; what follows is what was only
learnable by doing it.

## The one production seam

`flz_core/src/poll_core.rs` gained `PollCore::pollers_inside/0`, a read of the
counter the starvation supervisor already keeps, so the poll-core tests settle
on "a poller is inside the host" instead of sleeping. Nothing else in `lib/`,
`flz_core/`, `flz_transport/` or the SDKs changed behaviour.

## The first input after `start_operative/2` is dropped, and nothing re-drives it

`OperativeManager.start_operative/2` returns once the runner has acknowledged
`AddOperative` over the CONTROL channel. The runner opens its primary DATA
stream separately: `StreamChannel.init/1` publishes `{nil, liveness_ref}` under
the channel's name and the real ref only lands at `handle_call({:bind, ref})`.
Until then `StreamChannel.io_in/3` answers `{:error, :no_stream}`, bumps
`nil_drops`, fires `[:flezha, :stream, :drop]` — and a `send_io/4` CAST has
nobody to tell. A tier-on input is journaled (`Ingest.verdict/2` answers `:ok`
on `:no_stream` because the log holds it) but nothing forwards it when the
stream binds a moment later, so the first changelog never arrives.

This one race was the WHOLE e2e flake population once the sleeps were gone
(F6, F7, the tier-off DLQ case, and — under a combined unit+tier run — schema
evolution, compaction, entity timers, credit flow), and it was proved, not
guessed: a `-r` probe attaching to the drop telemetry saw 38 drops in one
combined run, attributed to exactly the failing operatives (20 of compaction's
50-cast burst, entity_timers' one `hello`), while the Rust runner never printed
`operative instance ... not found` — the frames never left the controller. The
`send_sync_io/5` doc had named the race all along; so had `sdk_smoke`'s
`send_until_forwarded` re-send loop and `schema_evolution`'s state-read
readiness wait, neither of which is a settle on the thing that drops.

The settle is `NodeTestHelper.await_stream_bound/2`:
`GenServer.call(overwatch.channel, :stream_ref_status) != nil` — the same
probe the acceptor uses to find a rebindable channel. It goes after every
`start_operative` that is followed by a send on a runner booted in the same
test or module. The sink-activation hypothesis the population table first
recorded is REFUTED: `Plugins.activate/1` runs inside the Lifecycle bind,
before the start reply, so a plugin-phase settle is a no-op (it was written,
measured as one, and deleted).

The product-side cure (a cast that cannot drop, or a start reply that waits
for the bind) is a behaviour change and is recorded as a found defect, not
made here.

## Where an outbox-drain settle is valid, and where it is not

`TierE2EHarness.await_outbox_drained/2` (`outbox_awaiting == %{}` and an empty
`pending_outbox` on every Entity writer) is only a settle once the records are
known to be AT the controller: after a sink observation (`await_changelog`,
`await_rows`) or a durable watermark. After a RUNNER-side live-state read it is
a lie — `late_input_correction` read `folded == [...]` from the runner, found
an empty outbox, drained, and saw only epoch-0 outputs because the replay's
records had not shipped yet. That test now waits for the first regenerated
output (`flz.epoch != "0"`) before draining.

## `outputs_at_checkpoint` cannot be settled on from the controller

An open interval ships NOTHING — not outputs, not changes — until it closes,
and the runner's `dispatches` heartbeat is on a 10 s cadence. So
`checkpoint_batched`'s "no output before the close" is proved by what
arrives, not by a window: the closing batch rides ONE record seq (an early
flush would ride its own), and the delay-closed output lands no earlier than
`max_delay_ms - 500` after its input — an arrival-time assertion, not a sleep.

## Small traps, each of which cost a run

- `unique/1` suffixes reach six and seven digits in a long run;
  `"tier_commit_block_workflow_" <> suffix` is 33 bytes against the 32-byte
  operative-id cap and fails only late in a combined run. The crash-resume
  ids are cut to 24 characters.
- `TestSinks.settle/1` on a NAME must target the sink the test actually
  registered: `pubsub_sink_test` registers a unique name unless a `:pubsub`
  sink leaked from another module, so the setup hands the effective name to
  the test.
- A `--include system` run in ONE BEAM after ~1 900 unit tests is a different
  load profile from the same files per half; the population that only shows
  there is the drop above, not "load".
- `mix test --repeat-until-failure 10 --max-failures 1 <file:line>` is the
  10×-alone drill in one VM; a leftover-state failure there is a leak.
- F1 (`dlq_e2e_test.exs:122`, was `:119`) is a WalWriter watermark defect
  (`complete_entry/2` re-enters `try_dispatch_outbox` inside
  `take_outbox_window/3`); it passes ~1 in 3 and is deliberately untouched.

## Deliberate sleeps that remain in `test/`

`NodeTestHelper.wait_until/3`'s interval (the one polling primitive every
copied loop collapsed onto) and `dlq_e2e_test.exs:165` inside the untouched F1
test. Rust keeps its 1 ms `wait_until` primitives, two supervisor-cadence
sleeps in `poll_core_tests.rs`, and the bench `quiet_cpu_pct` window.
