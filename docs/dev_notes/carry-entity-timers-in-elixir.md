# `entity_timers` in the Elixir SDK — the BEAM's timer service is the wake list, and the order on the wire is records, arm, outputs

`carry-entity-timers-in-elixir`: per-Entity timers of both kinds and the three
clock kinds in `lib/flezha/sdk`, which inherits nothing from `flz_core`;
`entity_timers` joins the required set, the sixth capability the programme made
universal. What `proposal.md` and `design.md` say is not repeated; this is what
the next person touching `Flezha.SDK.Timers`, the Entity's firing paths, the
ctx's session or the fixture twins needs.

## No wheel of this SDK's own, and the due-set fold is what makes that safe

A processing-time timer is one `Process.send_after/3` armed by the Entity
process; the BEAM's timer service is the reference's "one list per process
that owns no thread". What makes one timer per deadline correct rather than
merely cheap is that a due message never fires *its* timer: it folds **every**
processing-time timer due by `max(scheduled, now)` onto the Entity's queue,
ascending (`Timers.due/3`), and a firing whose timer has left the pending set
fires nothing (`admit/2`). So the order among same-millisecond deadlines, a
restore's burst of overdue timers, a duplicate `Fire` from two due messages
racing one dispatch and a stale message after a cancel all resolve the same
way, and none depends on the order the BEAM delivers messages in. A due
message with no room re-arms itself 5 ms later, the reference's holdback.

## The order on the wire is records, then the arm, then the outputs

The supervisor's slice run under load found it: `complete/5` first shipped the
records, then flushed the outputs, then armed the timers and moved the gauge,
so a test that settled on the output frame read `timers_armed` as 0 in the gap.
The order is now records → `reflect/3` (arm, disarm, gauge, telemetry) →
outputs, so "an output is on the wire ⇒ its dispatch's timers are armed and
counted" holds for every reader, operators included. A test that settles on a
**record** frame is still ahead of the arm — the records ship first by design —
and settles on the arm's own report, the `:registered` telemetry event.

## The session opens lazily, and the direction row is why

With the session opened in `Ctx.new/4` the timer-less push measured **22.6
µs/push against 21.0–21.6 before** on the harness micro-bench (a direction
row: the box's spread was 5–14 %). With the session opened on the first timer
call — `timers`, `timer_policy` and `firing` are fields on the ctx, and
`take_timers/1` answers `{pending, [], zero}` when nothing opened one — it read
**19.1 µs/push, spread 3 %**. The one exception is a firing: its `take_timers/1`
opens a session if `on_timer/4` did not, because the fired timer's removal is
op 0 and must commit even when nothing else was registered. `reflect/3`'s
first clause returns on `([], zero_tally)` for the same reason; the two zero
tallies are the same map, and must stay so.

## Two dispatches in one millisecond register the same timestamp

`timer_after(ctx, 60_000)` from two inputs dispatched inside one millisecond
names the same absolute timestamp; the second registration is idempotent and
ships no op — correct, and it looked like a lost frame in the first test. A
fixture that needs distinct deadlines derives them from the input
(`TimerProc`: `input` seconds).

## KI-137: an Entity outlived its Operative, and the fix is a monitor, not a trap

`Operative.stop/1` exits `:normal`, and a link discards a `:normal` exit
signal, so `RemOperative` left every Entity alive (the spec's "Operative
removal" scenario was false). Trapping exits in the Entity closes it too, but
gen_server's parent-EXIT handling then logs one error report **per Entity** on
an abnormal Operative exit (`GenServer terminating ** (stop) killed`) where
today they end silently. The Entity is now started unlinked (`Entity.start/1`),
its Operative monitors it (the `:EXIT` clause became the `:DOWN` clause, same
`forget_entity/3`), and it monitors its Operative and stops `:normal` on its
`DOWN` through `terminate/2` — which the timers need anyway to cancel the refs
and release the gauge. A killed Operative's Entity therefore ends `:normal`.

## A crashed Single or Static Entity is minted again at once

A lazy re-mint on the key's next input leaves a slot holding armed timers
waiting for an input the reference never needs (its slot outlives the worker).
`remint/4` in the `:DOWN` clause mints a Single or Static
Entity again on an abnormal reason, as `prewarm/1` minted it; a dynamic one
stays lazy. It cannot loop: everything a Procedure raises rides the ladder and
parks, so only a crash outside it re-mints, and a fresh Entity dispatches
nothing until an input or a due timer reaches it.

## Small things that will bite

- `:gb_sets` with the same members are not `==` — a balanced tree's shape
  follows its insertion order. Compare `Timers.all/2`, never the struct.
- The Entity struct sat at 30 fields; five more crossed credo's 31 (the Erlang
  flat-map boundary). Everything timer-shaped is one `timers` map field
  (`pending`, `clock`, `on_timer?`, `refs`, `high_water`), and the static
  policy rides `ctx_opts` beside the other static ctx facts.
- `Record.resolve/2` answers `{:timer_ops, decision}` for a format-3 header
  after the same language and schema checks; a timer-ops record at a migrated
  schema reports `migrated` (the core does the same) and its body is never
  migrated. A format-3 *snapshot* header is malformed.
- `Records.ship(s, [], ops)` ships the ops record alone at the next seq —
  what a park of a firing uses; a `NoChange` Procedure ships nothing and holds
  its timers in memory only (the host path's rule).
- The reference `session_timeout` is `test/apps/rust_runner/src/entity.rs`,
  not `state.rs`; the twin lives in `factories_state.ex` because it is
  changeful. `Jason` encodes `closed_at` as the e2e reads it.
- `mix bench --runtime elixir` drives the app's **prod** artifact; rebuild it
  through the manifest's bench command or the guard row measures the old SDK.
- `--check --lint` sees an untracked file only after `git add -N`, and a
  one-line insertion above a pre-existing jscpd pair re-keys it as a new one.
- `Procedure.timer_policy/1` names the procedure; a test ctx built with a
  `nil` module passes `timer_policy:` itself.
- The reference has no `timers_armed` after an untrappable kill either way; the
  gauge here over-states by the killed Entity's count until the runner
  restarts, and its replacement re-arms from the slot.

## Test map

- `test/flezha/sdk/timers_test.exs` — the set and the session, the twin of
  `timer.rs`'s tests; `test/flezha/state/wire_test.exs` pins the postcard
  bytes both SDKs write.
- `test/flezha/sdk/persistence_test.exs` — the clocks, the fold over a
  format-3 record, the record after the changes, the restatement, the slot.
- `test/flezha/sdk/entity_timers_test.exs` — the ten firing cases: both kinds,
  the exclusion, the poison firing, the missing handler, the restore burst,
  the cancel and the stale message, the replacement, the eviction, the cap.
- `test/system/entity_timers_e2e_test.exs` under `FLEZHA_E2E_RUNTIME=elixir`
  — the fires case and the re-seat from disk, `uds` and `quic`.

## What the change found that it did not fix

The harness micro-bench cannot resolve 3 % on this box (5–14 % spreads across
series); the row is a direction row and read −9 to −12 % after the lazy
session. The app's prod artifact goes stale silently between benches.
