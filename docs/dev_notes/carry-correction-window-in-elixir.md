# Late-input correction in the Elixir SDK — the marker that no test can prove, and the split taken one line early

`carry-correction-window-in-elixir`: the lateness classification, the correction
request, the replay slice and its event-time order in `lib/flezha/sdk`, which
inherits nothing from `flz_core`; `correction_window` becomes the **eighth**
universal capability and `@required_capabilities` reaches 15 atoms, leaving only
`workflow` (R7). What `proposal.md` and `design.md` say is not repeated. This is
what the next person touching `Flezha.SDK.Entity.Tier`, `Flezha.SDK.Entity.Clock`
or the `candle` twins needs.

## The slice's order is guaranteed by a call in another module, and nothing enforces that

**`Operative.push/6` pushes a slice member with `Entity.enqueue_block/2`, a
`GenServer.call`.** `kick/1` sends `:process` to the Entity's own mailbox *during*
the handling of that call, so the `:process` is queued before the caller's reply is
delivered and therefore always ahead of the Operative's next enqueue. The drain
provably keeps up one item at a time, and every member is buffered before the
end-of-slice broadcast can arrive. **That call is the guarantee.** If it ever
becomes a cast, the fold order goes wrong with no error anywhere and no gate in
this repo would catch it.

The `%Tier.SliceEnd{}` marker — taking the close off the Entity's own queue
instead of flushing on the message — is **defence in depth, and no test
distinguishes it.** The negative control was built and it refused to fail:
replacing the marker with an immediate flush left the correction suite at
**19/19**. Two constructions that would separate them were tried and neither
exists here — a full mailbox does not work, because `admit_waiter/1` enqueues the
waiter inside the same `process_next/1` that popped, and a queue-holding park does
not exist under the tier (`Tier.park/5`: "nothing is held; the record is the
durability"). The marker costs two lines and removes the dependency; it is kept
for that reason and for no measured one.

The inversions that DID fire, for contrast: no sort in `flush_slice` → 16/19; members
never buffered → 15/19; the classification forced `:on_time` → 9/13; the window
boundary `<=` → `<` → 12/13, the inclusive case alone.

## A member whose extractor raises is dispatched, not sorted at zero — a deliberate divergence

The reference sorts such a member at `0` (`event_time_of(p).unwrap_or(0)`) and
dispatches it first. Here `Clock.extract/2` answers `:error`, the `with` in
`admit_value/4` falls through to `dispatch/3`, and the retry ladder meets the raise
and parks it — **the member is never buffered at all.** Both runtimes end with the
input parked and no state change; only the position of that one member inside the
re-fold differs. Ours never buffers what it cannot order, rather than ordering by a
value it invented. A cross-SDK conformance test should find this written down
rather than trip on it.

## `Entity.Clock` was taken at 799 of 800, deliberately

The change reached **799** raw lines in `entity.ex` against a **blocking** 800 —
compliant, and one line from a tripwire. The split was taken anyway, on the
supervisor's ruling, because the next worker to touch the file hits the gate on its
first added line and takes an undesigned split under pressure; change 4 got its
unplanned `Entity.Tier` exactly that way, at 854.

`Flezha.SDK.Entity.Clock` is split **by mutation** (charter §7): everything that
writes `timers.clock`, `timers.refs` or `timers.high_water` — the clock resolution
and its captured extractor, the safe extraction, the high-water advance, the timer
reflection and the BEAM `send_after` arming. It aliases `Counters`, `Procedure`,
`Timers` and `State` and **never names `Entity`**, so the split forms no cycle.
`entity.ex` fell **799 → 718**. Two things stayed behind and are worth knowing:
`clock_now/2`'s `%Fire{}` clause, because moving it would make `Clock` name an
`Entity` struct and a struct pattern is a dependency edge (charter §11h); and the
whole queue/ladder half, which is the process, not the clock.

## A slice member needs no `admit/2` clause, and that is why `entity.ex` fitted

The plan gave `entity.ex` four seams. It got three: a member flows through
`admit_value/4` like any input and `Tier.admit/3` answers `{:buffer, event_time}`,
so the event time is extracted **once**, by the code that already extracts it, and
nothing is duplicated. The alternative — an `admit/2` clause that extracts for
itself — needed either a second `safe_extract` or a `Clock` that names `Entity`.

## The two `candle` twins collided anyway, and the cure was the in-suite one

`design.md` D8 predicted the risk and its mitigation was not enough: writing the
two fixtures "differently" still left `init_state`, `apply_change`,
`state_from_json` and `event_time_of` near-identical, and the detector counted the
**78-token** pair against `test` **and** `test/apps` at once — change 4's lesson,
repeated. Two more pairs came with it: my test file's header against
`effectively_once_test.exs`'s (40 tokens, `use` + `import` + the first alias), and
a builder fixture against `CorrectionDropOp` (40 tokens).

The cure is that the **in-suite** probe changed, never the app twin, because the app
twin is pinned knob-for-knob to `probe.rs`: its state key is `order` not `folded`,
its Change is a bare integer not `{:fold, at}`, it prepends, it parses with
`Integer.parse/1`, and it has no `state_from_json/1` at all. The header pair went
by moving the `import` below the aliases; the builder pair by declaring
`effectively_once/1` **last** in that fixture, which is legal and is the very
order-independence the test asserts. Both rows returned to baseline (78, 145).

## The metric could not see 81 lines of this change, in the direction that flatters

`lib/flezha/sdk/entity/clock.ex` and `test/flezha/sdk/correction_window_test.exs`
are new, and an untracked file is invisible to every metric (charter §11e, the
third silence). Because `entity.ex` **lost** 86 lines to a module the metric could
not count, `--check` reported `lib/flezha/sdk prod_loc 3491 → 4134` when the true
figure is **4215** (`clock.ex` counts 81 code lines). The change's own delta on top
of change 4's standing +569 is therefore **+155**, not +74 — under the survey's
+200 and inside the +210 ± 40 carried. `test` gains **179** test lines the same
way. **No number here is evidence until the supervisor has `git add -N`'d both
files.**

## `beyond_window_policy` is declared and read by nobody

`grep -rn beyond_window lib/` finds **zero** readers on the controller side; the
policy is applied entirely SDK-side, and the key exists so the golden and an
operator can see what a class chose. The reference is the same
(`operative_start.rs` emits it, no core reads it). It still has to ship on every
window-declaring class, because `test/fixtures/declarations/candle.json` pins all
four keys and the smoke scenario compares the whole map.

Related, and filed as **KI-140**: `stream-correction`'s "Both outcomes SHALL emit
telemetry identifying the entity and the lateness magnitude" is satisfied by **no
SDK** — the reference emits none. This SDK matches the reference rather than
diverging alone. It is the third member of a class (with KI-115's half B) that row
R11 owns.

## The measurement said nothing, and said so before it was run

Both rows were declared non-gating **in batch 0, on freshly measured floors**, not
on change 4's inherited ones — and the fresh floors were worse. The harness
micro-bench's own before-arm spreads were **17.6 / 14.3 / 18.3 %** across three
series of one identical arm (change 4 saw 15.5 / 9.4 / 2.1 %), against an effect
that is one `case` on a `nil` map key. Direction row, reported, never a gate:
before medians **22.87 / 20.33 / 20.67** µs/push, after **22.79 / 22.52 / 23.26**.
The direction is consistent and it is **inside the instrument's own floor**, the
arms were not interleaved, and the box's load ran 0.22 → 1.45 between them, so no
verdict is asserted against it.

The guard row is the steadier instrument and it did not trip: **123 917 / 123 905 /
124 163** msg/s before, **117 717 / 121 279 / 126 802** after — the after population
brackets the before one, a −2.1 % median move inside a ±8 % floor and far from the
±10 % that would have stopped the batch. Its own spread went 0.21 % → 7.2 % with the
load, which is the same story the micro-bench tells.

No feature-cost row was run. Change 4 established that this instrument measures the
harness — `BenchKeyed` costs 46 µs/push at n = 500 and 245 at n = 10 000 — and a
correction window's marginal cost is smaller than the tier's, which that instrument
already resolved as nothing measurable. **The e2e is the instrument that can see
this feature**, and it is pass/fail: `late_input_correction_e2e` asserts the fold
ORDER, which is exactly what a broken slice gets wrong.

## What the controller did not need

Not one line of `lib/flezha/operative`, `lib/flezha/runner` or `lib/flezha/state`
changed except `Wire.encode_correction_request/1` — four counted lines beside its
own decoder, which had existed alone since `stream-late-input-correction`. The
rewind, the baseline scan, the epoch bump, the request coalescing, the derived
retention and `flz operative replay` were all exercised, never written. That is
what "the controller is SDK-blind" buys, and it is why this was the smallest change
of row R6.
