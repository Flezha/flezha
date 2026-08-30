# `fix-first-input-drop-on-bind` — dev notes

## The defect was a comment, and the mechanism it promised never existed

`Ingest.verdict/2` answered `:ok` for `{:error, :no_stream}` on a durable class,
above this reason:

```elixir
# A durable target has already journaled the payload, so an unbound stream is
# not a delivery failure — the bind delivers it.
```

**Nothing ran at bind.** The payload sat in the input log (or the proc inbox),
the Controller reported it accepted, and no code path ever forwarded it. The
whole change is making that sentence true.

The design says "nothing calls `replay_slice/4` when a stream binds", and that
is only half right, which matters for reading the diff.
`Replay.replay_journaled_inputs/1` and `Replay.deliver_inbox_tail/1` *already*
ran on every operative bind — `Lifecycle.complete_init/4` calls them. They ran
**too early**: the operative binds over the CONTROL channel, and the SDK opens
its data stream afterwards, so the re-forward's frames hit `io_in/3` with a nil
`stream_ref` and were dropped exactly like a fresh ingest. So this is an
**ordering fix plus a re-arm**, not a missing mechanism, and
`reforward_undelivered_tail/1` is a four-line caller of the two functions that
were there all along.

## Two halves, and which face each closes

| half | what it is | closes |
|---|---|---|
| the gate | `Lifecycle.complete_init/4` waits for `StreamChannel.await_bind/2` between `bind_to_runner/3` and `tier_restore/2` | face one (start window) and face two (the restore's re-forward) — everything downstream of the bind now runs on a bound stream |
| the arming | a delivery that answers `{:error, :no_stream}` calls `Replay.arm_bind_reforward/1`; the next bind sends `{:stream_bound, name}` and the Overwatch re-forwards the tail | the third face nobody named: a stream that **drops and rebinds mid-life** without the operative rebinding — which the convergence Loop heals with `rebind_data_streams/1`, a path that runs no re-forward |

Either half alone leaves a hole. The gate cannot help an un-journaled class
whose stream drops later (there is nothing to replay — the input is refused
instead, which is the honest answer); the arming cannot help an un-journaled
class at all, which is why the gate applies to **every** class and not, as D2
said, only to un-journaled ones.

## The gate blocks the bind continue, and that parks nobody

`start_operative/2` does **not** return "on the control-channel ack", as the
proposal and KI-2 both state. `init_operative/2` awaits
`ProcessHub.start_child`, which resolves when the Overwatch's `init/1` returns —
*before* `handle_continue({:bind_runner, …})` even starts. So there is no start
reply to "complete from the bind notification", and D2's mechanism could not be
built as written.

What the bind continue *does* own is the operative's mailbox. A `send_io` cast
arriving during it stays queued and is dispatched after it. So a time-limited
selective `receive` inside the continue is the whole gate, **and the mailbox is
the queue** the design's non-goal forbids building a second one for. The wait is
`:stream_bind_wait_ms` (default 30 s); expiry parks the operative with
`{:stream_unbound, channel}` through the existing parked-error contract, and
`Convergence.Probe` reads `init_error != nil` reason-agnostically, so the
un-park ladder needed no change.

`StreamChannel.await_bind/2` answers `:ok` for a channel that is not running at
all. That is not laxity: an operative on a runner with no channel pool has
nothing to wait for, and its delivery failure is reported by `io_in/3` exactly
as before. It is also what keeps every controller unit test with a fake runner
passing — `attach_channel/2` only computes a name, so `GenServer.whereis/1`
answers `nil` and the gate is a no-op there.

## The waiter list IS the arming, and the owner is not `self()`

`Replay.arm_bind_reforward/1` records interest with
`StreamChannel.notify_on_bind/2`,
which decides `:bound`/`:unbound` **and** records the pid in the channel's own
turn — so a bind landing between the two cannot be missed. There is no per-
operative "armed?" flag: the channel's `bind_waiters` set is it, one-shot,
cleared by the bind that serves it. A second undelivered payload costs one call
and schedules nothing extra.

The pid registered is **`ClassRegistry.overwatch_pid/1`, not `self()`**. The
un-park path runs `complete_init/4` — restore and journal re-forward included —
inside a task, and a task registered as the waiter dies before the bind can
reach it. That registry entry already exists for the same reason (the fsync-gated
outbox flush resolves its `:on_output` target through it), so this is a second
reader, not a second mechanism. `self()` is the fallback for a state that was
never registered, which is only ever a test's.

Two call sites arm, and they are the only two callers of `Overwatch.deliver/2`:
`Ingest.verdict/2` (a fresh ingest) and `Replay.reforward/2` (a re-forward that
was itself dropped). The second is what makes the restore's re-forward
self-healing rather than needing its own gate.

## Forcing the window instead of racing it

The predecessor change (`fix-outbox-watermark-overwrite`) turned a claimed
14-in-20 race into one deterministic 0.4 s test by finding the helper that forces
the precondition. The same applies here, and the forcing tool is cheaper than it
looks: **a real `StreamChannel` starts unbound and stays there until something
binds a stream to it.** So `test/flezha/operative/bind_reforward_test.exs`

- starts a real `StreamChannel` (`start_link({runner, name, :primary})`) —
  `stream_ref` is nil, which IS the window;
- ingests / re-forwards into it and watches everything drop;
- binds a real `Flezha.Transport.Local.Stream` pair through the product's own
  `{:bind, ref}` call, with the test owning the SDK side, so every forwarded
  frame lands in the test's mailbox as `{:flz_data, id, payload_id, message,
  metadata}`;
- unbinds again with `{:flz_closed, _, _}` for the flap case.

No sampling, no sleeps, 0.8 s for ten cases. **The four delivery cases were run
red on the pre-fix tree** — each failing with "the input was never delivered" —
before any production line changed; the four gate cases name
`StreamChannel.await_bind/2`, which did not exist there, so they cannot be run
against it at all. The bind notification is
driven explicitly (`settle_bind/2`) and its absence is *tolerated*, so a failure
reads as the defect rather than as one missing message.

`test/operative/overwatch_io_drop_test.exs` was folded into it rather than left
beside it. Its one case (§6.3, the `io_in` call path counting the drop) needs
exactly this fixture, and `refactor_metrics.py`'s `test` area ratchets
`async_false_share` — a new `async: false` module trips a blocking gate, and this
one cannot be async (`:state_root`, registered names, a global telemetry
handler). Folding keeps the file count flat and puts one module behind one
question: what an unbound stream does to an input, and what the bind then owes.
The same ratchet counts `refutes`, which is why the "nothing was delivered"
assertions are `assert drain_frames() == []` and the gate's wait is proved by
reading the channel's `bind_waiters` rather than by a `refute_receive`.

**Task 1.2's 40x sampling run was not the reproduction** and was not used as one.
It was run afterwards as a regression check (see below).

## `told/2` is not the cast path

The design and the task list both say
`told({:io_in, _from}, _verdict), do: :ok` "discards the verdict on the cast
path". It does not: `:cast` is its own `reply_to` value, and that clause is the
**blocking** `:io_in` call, which is documented as reporting accepted-vs-parked
and nothing finer. The cast's verdict is discarded one step later, by
`reply(:cast, _reply), do: :ok`. The conclusion — that a refusal on the `send_io`
path reaches nobody — holds; the mechanism named for it was wrong.

`Ingest.durable_input?/1`, not `journal_input/2`, is the single authority on
which classes are journaled (two callers: `journal_input/2` and `verdict/2`).
The gate deliberately does not key off it.

## What `40/40` cost to measure, and the defect that was in the way

`effectively_once_checkpoint_e2e_test.exs:37` failed **4 of the first 40 runs**
at `refute InputLog.watermark(operative_id)` — a watermark of `6` after two
inputs. It is not this change's, and it is not load: an instrumented run showed
`preexisting=true` for `app_data/state/<namespace>/<operative_id>` **before the
test booted anything**, with a `last` offset of 8 or 10 — a previous run's whole
input log.

`TierE2EHarness.cleanup/3` removed `Path.join(state_root(), operative_id)`.
Durable state lives at `<state_root>/<namespace>/<class>`
(`Flezha.State.Registry.class_dir/1`), so that path **has never existed**: every
run leaked its state tree, and `unique/1`'s `System.unique_integer([:positive])`
restarts per BEAM, so repeated single-file runs collide on both ids soon enough.
64 leaked `cp_ack_*` trees had accumulated in `app_data/state`.

Fixed here (the namespace tree is the scenario's own, so it goes whole), because
it made the change's own acceptance number unmeasurable. Four sibling sites carry
the same mistake and are filed as **KI-42** — three of them are
`refute File.exists?` assertions that have never been able to fail.

With the path corrected: **40 of 40 pass, 0 `[:flezha, :stream, :drop]`, 0
leftover trees.**

## Deleting the workarounds is the proof, and it needed no product follow-up

`NodeTestHelper.await_stream_bound/2` is gone with its 29 call sites across 15
modules, and `sdk_smoke_e2e_test.exs`'s `send_until_forwarded` re-send loop is
gone with it (a single `send_io` plus `assert_receive` stands). The full system
suite is green without them and **no module needed a replacement settle** — which
is the evidence D5 asks for.

The remaining `GenServer.call(chan, :stream_ref_status)` calls in the tree are
not bind polls: they are either synchronisation barriers on the channel process
(`_ = GenServer.call(...)`) or assertions that a channel is unbound.

## Deleting the settles surfaced a stale assertion, and `| tail` nearly lost it

`schema_evolution_e2e_test.exs`'s "v1 code meeting v2 state is refused, not
decoded" went red **2 runs in 5** of the full system suite. It is not a
regression: the test asserted `{:ok, _}` from a start whose restore the SDK
refuses, and its own comment said so out loud — *"today the refused restore is
discarded and the Entity binds cold; `guard-cross-sdk-placement` turns that into
a failed bind"*. That sibling change landed long ago, so the assertion had been
latently wrong ever since; desettling the module and gating the bind only
tightened the timing enough to make it show.

**The stale assertion could not simply be inverted.** `{:error, …}` is as racy as
`{:ok, …}` was, in the other direction: for a non-tier class the restore verdict
travels **off-process** (`Replay.dispatch_restore/3` spawns, then sends
`{:restore_refused, _}`), so it races the `:registry_runner_set` call
`OperativeManager.finalize_init_operative/3` makes to read the operative's
`init_error`. Whichever message the Overwatch handles first decides the start's
return shape. The spec (`operative-state-persistence`, "A refused restore fails
the bind instead of cold-starting the Entity") constrains something else
entirely: *leave the operative unbound, emit telemetry naming the entity and the
reason, never silently serve from empty state.* So the test now asserts exactly
that — the refusal telemetry, the reason **by name** (`schema_version 2 is above
the running procedure's 1`, which is the point: a positional codec would have
decoded v2's `{delta, note}` as v1's `(delta)` and handed it the value 9), and
that the operative never reaches a serving state. `serving?/2` is one predicate
over both shapes the refusal can leave behind — parked in place, or stopped by
`finalize_init_operative/3` when the refusal won the race — and neither is
serving, while a cold bind would be. 8 of 8 runs green.

It is the **only** module of the fifteen desettled here with this exposure, and
the reason is checkable: it is the only one that stops an operative with
`preserve_state: true` and restarts it **under a different factory**, so it is
the only one where a restore can be refused at all. Every other desettled site
asserts `{:ok, …}` for a fresh operative id with no durable state behind it.

**Six full `mix test --include system` runs on the finished tree**, so the
schema-evolution correction is held against a failure that used to run 2 in 5:
seeds `149742`, `558432`, `377257`, `934591` green at 1909 passed / 5 skipped;
`682679` red with 10 failures, all `CliManagerScheduleTest`, all **KI-43** (a
Mimic global-mode leak reproducible in two seconds from two files in a directory
this change never touched); `567954` red with one, `workflow_crash_resume_e2e
_test.exs:249`, which **KI-18 already records as provably pre-existing** at
`refactor-baseline-2`. The schema-evolution case itself: 8 of 8 green in
isolation and green in all six suite runs.

**And it was nearly lost twice, to the same mistake: `mix test --include system
| tail -N`.** The failure block prints long before the summary, so a tail that
captures "Failed: 1 test" captures none of the detail, and two consecutive
sessions reported an unidentified failure that was fully identifiable. Redirect a
suite run to a file and grep it; never pipe it through `tail`.

## One test asserted the old lie, and the fix is the honest half of D3

`overwatch_confirming_ingest_test.exs` asserted `:ok` for a durable target with
`channel: nil`. Under the new rule that is `{:error, :not_ready}` — a nil channel
can never bind, so nothing will re-forward and `:ok` is exactly the
accepted-and-dropped claim being removed. The test now uses a real unbound
channel for the `:ok` case (which is the real window) and keeps the nil-channel
case as a refusal. The payload is still in the inbox either way: the contract is
append-before-forward, and the inbox's dedup absorbs the sender's retry.

## Worth knowing

- `handle_info({:stream_bound, _channel}, …)` ignores which channel announced
  the bind — the tail is owed either way and goes out on whichever channel the
  operative is bound to now — but it is **skipped while parked**, like the
  ingress cast, because the un-park's own bind re-forwards it and a stray
  notification would otherwise push input at the runner it is parked away from.
- **Bind latency (task 4.3), measured at the gate rather than by an e2e wall
  clock**: an instrumented run across the checkpoint, smoke and DLQ e2e modules
  (21 operative binds) shows a median wait of **~20 µs** — the channel is
  already bound by the time the operative bind reaches the gate, because the
  runner connect issues `ConnectSocket` before any operative starts — and a
  worst case of **5.7 ms** (7 binds waited at all: 0.99, 1.08, 1.62, 3.25, 4.24,
  4.88, 5.66 ms). For comparison, the deleted `await_stream_bound/2` polled on a
  **50 ms** `Process.sleep`, so the suite was already paying at least one 50 ms
  tick wherever the stream had not bound. The gate does not add latency; it
  removes a polling quantum from the place that was paying it.
- The arming lives in `Replay`, beside the re-forward it schedules, not in
  `Overwatch` beside `deliver/2`. Both halves of "the bind re-forward" are then
  one module's business, and `overwatch.ex` stays under the charter's 800-line
  file ceiling (it reaches 815 with them).
- `arm_bind_reforward/1` logs nothing on `:unarmed`. The refusal is the caller's
  answer and `io_in/3` has already emitted `[:flezha, :stream, :drop]` naming the
  operative; a per-input warning would be a second report and a spam risk under a
  storm.
