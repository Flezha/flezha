# `finish-the-suite-log-gate` — three sources closed, and two more the gate found

## What the gate was, and what it is now

`.github/workflows/elixir.yml:150`–`:163` runs `mix test | tee /tmp/mix-test.log`
and greps it for `[error]`/`[critical]`, exiting 1 on any hit.
`green-the-suite-log-gate` closed two of its four known sources and filed the
rest; its verdict was that **the gate cannot be relied on to be green until
KI-48 and KI-49 are both closed**. KI-50 appeared after it landed.

Baseline here, one full `mix test` (seed 238856): `mix test` **exit 0**,
`1836 passed`, and CI's grep matching **one** line — KI-48. A green suite with a
red gate, which is the reportability failure the change exists to remove.

After: two full runs on fixed seeds (424242, 909091), `mix test` exit 0,
`1839 passed`, **grep exit 1 — zero hits**, both times.

## KI-48 — Shape B, in `send_control`, and the timeout clause is the real change

Design D1 offered two shapes and recommended B. B still fit the code and B is
what shipped: `ControlChannel.send_control/3` wraps its `GenServer.call` in the
two-clause `catch` that `ControlUnit.ping_runner/2` and `send_control_await/3`
already carry one layer up —

    :exit, {:timeout, _} -> {:error, :timeout}
    :exit, reason        -> {:error, {:control_channel_exit, reason}}

— in the single funnel all six of `ControlUnit`'s `handle_call`s cross. Nine
call sites read, **none changed**: the six units each already matched
`{:error, reason} -> {:reply, {:error, reason}, state}`, `state/commands.ex:46`
returns the result behind an `{:ok, reference()} | {:error, term()}` spec, and
`:223` has a passthrough `other -> other`.

**Shape A was rejected for a reason that is not the obvious one.** Reordering
the child list makes `ControlChannel` start *before* `ControlUnit`, and
`handle_info` hands every inbound `CommandResponse` to the unit with
`GenServer.cast(state.control_unit, …)` (`:215`) — a cast to an unregistered
via-tuple returns `:ok` and **drops the message silently**. Shape A trades one
`[error]` line for a silent drop in a repository whose stated default is that
nothing drops. It would also have owed a spec delta, since
`native-transport/spec.md:30` writes `RunnerSupervisor.init/1` into the spec.

**The timeout clause is a behaviour change and is deliberate.** Before, a
`ControlChannel` that was merely *slow* killed its `ControlUnit` after 5 s and
took every in-flight waiter in `response_refs` with it. Now the unit lives and
answers `{:error, :timeout}`. `Lifecycle.timeout_class?/1` already treats that
family as "a busy runner", so the caller side was defended all along; this
finally defends the unit. It gets its own test rather than riding the shutdown
one.

**`send_control/2` became `send_control/3` with a defaulted timeout, and that
was approved rather than assumed.** The 5 s was already there — implicitly, as
`GenServer.call`'s default, and visibly in the reported exit's `…, 5000)`. It is
now the named `@control_call_timeout_ms`. Every production caller still calls
arity 2 and all five Mimic stubs on `:send_control` are 2-arity and still
intercept. The alternative was a 5 s test in a 58 s suite, which is exactly the
regression shape `docs/dev_notes/` blames for the last slowdown.

**Both new cases were run red first**, with the seam in place and the `catch`
absent — and each red *is* the defect, the caller exiting instead of being
answered: `** (exit) exited in: GenServer.call(…, 50)` / `** (EXIT) time out`,
and `** (stop) exited in: GenServer.call(…, 5000)` / `** (EXIT) killed`.
`1/3 passed` before, `3 passed` in 0.08 s after.

**`test/flezha_test.exs` is not a reproducer for KI-48.** The predecessor
recorded "2 of 2 runs of that file alone"; with the guard removed here it was
**0 of 2** (`14 passed`, zero `[error]` each) while a full run on the same tree
still carried the line. That note is corrected in place. The load-bearing
evidence is therefore the full-run pair, not the file.

## KI-49 — the task always completes; the ladder it dies in is the *next* test's

D2 asked which shape first: await, or stop. **Await.**
`Lifecycle.spawn_migrate_task/1` is `async_nolink` over `complete_init/4`, and
*both* of `complete_init`'s answers `send(parent, {:migrate_task_result, …})`.
There is no abandon path. In a unit test it always parks —
`ping_runner` → `whereis` `nil` → `{:ping_failed, :no_control_unit}` — and with
the module's `setup` overriding `:overwatch_retry_backoff_ms` to `[1]`,
`rest == []` on attempt 1 and there is no ladder at all.

**Which is what explains the entry's own quoted evidence.** KI-49 quotes a
`startup attempt 1 failed … — retrying` warning, and that line is *unreachable*
while `[1]` is in force. It appears because the task needs a few ms to clear
`bind_to_runner`, `put_overwatch`, `Plugins.prepare` and `resolve_init_state`
before it first reaches `resolve_runner/4` — by which time its test has ended
and **`on_exit` has restored the production backoff**. The orphan then runs the
full ladder, which is what keeps it alive long enough to land on the next test's
`^inst`-pinned stub. The retry ladder is not why it outlives its test; it is
what it does *because* it outlived its test.

**The entry names one test. Six leak.** `:116`, `:155` (the filed one), `:185`,
`:275`, `:320`, `:330` — and the last two spawned a task through `stub_rebind/0`
and then **discarded the state** as `{:reply, :ok, _}`, so their leak is
invisible to anyone grepping for `migrate_task != nil`. That is the trap: the
shape to search for is "a test that starts a task", not "a test that asserts one
exists".

One helper, reading the handle the tests already held:

    assert_receive {:migrate_task_result, ^pid, {:park, _parked, {:ping_failed, _}}}, 2_000

which is strictly stronger than the `assert migrate_task != nil` it follows. The
`^inst` pin is untouched — it is what proves the rehydrate resolves the right
instance, and loosening it would have silenced the symptom and the assertion in
one edit. Proved load-bearing by expecting a wrong reason: exactly those six go
red. Measured 3 of 10 seeds red at HEAD (777, 432113, 5150), **0 of 10** after,
same seeds, unchanged 1.6 s.

## KI-50 — the rate was never reproduced, and that is the honest record

**Not reproduced in 15 runs at this HEAD**: `phase_test.exs` alone 5/5 green,
the plugin tree 8/8 then 5/5 green, two full `mix test` runs green. The entry's
~1-in-3 in-suite rate stands neither confirmed nor refuted here.

The structural defect is certain by reading and was fixed regardless: an
`async: true` module attached a **VM-global** `:telemetry` handler that forwarded
*every* `fail_open` to the test pid. `MessageTestHelper.forward_telemetry/2`
gained an optional metadata predicate (default: forward everything, so its three
callers are untouched), the hand-rolled duplicate in `phase_test.exs` is deleted,
and each test mints its own `plugin_id` through the id argument `subscribed/2`
already took. A foreign event is never delivered — which is why this is not the
forbidden settle: a settle waits the foreign message past and leaves it to land
somewhere else.

**The census D3 asked for was run and paid off, just not the way it expected.**
A scratch telemetry recorder plus an `ExUnit` formatter stamping test start/end
on the same monotonic clock, over two full runs on the unfixed tree:

- **exactly four `fail_open` emits per run**, identical population both times —
  three from `phase_test` itself, **one** from `table_health_test.exs`
  (`plugin_id: "r"`);
- `Convergence.Probe`'s Loop tick, D3's second named candidate, emitted **zero**
  times in both runs and is out of suspicion (`config/test.exs` leaves the Loop
  off, and the one module that starts it drives no failing plugin probe);
- the two `async: true` emitters **never overlapped** in either run.

So the window is one foreign emit per run against three ~1 ms observation
windows — which is why 15 runs found nothing, and why chasing a sixteenth would
have been the wrong spend.

**What replaces the missing race is a deterministic reproduction.** The new
scoping case emits a foreign `fail_open` from a `Task` and `Task.await`s it —
`:telemetry.execute/3` runs every handler *in the emitting process*, so the
await is an ordering guarantee, not a settle — then refutes, then causes its own
emit and asserts it arrives. Remove only the filter argument and that one case
fails with `Unexpectedly received message {:fail_open, …, %{plugin_id:
"foreign_fail_open_1380"}}`. The mechanism, on demand, forever.

## The gate was proved, and then it immediately reported

`green-the-suite-log-gate`'s method, verbatim: a scratch test emitting one
uncaptured `Logger.error` turned CI's grep **red while `mix test` still exited
0** (`1840 passed`); the same test wrapped in `with_log` **and asserted** turned
that line green (`gate-probe` occurrences: 1 → 0); the file was deleted. Both
demonstrations used a **full** `mix test`, because the gate runs on the full
plain suite and a subset would prove the grep works rather than that the suite
is quiet.

**And in those two runs the gate found two sources nobody had seen** — which is
the change's value arriving before the change had finished landing:

- **KI-53** — `InputLog` sets `trap_exit`, `spawn_link`s its fsync companion,
  and has **no `handle_info/2` clause at all**, so the syncer's `EXIT` is logged
  by `GenServer`'s default handler. Observed *after* `Result: 1840 passed` had
  printed: the application master's shutdown sweep kills the app's remaining
  processes in unspecified order, so a syncer dies while its still-trapping log
  is alive to report it. The log line is the small half — a syncer that dies for
  a *real* reason is also ignored, and every later `send(state.syncer, :sync)`
  succeeds silently against a dead pid, so **the fsync stops and nothing says
  so**. Filed, not fixed: "match `{:EXIT, _, _}` and return `{:noreply, state}`"
  turns a durability hole into a silent one, so it is a design decision, not a
  fold-in — the same call `green-the-suite-log-gate` made on KI-48.
- **KI-54** — a `ForwardWorker` 250 ms delivery retry outlives its test and
  calls `FakeStartRegistry.pid_of/2` on an ETS table that has been deleted. The
  same shape as the KI-49 this change closed, in a different module.

Each appeared in **1 of 6** full runs. Both are filed and neither is fixed.

## What a reader should take from this

The population is whatever the last N runs sampled, and N is always too small.
KI-21 counted three sources from one run; `green-the-suite-log-gate` found a
fourth and said the set was four; this change closed three of those and found
two more. The gate is worth having precisely because that is true — but "two
clean full runs" is evidence the gate can report, not proof the suite is quiet.

## What this change did not do

- **KI-53 and KI-54 are not fixed**, so the gate can still go red on either.
  Neither appeared in the two closing full runs.
- **`mix test --include system` is untouched** — a different population,
  dominated by KI-17's family, and the gate does not run there.
- **KI-18's flakes are untouched.** `test/plugin/kafka_test.exs:285`
  (`expected <1ms, got 1384us` / `1541us`) fired 1 of 8 at baseline and 1 of 5
  after; it is pre-existing, is a failure rather than a log line, and is
  recorded in `close-mimic-global-leak.md`.
- **The other 36 hand-rolled `:telemetry.attach` sites in `test/` were not
  swept.** The helper is now general enough for them; migrating them is not this
  change, and none of them is currently red.
- **`scripts/format-check.sh` still cannot run on this box** — `rubocop` is
  absent (KI-4). The touched files are `mix format --check-formatted` clean, and
  `check-kind-naming`, `check-overflow-naming`, `check-sdk-boundary`,
  `check-mimic-global` and `check-build-artifacts` all exit 0.
