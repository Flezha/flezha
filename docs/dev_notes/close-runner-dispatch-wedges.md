# close-runner-dispatch-wedges — implementation notes

Three ways a runner stopped doing work with nothing to bring it back: a dead
pool thread nobody replaced, an activation a host took and never answered, and
a runner the controller could only evict. The gotchas below are the ones that
are NOT obvious from the diff.

## `abandon` replaced `rehand_unprocessed` — one mechanism, not two

The proposal asks for a new "abandon" answer beside `complete` and
`retry_after_failure`. `PollCore::rehand_unprocessed` already WAS that answer:
put these items back at the head of the line, release the entity, consume no
ladder attempt. So it was renamed rather than joined by a twin. The seam is now
exactly three calls, the ABI's third entry point has the same name as the crate
call it wraps, and there is one place where "the host did not process these"
is implemented.

Two callers, one contract: the tail that sat behind a head the host parked
(`host::apply_result`), and a host whose worker died holding the batch.

## Only UNTOUCHED items may be abandoned — and the Rust guard pays a clone for it

`Handout` (`sdk/rust/src/operative/dispatcher.rs`) keeps the item being
dispatched in `remaining` and dispatches a shallow clone (`WorkItem` is Arc
bumps). Popping first would lose that item on an unwind; abandoning an item the
host already ran would execute it twice. The pop happens in the `Done` arm
BEFORE `apply_ops`, because an item whose effects were applied is processed
even if arming its timers then panics.

## The core reports; it never reclaims

`outstanding_oldest_age_ms()` exists so the CONTROLLER can decide. There is
deliberately no deadline in the core: a ten-minute Procedure and a lost handout
are byte-for-byte identical from the boundary, and a timer that re-handed would
re-execute precisely the slowest, most expensive work in the system. The remedy
is `rebind_operative`, which resumes from durable state.

## The abort fixture cannot kill a Rust or Java worker — and that is the finding

The abort case was specified as "aborts its worker". It rides the EXISTING
`wedge_probe` fixture rather than a second operative: `wedge` blocks forever
and `abort` ends the activation violently, on one factory in each of the three
test apps, so `pool_wedge` lists one fixture id and there is no near-duplicate
operative to keep in step across three languages. On the BEAM that is real
(`Process.exit(self(), :kill)` ends the Entity untrappably). On Rust and Java it
is NOT reachable from user code: `dispatch_item` wraps the Procedure in
`catch_unwind` and `ActivationDispatcher` catches `Throwable`, so a panic or an
`Error` becomes a retryable ladder verdict, never an unanswered handout. The
The fixture therefore ends its activation as violently as its host allows, and
the scenario asserts what is true on all three: the entity is served again, and the
aborting item produced AT MOST ONE output (Rust/Java: one, via the ladder;
Elixir: none, lost with the process). The abandon guard itself is proved where
it is reachable — `dispatcher::tests::an_unwinding_worker_re_hands_its_activation…`
and `PollHostTest.aThrowingDispatchAbandons` / `aThrowingCompleteAbandons`.

## Elixir: the input and the exit signal race, and the input used to win

The Operative traps `:EXIT` and drops a dead Entity — but a `:block` enqueue is
a `GenServer.call`, and the input cast and the exit signal are two messages
racing for the same mailbox. When the input won, the Operative called a pid
that was already dead and **died with it**, leaving the key permanently
unroutable — the exact opposite of the requirement. `Operative.enqueue/5` now
catches the exit, forgets the entity and retries once against a fresh one, so
"the key is routable again on its next input" holds by construction rather than
by ordering luck. `dispatch_test.exs` forces the losing order with
`:sys.suspend/1`; without the fix it takes the Operative down.

This is what the conformance `abort_probe` fixture found on the Elixir arm —
the wedge and restart drills both passed, only the abort one did not.

## Java: an `Error` now ends a fetcher, so that it can be replaced

`fetchTurns` used to log and `continue` on ANY throwable from `poll`. An
`Error` (OOM, StackOverflow, LinkageError) retried forever is a fetcher that
can never fetch; now it ends the thread and `replaceFetcher` starts a fresh
one. The replacement is started BEFORE the dying thread releases its slot, so
`liveFetchers` never dips to zero and reads as a drained runner.

## The terminal disconnect must ship state BEFORE the write half closes

`command::handle_command`'s disconnect arm answers, then `shutdown(Shutdown::Write)`
so the ack is delivered before the peer sees the close. The graceful shutdown's
per-operative `RunnerShutdownNotification`s ride that SAME control channel, so
running it after the shutdown gave `Broken pipe (os error 32)` on every
operative — caught by the e2e, not by review. Order is now: reply →
`run_active_graceful_shutdown()` → `shutdown(Write)` → `exit_terminally`.
The per-operative acks time out (2 s, parallel) because the thread that would
dispatch them is the one running this handler; the notification write is what
actually ships the state.

Exit code is `70` on every SDK (`flz_core::shutdown::TERMINAL_EXIT_CODE`,
`Flezha.SDK.RunnerServer.@terminal_exit_status`). Non-zero is load-bearing: a
zero exit reads as a clean stop and the orchestrator does not restart the pod.
The Elixir SDK in `:native` mode (in-node runner) stops the runner instead of
halting the VM — halting would take the controller down with it.

## A disconnect is one named verb on the existing control seam

The first cut added a `handle_call({:disconnect_runner, terminal?, reason})`
clause to `ControlUnit` — a parallel mechanism beside `send_control_await/3`,
which already sends any `{tag, %RunnerCommands.*{}}` and awaits the reply with
a `whereis` guard, a timeout and an exit catch. Both disconnect clauses are
gone; `ControlUnit.disconnect(runner_id, terminal: …, reason: …)` is the one
named verb, and `FleetManager`'s CLI-facing disconnect gained that guard for
free. `Heal` no longer reaches past `ControlUnit` to its message protocol.

## The verdict names the RUNNER; the Diff resolves the operatives

The heartbeat's handout self-report is process-global, so
`entity_dispatch_stalled` cannot name which operative owns the stuck
activation. It carries `runner_id` at scope `:operative`, and
`Diff.wedged_operative/2` gained a runner-ctx clause that resolves it to every
operative bound to that runner — the same shape `wedged_connection/2` already
used. `Diff`'s `{:operative, ref}` therefore became `{:operative, refs}`, and
`{:runner, id}` became `{:runner, ns, id}` so a runner-scoped op can name its
namespace in the restart telemetry.

## The wedge ladder needed `ladder?/1` widened

`Planner.ladder?/1` decided whether an applied op advances its cooldown anyway
(`:ok` means "the remedy was applied", not "the stall cleared"). It matched only
`:output_stall`. Without widening it to every `@proved_not_producing` reason,
`evict_runner` returning `:ok` would reset the cooldown, the attempt count would
never reach 1, and `restart_runner` would be unreachable. Side effect, wanted:
connection/operative wedge ops are now throttled by a growing cooldown instead
of re-firing every tick.

## Elixir in-flight tracking is a slot table, not a counter

A counter cannot be un-leaked: an Entity killed untrappably would hold its
increment for the life of the runner. `Flezha.SDK.Counters` instead keeps 64
start-timestamp slots indexed by `phash2({operative_id, node_ref, entity_key})`;
the Entity claims and releases its own slot inside a `try/after`, and the
Operative releases it from its `:EXIT` clause for the kill an `after` cannot
reach. Two identities colliding on a slot means one runs unrecorded — the
report under-states, which is the safe direction for a signal a rebind hangs
off. `inflight` and `inflight_oldest_ms` are both derived from that table, so
neither can drift from the other.

Timestamps are wall-clock ms because `:atomics` are unsigned and `0` is the
free-slot marker.

## The wedge drill needed a replacement runner up FRONT

`control_plane_wedge_e2e_test` used to boot the replacement only after the
eviction. With nowhere to place the operative, every rebind attempt failed and
burned a rung of the standard 5 s → 30 s → 60 s → 300 s cooldown, so the drill
was timing the backoff ladder rather than the remedy — reliably enough on Rust,
never on the JVM (a 122 s run that never rebound). The replacement is now up
before the eviction, which is also what a real deployment has.

## The drills were racing the sink plugin, not the runner

Every intermittent failure in these e2e drills ("never produced", 30–130 s
runs) was one cause: a freshly bound operative's sink plugin activates
asynchronously, and an emit that beats the subscription is dropped with nothing
to re-drive it. Sending ONE frame and waiting proves nothing about the runner.
`produces?/5` re-sends until the output lands; the file went from 33–131 s and
1-in-2 red to ~9 s and 9/9 green across rust, java and elixir. If a drill here
starts flapping again, suspect the plugin race before the SDK.

## `pool_wedge` is required now

`test/helpers/node_test_helper.ex` `@required_capabilities`. The Rust manifest
had `"pool_wedge": []` — carried but with no fixture, which the gate accepts;
it now lists `wedge_probe` and `abort_probe`, and Java and Elixir stopped
deferring it. `PortPilot.open_port` gained a `:port_opts` passthrough so the
restart drill can ask for `:exit_status` without re-deriving the runner's
environment.

## Pre-existing reds found on the way

`test/apps/java_runner`'s `FactoryIdsTest` pins the Rust app's live factory set
by hand and had gone stale two ids ago (`schema_evolution_v1/v2`, from
`state-evolution`). `scripts/build-dist.sh` packages without running it, so
nothing caught it. Fixed here because this change edits that file; the list is
hand-maintained and will drift again.

**Java could not serialize a `Stateless` state.** `ActivationDispatcher.stateJson`
runs Jackson, and Jackson refuses an empty bean — so `MigrationSerialize` AND
`LiveStateRead` failed for every stateless Java operative, which means the
handover state ship and `flz state get` were both broken there. Reusing the
existing graceful shutdown for the terminal restart is what put it in the log.
Fixed by special-casing the SDK's own marker (`Stateless` ships `{}` and reads
back through the no-arg constructor) rather than disabling
`FAIL_ON_EMPTY_BEANS`: that refusal is RIGHT for a user state class with no
readable properties, and turning it off would silently ship `{}` for a real
author mistake.

## Threshold

`FLEZHA_CONVERGENCE_ENTITY_STALL_MS`, default 300 s — far above the longest
activation any bench-fleet fixture produces (a chained workflow turn is
milliseconds; the slowest e2e Procedure sleeps seconds). Raise it where real
Procedures legitimately run longer; the verdict's detail carries the reported
age so a false positive is diagnosable from the log line alone.
