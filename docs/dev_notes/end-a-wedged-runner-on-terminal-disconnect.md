# end-a-wedged-runner-on-terminal-disconnect — implementation notes

KI-130: a wedged php runner could not die, came back under a fresh id and won the
rebind. The proposal and design in the archived change carry the timeline; what
follows is what the next person touching the wedge ladder, the graceful ship or
the drill needs.

## The ladder's second rung was unreachable on every SDK

`evict_runner` closes the session (`Session.do_evict_runner/2` →
`stop_runner_sup/1`), the SDK's control reader sees EOF, and
`Coordinator::handle_reconnect` re-registers the **same process** ~1 s later
under a fresh session token with its cached stable id. The Planner keys the wedge
ladder on the session token, so "restart only if a later tick still proves the
same runner wedged" could never fire for it: the target had changed its name.
That holds for every `flz_core` SDK and for the Elixir SDK — nothing survives its
own eviction. The rung fired only from `Heal.execute/1` called by hand.

The ladder is inverted now: `restart_runner` first, on the session the verdict
named (the one moment the controller can be sure which process it is talking
to), `evict_runner` above it for a process that did not exit — still connected
under the same token, `Stopped` in the core, re-proved by the classifier. A
`Stopped` core refuses `reconnect`, so an evicted one cannot return. The
orchestrator's fresh process presents the same stable id and is bound as any
reconnect is; no per-process nonce is needed, because the process the verdict
named is ended before it can return.

## The rung ended no process, and rust proved it, not php

`finish_disconnect` runs `run_active_graceful_shutdown` on the coordinator
thread; `serialize_slot` waited on `with_entity_exclusive` for a gate the wedged
handout holds for the life of the process. A probe that wedged the **rust**
reference runner and called `Heal.execute(:restart_runner)` directly saw the ack
`:ok` and no exit in 45 s, session still active — identical on php and node.
`exit_terminally`'s "exiting 70" line in the KI's evidence came from the drill's
second test, whose runner was not wedged. What was php-specific was only that
nothing else ended it: the rust test app exits on stdin EOF at once, node after
a 5 s grace, so their returned process was dead before it could win a placement.
In production nothing closes stdin, and every SDK flapped.

## The bound is on the gate, and the host wait follows it

`PollCore::try_with_entity_exclusive` is the existing acquisition loop with its
5 s warning threshold made a deadline; `with_entity_exclusive` is the no-limit
call of the same loop (`wait_for_gate`). Only the winner releases the gate: a
caller that lost it at the bound returns `None` and touches nothing, which the unit
test pins (`drain_active` still set after the ship).

`serialize_slot` uses the bound for both purposes. During a graceful ship
(`StatePurpose::Migration`) the `migration_serialize` host command waits the same
5 s (`host_command_within`), not `HOST_COMMAND_TIMEOUT`'s 30 s — a single-loop
host whose only loop is wedged answers no command, so N healthy host Entities
would otherwise cost N × 30 s against the container's grace period. A live read
keeps the 30 s. A busy Entity is `Error::State` with the `entity_busy:` prefix,
which `collect` logs and omits: the notification ships without it, and what it
had was its last completed activation's state, already in the periodic snapshot
or the WAL.

**A read that names one Entity answers its failure.** `collect` omits a
failing Entity in a walk — a ship must still carry the others — but the
`entity_key` arm of `collect_procedure` propagates `serialize_slot`'s error, so
`flz state get` on a busy Entity is refused with `entity_busy:` after the bound
instead of answering `{}` for an Entity that has state.

## `process::exit(70)` from the coordinator thread ends a foreign runtime

The exit is the core's, from its own thread, with the host's main thread still
inside user code — php's `sleep(3600)`, node's dispatch, python's and ruby's
pools, the JVM. Every arm of the drill proved it (the numbers are in the
checkpoint report): no runtime's atexit handlers needed the wedged thread.

## The Elixir SDK re-registered the process the controller had just ended

`Flezha.SDK.child_spec/1` defaults `restart: :permanent`, and
`RunnerServer.dispatch({:runner_disconnect, %{terminal: true}}, …)` answered
`{:stop, :normal, _}` after `System.stop(70)`: the supervisor restarted the
server at once and the fresh one connected with the same stable id — a third
session in `active_runners` beside the replacement — until `:init.stop` took
the tree down. KI-126's Elixir twin, found by the drill's new
`active_runners == [runner_b]` assertion on the first elixir arm. The terminal
branch now leaves the server up with `disconnecting?: true` (closes are ignored,
nothing reconnects) and lets the VM's stop end it. The `:native` branch still
stops the server — an in-node runner under a `:permanent` spec would be
restarted the same way; not this change's, named here.

## The drill's traps

- **The wedged runner needs `port_opts: [:exit_status]`** to assert the exit;
  `probe_on_its_own_runner/3` threads it. Phase 2 no longer closes A's port —
  the process closed it — but the `on_exit` close stays for a red run.
- **`active_runners == [runner_b]` is asserted on the exit-status settle**, not
  refuted after a sleep: the process that exited cannot be back, and a `refute`
  here would be a `refutes` ratchet line for a fact a positive assert states.
- **The second test wedges before the restart.** A terminal disconnect to a
  healthy runner proves nothing about the rung it exists for.
- **A pgrep 0 s after `mix test` lies**: healthy php runners take up to ~2.5 s to
  drain after the BEAM's stdin EOF (the 2 s ack wait against a dead controller).
  Settle 6 s before counting leftovers.

## What this did not do

The other four `with_entity_exclusive` callers (`compact_slot`,
`restore_into_slot`, the two `apply_state` sites) are controller commands with no
limit on that wait, on the same coordinator thread; none is on the terminal path.
`Heal.restart_runner` does not mark the session retiring, so the close it causes
still mints an `output_stall` and a duplicate rebind.
