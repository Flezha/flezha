# Relay lifecycle

How a relay instance starts, binds, halts, sleeps, moves and stops on the controller, as the
code does it. Written for contributors who change one of those transitions. What a user sees
is in the published pages (`docs/public/topics/core/workflows.md` "Sleeping when idle",
`docs/public/architecture/steps_profile.md` "Sleeping"); where a relay runs is
[`placement.md`](placement.md).

**Contents**

1. [The process and its modules](#the-process-and-its-modules)
2. [Phase](#phase)
3. [Cause](#cause)
4. [The walk](#the-walk)
5. [The five scopes' rows](#the-five-scopes-rows)
6. [Guards](#guards)
7. [Adding a transition](#adding-a-transition)

## The process and its modules

One `Flezha.Relay.Overwatch` per relay instance, a ProcessHub child of the namespace's hub. Its
callbacks route; the work is in the module that owns the phase:

| Module | Owns |
|---|---|
| `Flezha.Relay.Lifecycle` | the first bind (`bind_continue/3`, `complete_init/5`), the halt (`halt/2`), the `:process` walk (`terminate/2`, `shutdown_steps/0`), `departure/1`, `release_lineage/1` |
| `Flezha.Relay.Rebind` | every later bind, run in a task linked to the Overwatch (`start_bind/3`, `bind_task_reported/2`, `apply_bind_result/2`, `bind_task_exited/2`, `terminate_bind_task/1`), the loop's `{:rebind, target}` (`rebind/2`), a runner's new session (`session_replaced/4`) |
| `Flezha.Relay.Suspend` | sleep and wake (`suspend_at/3`, `enter_suspended/2`, `evict_runner/2`, `wake/2`, `wake_deadline/1`, `rehydrate_if_pending/1`, `runner_available/1`), and a steps loop's rewind (`rewind_to/2`) |
| `Flezha.Relay.Handover` | a live move between controllers: `prepare/1` on the source, `absorb/2` on the destination, `consent/1` |
| `Flezha.Service.RelayManager.Teardown` | the `:relay` walk of a stop (`stop_relay/3`, `request_stop/2`) and the `:namespace` walk's relay row (`stop_all/2`) |
| `Flezha.Service.RelayManager.Admission`, `Flezha.Service.RelayManager.Init` | the `:start` walk's rows |

## Phase

`%Overwatch{}.phase` is one of `:unbound` (its initial value), `:running`, `:halted`,
`:suspended`. `Flezha.Relay.Overwatch.phase/1` reads it, and answers `nil` for a state from an
older controller without the field. Only the transitions write it:

| Phase | Written by |
|---|---|
| `:running` | `Lifecycle.complete_init/5` through `record_and_replay/3`, at the end of every bind; a task's bind is adopted by `Rebind.apply_bind_result/2` |
| `:unbound` | `hold_unbound/2` in `Lifecycle.bind/3`, when no runner answered within `overwatch_runner_wait_ms`; also `halt/2` on a relay holding no runner |
| `:halted` | `Lifecycle.halt/2`, the one place every halt goes through. It keeps `:suspended` for a sleeper and `:unbound` for a relay with no runner (`halted_phase/1`) |
| `:suspended` | `Suspend.evict_runner/2`, from a park (`suspend_at/3` → `enter_suspended/2`), from boot over a sleep record (`Lifecycle.bind_continue/3`), and from a rewind (`rewind_to/2`) |

`halt_reason` is set by `halt/2` and `hold_unbound/2` and cleared by the bind
(`complete_init/5`). `bind_task` is `%{pid, then, from}` while a task binds: `from` is the phase
the bind started from, and `Rebind.apply_bind_result/2` does not adopt a bound state when the
phase changed meanwhile (a halt or a park landed during the bind).

The loop reads the phase, it does not derive one: `Flezha.Convergence.Probe` puts
`Overwatch.phase/1` into its observation and `Flezha.Convergence.Probe.Judge` turns `:suspended`
and `:halted` into the verdicts of the same names. `Flezha.Service.Relay.Info` reports the live
field, and the loop's record only when the Overwatch could not be read.
`Teardown.request_stop/2` matches `phase: :suspended` to wake a sleeping steps relay before it
stops.

## Cause

`%Overwatch{}.cause` names the transition that will decide how the process ends. The code
writes these values:

| Cause | Written by |
|---|---|
| `:start` | `handle_call({:registry_runner_set, _}, ...)`, the deliberate start taking its input |
| `:arrive` | `Handover.absorb/2`, when the moved state reached a runner |
| `:move` | `Handover.prepare/1` (for the lineage release) and `handle_info(:calls_handed_over, ...)` |
| `:stop`, `:complete`, `:rescale`, `:failed_start` | `Teardown.stop_relay/3` through `handle_call({:cause, cause}, ...)`, before the hub stops the process |
| `nil` | the initial value; `Teardown`'s `:hub_stop` row writes it back when the hub stop fails, so a relay still running does not end later as stopped |

`Lifecycle.departure/1` is its one reader: `handed_over?` or `:move` is `:moved`; the four
named stops (`@named_stops`) are `:stopped`; any other value falls back to the declared list
(declared there is `:moved`, absent is `:stopped`). The `:process` walk's `:lineage` and
`:plugin_table` rows act on that answer. `arrived_via_hotswap?` and `handed_over?` stay separate
booleans because later code reads them long after the transition: `Replay.carrier/1` reads
`arrived_via_hotswap?` for `Replay.restore_on_bind/2`, the `{:kick_restore, _}` cast and
`Rebind.bind_relay/1`, in one precedence: a handover that reached a runner excludes the durable
restore, which overrides the seed, and a shutdown snapshot seeds before the init values.

## The walk

`Flezha.Shutdown.run/5` folds the steps of one scope in their declared order, each once, in the
caller's process, under one budget; it re-checks the deadline between steps and names in its
report every step that did not run. A step that errors, raises or exits halts the walk unless
the caller passes `on_error: :continue`. A module declares its steps with `shutdown_steps/0`
(`@behaviour Flezha.Shutdown.Step`); `Flezha.Shutdown.steps/1` discovers them at runtime from the
application's modules and refuses two steps that share a `{scope, order}`. A failing `:start`
step is a refusal and is not logged.

`Flezha.Shutdown.quiesced?/2` answers whether a scope is being torn down, from the table the
coordinator (`node_phase/1`) and the namespace walk (`run/5` on `:namespace`) write.
`Flezha.Shutdown.admit/2` is the one refusal a runner connect and a relay start give
(`{:error, :shutting_down}`).

The budgets come from `Flezha.Config.Durations`: a relay stop has `stop_release_timeout_ms`
(15 s); the `:process` walk has `stop_process_budget_ms` (3 s less); the Overwatch child spec's
`shutdown` is `overwatch_shutdown_ms` (2 s less than the stop), stamped by
`Flezha.Service.RelayManager.Init.with_shutdown/1`, so the supervisor waits for the `:process`
walk to finish.

## The five scopes' rows

### `:start`, run by `RelayManager.start_relay/3` (`start_walk/3`)

| Order | Row | Module | Does |
|---|---|---|---|
| 10 | `:reserve` | `Admission` | takes the relay and its Cells from the namespace's quota in one step (`Quotas.record_allocation/3`), so two concurrent starts cannot both fit in the last unit |
| 20 | `:disk` | `Admission` | refuses a namespace over `max_state_bytes` |
| 30 | `:namespace_claim` | `Admission` | `Paths.claim_namespace/2` |
| 40 | `:declared_start` | `Init` | the hub start as declared intent (`init_relay/4`); its answer is the start's |

After the walk, `Admission.settle_start/2` keeps the quota take when the start answered
`{:ok, _}` and gives it back otherwise.

### `:process`, run by `Lifecycle.terminate/2` in the Overwatch's `terminate/2`

| Order | Row | Does |
|---|---|---|
| 5 | `:calls` | answers every caller still waiting (`Flezha.Relay.Calls.Inbound.end_all/4`) |
| 10 | `:bind_task` | kills an in-flight bind task (`Rebind.terminate_bind_task/1`) |
| 20 | `:class_registry` | drops the in-memory class entry |
| 30 | `:control_unit` | tells the runner to drop the relay and waits up to half the budget for its answer, which comes after the Cells' last changes shipped; a gone or silent runner is not an error |
| 35 | `:writers` | closes the class's Cell writers, flushed |
| 37 | `:lineage` | `release_lineage/1`, after the final flush |
| 40 | `:plugin_table` | the on-terminate hooks of a bound relay, then every subscriber; a moved relay's plugins wait out their idle stop |

### `:relay`, run by `Teardown.stop_relay/3`

| Order | Row | Does |
|---|---|---|
| 10 | `:stream_flush` | flushes the runner's stream channels, at most 1 s |
| 20 | `:hub_stop` | stops the child through the hub (`hub_stop_children/3`); the declared entry goes first, so no reconcile brings it back. A stop that got no answer is asked again within the budget (`Flezha.Config.Backoff.again?/4`) |
| 25 | `:final_flush` | flushes the stream channels again, so the Cells' last changes the runner shipped as it dropped the relay land before `:state_dir`, and a deleted relay's files are not written again for the next start of its id |
| 30 | `:state_dir` | on the node the Overwatch ran on: deletes the state, or keeps it for a rescale (`reclaim_state_dir/2`) |
| 40 | `:quota` | gives back the relay and its Cells |
| 50 | `:announce` | `[:flezha, :relay, :stopped]`, which controller plugins hear; `Flezha.Observability.HookSink.stopped/4` is attached to it |

The cause picks the rows (`acts?/2`): `:stop` and `:complete` run all six, `:rescale` all but
`:announce`, `:failed_start` only `:hub_stop`.

### `:namespace`, run by `FleetManager`'s `{:stop_namespace, _}`

| Order | Row | Module | Does |
|---|---|---|---|
| 10 | `:relays` | `Teardown` | `stop_all/2`: every relay through the hub in one call, state directories reclaimed, no quota released |
| 20 | `:runners` | `Flezha.Runner.FleetManager.Session` | marks each active runner retiring and stops its supervision tree |
| 40 | `:hub` | `Flezha.Service.HubManager.Lifecycle` | `HubManager.stop_hub/1`, which also wipes the registry directory |

The namespace reads as quiesced while its walk runs.

### `:node`, run by `Flezha.Shutdown.Coordinator`

The coordinator's quiesce marks the node (`Shutdown.node_phase/1`) and declares
`{:node_draining, ...}` to the loop. The drain is the rows below order 20 and halts on a
failure; finalise is the rows from 20, each failure logged and the next row run.

| Order | Row | Module | Does |
|---|---|---|---|
| 10 | `:drain` | `HubManager.Lifecycle` | `drain_hub/2` for every running namespace at once: live handover through the consent gate, then a wait for the local children to exit |
| 20 | `:fsync` | `Flezha.State.FsyncBatcher` | syncs every pending write |
| 30 | `:audit_log` | `Flezha.Observability.AuditLog.Writer` | closes the audit log |
| 40 | `:hubs` | `HubManager.Lifecycle` | terminates `HubManager` in `Flezha.Supervisor`, so it is not restarted |
| 50 | `:transports` | `Flezha.Transport.Server` | terminates every transport the same way |

Below the walk, `Flezha.Supervisor` stops its children in reverse order of the rows in
`Flezha.Application.controller_children/0`. `Flezha.Process.DurableTasks` sits before the state
side, and each task started through `DurableTasks.start/1` traps exits, so the stop waits for it,
up to five seconds, before the writers close.

## Guards

What an Overwatch refuses or holds while a transition is under way:

| Clause | Guard | Does |
|---|---|---|
| `handle_call({:io_in, _, _}, ...)` | `halt_reason != nil` | answers `{:error, :startup_failed}` |
| `handle_call({:ingest, _, _}, ...)` | `halt_reason != nil` | answers `{:error, :not_ready}` |
| `handle_cast({:io_in, _, _}, ...)` | `halt_reason != nil` | drops the input and counts it (`halted_drops`) |
| `handle_cast` of `:io_out` / `:io_out_batch` | `bind_task != nil` and (`phase == :suspended` or `halt_reason != nil`) | holds the output until the bind is adopted |
| `handle_call({:registry_runner_set, _}, ...)` | `halt_reason != nil` | refuses the start's input with the reason |
| `handle_info({:stream_bound, _}, ...)` | `halt_reason != nil` | does not re-forward the input tail |
| `handle_cast({:kick_restore, _}, ...)` | `Replay.carrier/1` names `:handover` | skips the WAL restore |
| `Rebind.rebind/2` | `bind_task != nil` | answers `{:error, :busy}` |
| `Rebind.session_replaced/4` | `bind_task != nil` | leaves a restarted runner's binding to the bind under way |
| `Suspend.wake/2` | `phase != :suspended` | does nothing; with a bind in flight it carries `then` onto it |
| `Rebind.apply_bind_result/2` | `phase != bind_task.from` | does not adopt the bound state |
| `Handover.consent/1` | corrections pending, an awake durable steps instance, or an effectively-once outbox owing output | defers the move |

## Adding a transition

A new transition, for example "drain a relay off its runner", touches these places:

1. **The Overwatch clause** that starts it, one line that hands the state to the module owning
   the phase (`Rebind` for a bind onto another runner, `Suspend` for sleep or rewind,
   `Lifecycle` for the process walk).
2. **Its cause**, set where it begins. If the process can end during it, decide in
   `Lifecycle.departure/1` whether it ends as `:moved` or `:stopped`: a stop by name joins
   `@named_stops`, anything else falls back to the declared list.
3. **Its rows**, only if it releases something: a row in the `shutdown_steps/0` of the module
   that owns the resource, with an order unique in its scope, and in `Teardown.acts?/2` the
   causes that run it.

The phase is written only by the transition functions above; a new phase also needs its
verdict in `Probe.Judge`'s `decide(:relay, ...)`. A new node stop row is one entry in its own
module's `shutdown_steps/0`: below order 20 it is part of the drain, from 20 part of finalise.
A new child of the controller tree is one `{child, gate}` row in
`Flezha.Application.controller_children/0`, placed by the order it must start and stop in.
