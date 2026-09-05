# Overwatch Lifecycle

For controller contributors. Every operative has one `Flezha.Operative.Overwatch`
GenServer on the controller that binds it to a runner, drives its plugins
through their two phases, feeds it input and tears it down. This doc covers the
boot sequence, the ping-before-start rule, the parked-error state and the three
ingest entry points. What the convergence loop does *to* an Overwatch is in
[`convergence_loop.md`](convergence_loop.md); the plugin phases are in
[`../plugins/lifecycle.md`](../plugins/lifecycle.md). The code is split across
`Flezha.Operative.Overwatch` (the process), `Lifecycle` (bind, retry, park),
`Plugins` (prepare / init state / activate) and `Ingest` (journal and verdict).

## Boot sequence

1. **`init/1`** returns `{:ok, state, {:continue, {:bind_runner, …}}}` at once.
   It is called synchronously from the distributed supervisor's DETS replay,
   and a call into `FleetManager` / `OperativeManager` from here deadlocks
   (they are waiting on the hub, which is waiting on this supervisor).
2. **`handle_continue({:bind_runner, …})`** first claims the class's
   durable-state namespace (`Flezha.State.Paths.claim_namespace/2`); a class
   whose state belongs to another namespace stops the Overwatch with
   `{:shutdown, {:namespace_conflict, id, owner}}` rather than read or write
   it. It then waits up to `:overwatch_runner_wait_ms` (30 s) for an active
   runner advertising the operative's `factory_id`
   (`OperativeManager.await_runner/5`); the persisted `runner_id` is advisory
   only. No runner in time exits with `:runner_not_ready` and startup is
   deferred.
3. **`complete_init/4`** runs, in order:
   - **prepare** — `Plugins.prepare/2` registers each plugin's GenServer and
     runs `init_provider/1`; plugins enter `:prepared` in
     `Flezha.Plugin.Extension.Table` and open no upstream connection.
   - **resolve init values** — `Plugins.resolve_init_state/2` runs the
     `:on_operative_init` fold chain unless inline `init_values` are non-empty.
   - **start the SDK, once** — the live runner is re-resolved, then ONE
     ping → `AddOperative` attempt is made; the binding is committed to the
     runner it was made on. A failure parks; the next attempt is the
     convergence loop's.
   - **decode declarations** — the class's SDK-side declarations ride the
     `AddOperative` response (`effectively_once`, `correction_window_ms`) and
     are resolved before activation so source adapters pick the tier's
     blocking ingest mode.
   - **restore and replay** — under the effectively-once tier every persisted
     Entity is restored and the journaled input tail re-forwarded before any
     source activates, so replayed offsets reach the runner ahead of fresh ones.
   - **activate** — `Plugins.activate/1` calls `Table.bulk_activate/2`; each
     plugin moves to `:running` and a provider's optional `activate/1` runs now
     (the WebSocket provider opens its connection and subscribes here, never in
     `init_provider/1`).

## Ping before `AddOperative`

The attempt issues a `Ping` control-channel round trip
(`Flezha.Runner.ControlUnit.ping_runner/2`, deadline
`:overwatch_ping_timeout_ms`) before `AddOperative`. It traverses the same
`ControlChannel → SDK → ControlChannel` path, so a successful round trip proves
the channel is bound and the reply path intact. The gate is
`:overwatch_ping_enabled` (default `true`).

A ping failure does not reset the channel here. The attempt parks the operative
with the ping's reason, and `Lifecycle.park/2` classifies what that reason says
about the transport (`transport_verdict/1`: `:dead`, `:busy`, or unclassified)
and declares both to the convergence loop. The loop plans
`reset_control_channel` for the runner — before the rebind when the transport is
dead, and only once the `{:parked, id}` key has walked two rungs when the ping
merely timed out, because the channel is live and carries every other instance
on that runner. The timeout class is `:timeout`,
`{:control_unit_exit, {{:timeout, _}, _}}` and `{:control_unit_exit, {:timeout, _}}`.

## One attempt, and the parked-error state

There is no retry ladder in the Overwatch: it holds no delay list, no jitter and
no per-attempt runner re-resolution. When the one attempt fails the Overwatch
**parks** instead of crashing its supervisor, declares
`{:operative_parked, namespace, operative_id, reason}`, and the loop drives the
un-park `rebind_operative` on the `{:parked, operative_id}` key of its own table
— eligible at the declaring tick, then 5 s, 30 s, 60 s, 300 s. A runner
connecting in the namespace releases every parked key in it, so a runner
arriving mid-cooldown is used at once.

| While parked | Behaviour |
|---|---|
| `state.init_error` | holds the last failure (`{:ping_failed, reason}` or `{:start_operative_failed, reason}`) |
| `call :registry_runner_set` | replies `{:error, reason}` |
| `call {:io_in, msg, md}` | replies `{:error, :startup_failed}` |
| `cast {:io_in, msg, md}` | dropped silently |
| `call {:ingest, msg, md}` | replies `{:error, :not_ready}`, so a forwarding sink retains and retries |
| plugins | stay `:prepared` — no upstream connection, no `:io_in` traffic, memory at the idle baseline |

Un-parking is event-driven, not timed: when the convergence loop resolves a
healthy runner for the factory it sends `{:runner_migrate, runner_id}`, and the
Overwatch re-runs `complete_init/4` in a migrate task (`:unpark` mode); on
success `init_error` clears.

## Ingest entry points

Every inbound payload runs exactly one pre-delivery path — the `:on_input`
extension chain, then `journal_input/3` (the durable input log under the
effectively-once tier, the durable inbox of a durable workflow instance). Three
entry points drive it and differ only in what they report:

| Entry point | Caller | Reports |
|---|---|---|
| `cast {:io_in, msg, md}` | plugins, source adapters, `OperativeManager.send_io/4` | nothing |
| `call {:io_in, msg, md}` | source adapters in blocking-ingest mode (the WebSocket provider under the tier) | `:ok`, or `{:error, :startup_failed}` when parked |
| `call {:ingest, msg, md}` | `OperativeManager.send_io_confirming/5` — the operative-forward plugin | a typed verdict |

The blocking `:io_in` call exists because the tier parks ingestion at zero
source credit, which only back-pressures a source that awaits the reply. Under
the tier both calls are answered when the journal answers the append: the
Overwatch hands the append over (`InputLog.append_async/5`, or
`ProcInbox.append_async/5` for a durable workflow) and keeps serving, so the
park lands on the caller and never on the operative's host — which is what
keeps the output chains that mint the credit running.

The confirming `:ingest` call reports what happened, because its caller is a
retry loop that must tell "done" from "try again":

| Verdict | Meaning |
|---|---|
| `:ok` | forwarded to the bound runner, or durably journaled |
| `{:ok, :absorbed}` | filtered by an `:on_input` plugin, or absorbed as a duplicate |
| `{:ok, :suspended}` | durable in the inbox; delivered when the instance rehydrates |
| `{:error, :not_ready}` | parked with an `init_error`, or unbound with no durable log |
| `{:error, :refused}` | the durable log rejected the append |

`{:error, :not_found}` (no pid for `(namespace, operative_id)`) is minted by
`OperativeManager`, not by the Overwatch. Two consequences: "filtered" and
"duplicate" are successes — retrying either is wasted work or an infinite loop,
since a deterministic filter filters the retry too; and an unbound stream is
not uniformly an error — a target with a durable log already holds the payload
and the bind delivers it, so reporting `not_ready` there would make the sender
re-append at a new offset and manufacture a duplicate. The plugin that consumes
these verdicts: [`../plugins/operative_forward.md`](../plugins/operative_forward.md).

## Runtime config

| Key | Default | Purpose |
|---|---|---|
| `:overwatch_runner_wait_ms` | `30_000` | how long the bind waits for an active runner |
| `:overwatch_ping_timeout_ms` | `2_000` | per-ping deadline |
| `:overwatch_ping_enabled` | `true` | gate for the ping pre-check |

## See also

- [`convergence_loop.md`](convergence_loop.md) — the loop that rebinds and un-parks
- [`../plugins/lifecycle.md`](../plugins/lifecycle.md) — `init_provider/1` vs `activate/1`
- [`../plugins/operative_forward.md`](../plugins/operative_forward.md) — the verdict consumer
- [`../resilience/overview.md`](../resilience/overview.md) — where the Overwatch sits among the healers
- [`../operations/memory_drift.md`](../operations/memory_drift.md) — investigating BEAM memory growth under load
