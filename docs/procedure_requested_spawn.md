# Procedure-requested spawn: a running procedure asks for a worker

A procedure that detects a condition — a fraud signal, a batch that just
closed, an alert worth investigating — often wants a short-lived worker for it.
Before this capability the only way to start a workflow operative was from
outside the cluster (`flz operative start`, or a cron `flz schedule`), so every
pipeline that needed one grew its own external dispatcher.

`ctx.spawn_workflow` (Rust) / `ctx.spawnWorkflow` (Java) closes that gap: one
call, from inside any procedure, spawns-or-feeds a workflow instance in the
requester's own namespace.

## Contract

| | |
|---|---|
| Callable from | any `Procedure::run` — stream or workflow |
| Target kind | **workflow** only (convergence owns stream lifecycles) |
| Namespace | the requester's, always — the wire carries no namespace field |
| Verb | spawn-**or**-deliver on `(namespace, instance_id)` |
| Blocking | never: the call returns a handle before the controller decides |
| Failure posture | isolated — a refusal never faults `run`, its emits, or its journaling |
| Replay | live processing only; a replayed invocation issues nothing |

```rust
let _ = ctx.spawn_workflow(
    format!("triage@{subject}"),   // instance id — the identity that dedups
    "triage",                      // workflow factory id
    input.as_bytes().to_vec(),     // first input on a spawn, one more on a delivery
    IfRunning::Deliver,
);
```

```java
ctx.spawnWorkflow("triage@" + subject, "triage", input.getBytes(UTF_8), IfRunning.DELIVER);
```

Worked examples: `sdk/rust/examples/spawn_workflow_worker.rs` and
`sdk/java/flezha-sdk-examples/.../E08_SpawnWorkflowWorker.java`.

## Spawn-or-deliver is one verb, deliberately

The dominant pattern is "the first event creates the worker, the following
events feed it". Exposing that as `spawn` plus `send` would put a race between
the existence check and the delivery in every caller. One verb moves the race
to the one place that can resolve it — the controller's operative registry:

| The id is | `if_running: deliver` | `if_running: error` |
|---|---|---|
| free | `spawned` (payload = first input) | `spawned` |
| taken | `delivered` (payload = one more input) | `already_running` |

Concurrent requests for one id resolve to **exactly one** spawn plus deliveries
for all the others: the losers of the registry's uniqueness check convert to
the same delivery a later request would have made. A completed, torn-down id
counts as free, so a request arriving after the worker finished starts a fresh
one — workers must be idempotent per batch.

### The payload really is the first input

The spawn carries its payload as the instance's `:seed_input`, ingested inside
the instance's bind continue. Nothing else in that process's mailbox — including
a concurrent requester's delivery — can be ingested ahead of it, so the ordering
guarantee is structural rather than timing-dependent. A durable instance whose
inbox already holds entries is a restart rather than a spawn, and is not
re-seeded.

## Failures never reach the requester's `run`

The call returns a handle immediately. Everything that can go wrong resolves
that handle instead of the procedure:

| Outcome | Meaning |
|---|---|
| `AlreadyRunning` | the id is taken and the request said `error` |
| `FactoryUnknown` | no connected runner **on any cluster node** registers that factory |
| `Refused(reason)` | quota exhausted, controller shutting down, start failed |
| `Timeout` | no answer within 30 s |
| `Unavailable` | no control plane to request on |

Factory resolution is cluster-wide: when the requester's runner is connected
to a controller node that holds no runner for the factory, the controller
resolves the capable nodes through the cluster directory and executes the
spawn-or-deliver on one of them, so `FactoryUnknown` is an honest cluster-wide
verdict rather than a node-local one.

Reading the handle is optional. A handle nobody inspects still reports a
failure — once, as a warning line — so fire-and-forget never means
fire-and-forget-the-diagnosis. Rust hands an unobserved handle to a single
process-wide reaper thread on drop; Java logs from the reply callback unless the
caller has already claimed the handle.

This isolation is the point: a detector's correctness must not depend on worker
availability.

## Replay issues nothing

Spawning is a side effect of **live** processing. A replayed invocation — a
crash-resume re-forward, or a late-input correction re-running the slice —
answers `SkippedOnReplay` / `SKIPPED_ON_REPLAY` without touching the wire, so a
replay never doubles the workers a run created.

The workers a replay *would* have created are not re-requested. That is the
correct trade: the first run already created them, and a workflow worker that
finished is finished.

## What it inherits, and what it does not add

The request is resolved on the same operative-management path
`flz operative start` uses, so the spawned instance is an ordinary workflow
operative in every respect: namespace quota (`max_operatives`,
`max_entities_per_operative`), the durable inbox, loop and suspension
semantics, `WorkflowCompleted` teardown, retention, and `flz operative list`.

Nothing new is added: no separate registry, no spawn journal, no scheduling
(that is `flz schedule`), and no cross-namespace reach.

## Limits worth knowing

- **A buggy requester can spawn in a loop.** The namespace's bounded quota is
  the backstop; `[:flezha, :operative, :requested_spawn]` carries the outcome
  per request for rate alerting. A hard per-requester rate limit is deliberately
  not built until real abuse appears.
- **A request from a runner partitioned from its controller fails async**, like
  any control-plane send. Fire-and-forget callers are built for that.
- **Id reuse right after completion spawns fresh.** A delivery aimed at a run
  that just finished starts a new one instead.

## See also

- `docs/sdk_protocol_spec.md` — `SpawnWorkflowRequest` / `SpawnWorkflowResponse`
- `docs/resilience/workflow_operative.md` — the lifecycle the spawned instance follows
- `docs/operative_forward_plugin.md` — feeding an operative that already exists,
  as configuration rather than from procedure code
