# Procedure-requested spawn

For operative authors. `ctx.spawn_workflow` (Rust), `ctx.spawnWorkflow` (Java) and
`Flezha.SDK.Ctx.spawn_workflow/5` (Elixir) let a running procedure — in a stream or a workflow
operative — spawn-or-feed a workflow instance in its own namespace with one call. This page is
the contract: what the verb does, returns, refuses and inherits.

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

```elixir
Flezha.SDK.Ctx.spawn_workflow(ctx, "triage@" <> subject, "triage", input, :deliver)
```

Worked example: `sdk/rust/examples/spawn_workflow_worker.rs`.

## One verb: spawn-or-deliver

The existence check and the delivery race in any caller that has `spawn` and `send` as two
calls; one verb resolves the race in the controller's operative registry:

| The id is | `IfRunning::Deliver` | `IfRunning::Error` |
|---|---|---|
| free | `Spawned` (payload = first input) | `Spawned` |
| taken | `Delivered` (payload = one more input) | `AlreadyRunning` |

Concurrent requests for one id resolve to **exactly one** spawn plus deliveries for the rest.
A completed, torn-down id counts as free, so a request arriving after the worker finished
starts a fresh one — workers must be idempotent per batch.

The payload is the instance's first input by construction: it rides the `:seed_input` start
opt and is ingested inside the instance's bind, ahead of anything else in its mailbox. A
durable instance whose inbox already holds entries is a restart, not a spawn, and is not
re-seeded.

## Failures resolve the handle, never `run`

| Outcome | Meaning |
|---|---|
| `AlreadyRunning` | the id is taken and the request said `Error` |
| `FactoryUnknown` | no connected runner **on any cluster node** registers that factory |
| `Refused(reason)` | authorization is on and `runner@<namespace>` holds no role that grants the spawn: bind it with `flz role bind runner@<namespace> spawner <namespace>`, which also lets that connection call `Operative.start` and `Operative.send` on the [administration surface](admin.md) and nothing else ([`authn_authz.md`](../operations/authn_authz.md)); quota exhausted; controller shutting down; start failed |
| `Timeout` | no answer within 30 s |
| `Unavailable` | no control plane to request on |

A start is judged the way `flz operative start` is and recorded the same way; a delivery
is judged as `operative.send` and recorded only when refused, because it happens once per
event.

Factory resolution is cluster-wide: a controller node with no runner for the factory routes
the call to a node that has one, so `FactoryUnknown` is an honest cluster-wide verdict.

Reading the handle is optional (Rust `wait()` / `try_result()`, Java `await()` /
`tryResult()`, Elixir `Flezha.SDK.Spawn.await/2` / `Flezha.SDK.Spawn.try_result/1`). An
uninspected handle still reports a failure once, as a warning line: Rust through one
process-wide reaper thread on drop, Java from the reply callback unless the caller already
claimed the handle, Elixir from the runner process that answered it unless a read claimed the
handle first.

An Elixir handle's answer is a message to the process that made the request, so read it inside
the invocation that made it or not at all. A reply that arrives after `run/3` returned is
dropped, and the warning is then what reaches you.

## Replay issues nothing

A replayed invocation — a crash-resume re-forward, a late-input correction re-running the
slice — answers `SkippedOnReplay` / `SKIPPED_ON_REPLAY` / `{:ok, :skipped_on_replay}` without touching the wire, so a replay
never doubles the workers a run created.

## Inheritance and limits

The request resolves on the path `flz operative start` uses, so the spawned instance is an
ordinary workflow operative: namespace quota (`max_operatives`,
`max_entities_per_operative`), durable inbox, loop and suspension, `WorkflowCompleted`
teardown, retention, `flz operative list`. There is no separate registry, no spawn journal, no
scheduling (that is `flz schedule`), no cross-namespace reach.

- **A buggy requester can spawn in a loop.** The namespace quota is the backstop;
  `[:flezha, :operative, :requested_spawn]` carries `namespace`, `instance_id`, `factory_id`
  and `outcome` per request for rate alerting. There is no per-requester rate limit.
- **A runner partitioned from its controller fails async**, like any control-plane send.
- **Id reuse right after completion spawns fresh** instead of delivering.

## See also

- [`protocol_spec.md`](protocol_spec.md) — `SpawnWorkflowRequest` / `SpawnWorkflowResponse`
- [`../resilience/workflow_operative.md`](../resilience/workflow_operative.md) — the spawned
  instance's lifecycle
- [`../plugins/operative_forward.md`](../plugins/operative_forward.md) — feeding an existing
  operative as configuration
