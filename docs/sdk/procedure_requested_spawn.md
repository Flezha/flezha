# Procedure-requested spawn

For operative authors. `ctx.spawn_workflow` (Rust, Python, Ruby), `ctx.spawnWorkflow` (Java,
Node), `$ctx->spawnWorkflow` (PHP) and `Flezha.SDK.Ctx.spawn_workflow/4` (Elixir) let a running
procedure — in a stream or a workflow operative — spawn-or-feed a workflow instance in its own
namespace with one call. This page is the contract: what the verb does, returns, refuses and
inherits.

## Contract

| | |
|---|---|
| Callable from | any `Procedure::run` — stream or workflow |
| Target kind | **workflow** only (convergence owns stream lifecycles) |
| Namespace | the requester's, always — the wire carries no namespace field |
| Verb | spawn-**or**-deliver on `(namespace, instance_id)`; with no id, always a spawn under a generated one |
| Blocking | never: the call returns a handle before the controller decides, in every SDK |
| Failure posture | isolated — a refusal never faults `run`, its emits, or its journaling |
| Replay | live processing only; a replayed invocation issues nothing |

The factory and the payload come first; the id and the policy are optional named arguments:

```rust
let job = ctx.spawn_workflow("triage", input.as_bytes().to_vec());
let one = ctx.spawn_workflow_with(
    "triage",                              // workflow factory id
    input.as_bytes().to_vec(),             // first input on a spawn, one more on a delivery
    SpawnOptions::id(format!("triage@{subject}")).if_running(IfRunning::Deliver),
);
```

```java
SpawnHandle job = ctx.spawnWorkflow("triage", input.getBytes(UTF_8));
SpawnHandle one = ctx.spawnWorkflow("triage", input.getBytes(UTF_8),
        SpawnOptions.id("triage@" + subject).ifRunning(IfRunning.DELIVER));
```

```elixir
job = spawn_workflow(ctx, "triage", input)
one = spawn_workflow(ctx, "triage", input, id: "triage@" <> subject, if_running: :deliver)
```

```python
job = ctx.spawn_workflow("triage", payload)
one = ctx.spawn_workflow("triage", payload, id=f"triage@{subject}", if_running="deliver")
```

```ruby
job = ctx.spawn_workflow("triage", payload)
one = ctx.spawn_workflow("triage", payload, id: "triage@#{subject}", if_running: "deliver")
```

```php
$job = $ctx->spawnWorkflow('triage', $payload);
$one = $ctx->spawnWorkflow('triage', $payload, id: "triage@{$subject}", ifRunning: 'deliver');
```

```js
const job = ctx.spawnWorkflow('triage', payload);
const one = ctx.spawnWorkflow('triage', payload, { id: `triage@${subject}`, ifRunning: 'deliver' });
```

Rust and Java build the options only from an id, so a policy without an id cannot be written
there; the other five raise an argument error at the call and send nothing.

Worked example: `sdk/rust/examples/spawn_workflow_worker.rs`.

## One verb: spawn-or-deliver

The existence check and the delivery race in any caller that has `spawn` and `send` as two
calls; one verb resolves the race in the controller's operative registry:

| The id is | `IfRunning::Deliver` | `IfRunning::Error` |
|---|---|---|
| not given | `Spawned` under a generated id | (no policy without an id) |
| free | `Spawned` (payload = first input) | `Spawned` |
| taken | `Delivered` (payload = one more input) | `AlreadyRunning` |

A generated id is the factory id, trimmed to fit the 32-byte limit, a `-` and 16 characters
of time and randomness, such as `triage-k7m2qzxw4pd3fn2a`. A resolved handle answers the id the
request resolved to, given or generated, beside the outcome.

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

Reading the handle is optional. Each SDK has a read that waits and one that never waits:

| SDK | Waits | Never waits |
|---|---|---|
| Rust | `wait()` | `try_result()` |
| Java | `await()` | `tryResult()` |
| Elixir | `Flezha.SDK.Spawn.await/2` | `Flezha.SDK.Spawn.try_result/1` |
| Python, Ruby | `wait` | `try_result` |
| PHP | `wait()` | `tryResult()` |
| Node | `await wait()` | `tryResult()` |

A resolved read carries `{id, outcome}`; a failure comes out of the read in the form that SDK
reports an error (a returned error, a thrown exception, a rejected promise), never out of the
spawn call. The never-waiting read answers that the result is pending until it has arrived.

Read a handle inside the invocation that made it or not at all. A handle nobody read by the end
of that invocation reports a failure once, as a warning line naming the instance id (empty when
none was given), the factory id and the reason, and a later read answers that it was released.
An Elixir handle's answer is a message to the process that made the request, so a reply that
arrives after `run/3` returned is dropped, and the warning is then what reaches you.

## Replay issues nothing

A replayed invocation — a crash-resume re-forward, a late-input correction re-running the
slice — answers the outcome `skipped_on_replay` without touching the wire, so a replay never
doubles the workers a run created. Its id is the one the call gave, or empty when it gave none:
a generated id existed only in the controller's answer to the live run.

## Inheritance and limits

The request resolves on the path `flz operative start` uses, so the spawned instance is an
ordinary workflow operative: namespace quota (`max_operatives`,
`max_entities_per_operative`), durable inbox, loop and suspension, `WorkflowCompleted`
teardown, retention, `flz operative list`. There is no separate registry, no spawn journal, no
scheduling (that is `flz schedule`), no cross-namespace reach.

- **A buggy requester can spawn in a loop.** The namespace quota is the backstop;
  `[:flezha, :operative, :requested_spawn]` carries `namespace`, `instance_id` (the id the
  request resolved to), `factory_id` and `outcome` per request for rate alerting. There is no
  per-requester rate limit.
- **A runner partitioned from its controller fails async**, like any control-plane send.
- **Id reuse right after completion spawns fresh** instead of delivering.

## Dangers of a spawn without an id

- **It always starts a new instance.** It can never deliver into a running one, because there
  is no id to find it by.
- **Reissued after a lost or timed-out answer, it starts a second instance.** The first may
  have started; nothing ties the second request to it.
- **Skipped on replay, it answers an empty id.** The id was generated during the live run.

A caller that needs at most one instance, or the id on replay, passes its own id.

## See also

- [`protocol_spec.md`](protocol_spec.md) — `SpawnWorkflowRequest` / `SpawnWorkflowResponse`
- [`../resilience/workflow_operative.md`](../resilience/workflow_operative.md) — the spawned
  instance's lifecycle
- [`../plugins/operative_forward.md`](../plugins/operative_forward.md) — feeding an existing
  operative as configuration
