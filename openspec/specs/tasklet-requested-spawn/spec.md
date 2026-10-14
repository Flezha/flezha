# tasklet-requested-spawn Specification

## Purpose

The tasklet-requested-spawn capability lets a running tasklet ask the cluster for a steps relay, closing the gap that made a steps instance startable only from outside the cluster (`flz relay start`, `flz schedule`) and forced every pipeline needing a worker to bolt an external dispatcher onto the controller. One SDK call — `ctx.spawn_steps` (Rust) / `ctx.spawnSteps` (Java) — spawns-or-delivers on a single `(namespace, instance_id)` in the requester's own namespace: a free id starts the factory with the payload as its first input, a taken id receives the payload as one more input or refuses. Concurrent requests race-resolve at the relay registry to exactly one instance. The call is fire-and-forget with an async result: quota, transport, and controller failures resolve the caller's handle and never fault the requesting tasklet, its emit path, or its journaling. Spawning is a side effect of live processing only, so a replay never doubles the workers a run created. Everything else — quota, the durable inbox, exit triggers, `RelayCompleted` teardown, retention, `flz relay list` — is the existing steps lifecycle, inherited unchanged.
## Requirements
### Requirement: A tasklet SHALL be able to request spawn-or-deliver of a steps relay

Every SDK SHALL expose a context call that takes a steps relay `factory_id` and a `payload` first,
and an optional `id` and an optional `if_running` as named arguments in the form its language
has, usable from inside any tasklet: Rust, Java, Node, Python, Ruby, PHP and Elixir through the
shared core. The call shapes SHALL be:

- Rust: `ctx.spawn_steps(factory_id, payload)` and
  `ctx.spawn_steps_with(factory_id, payload, SpawnOptions::id(id).if_running(IfRunning::Error))`
- Java: `ctx.spawnSteps(factoryId, payload)` and
  `ctx.spawnSteps(factoryId, payload, SpawnOptions.id(id).ifRunning(IfRunning.ERROR))`
- Elixir: `Flezha.SDK.Ctx.spawn_steps(ctx, factory_id, payload, id: id, if_running: :error)`
- Python: `ctx.spawn_steps(factory_id, payload, id=id, if_running="error")`
- Ruby: `ctx.spawn_steps(factory_id, payload, id: id, if_running: "error")`
- PHP: `$ctx->spawnSteps($factoryId, $payload, id: $id, ifRunning: 'error')`
- Node: `ctx.spawnSteps(factoryId, payload, { id, ifRunning: 'error' })`

With an `id` and `if_running: deliver` (the default when an `id` is given), the controller SHALL
spawn a steps instance of `factory_id` under that id if absent, or deliver `payload` to the
existing instance as one more message if present. With an `id` and `if_running: error`, an existing instance
SHALL fail the requester's read with the code `relay.already_running`. Without an `id`, the controller SHALL
generate one as the `generated-relay-ids` capability defines and SHALL always spawn a new
instance: there is no running instance to deliver to, so `if_running` has no meaning. Rust and
Java SHALL construct `SpawnOptions` only from an id, so `if_running` without an `id` cannot be
written there; Elixir, Python, Ruby, PHP and Node SHALL raise an argument error at the call when
`if_running` is given without an `id`, and SHALL send nothing.

The payload SHALL be encoded as the SDK's output with no codec encodes a value, the rule `ctx.complete`
already applies: bytes as they are, a string as UTF-8, any other value as JSON. It SHALL be an ordinary
entry message of the spawned instance, delivered through the entrance every message takes: it names no
channel, so it SHALL enter the instance's first declared entry channel, be decoded by that channel's
codec or parser, and run the chain that channel roots, in every loop mode. The spawned instance SHALL
follow all existing steps lifecycle semantics (quota, exit triggers, `RelayCompleted`, teardown,
retention). The handle SHALL resolve to a result carrying `id`, the instance id the request
resolved to (the caller's or the generated one), and `outcome`, one of `spawned`, `delivered`
or `skipped_on_replay`.

The request SHALL NOT carry a namespace: the controller resolves it against the control channel the request arrived on, so a request cannot reach another namespace.

Factory resolution SHALL be cluster-wide: the controller SHALL route the spawn to a runner registering `factory_id` connected to ANY controller node in the cluster, not only runners connected to the node that received the request. `relay.factory_unknown` SHALL be yielded only when no connected runner on any cluster node registers the factory, resolved against the fleet state current at request time. A factory that is registered but whose established kind is `flow` SHALL yield `relay.factory_not_steps`, decided from the fleet's factory record before anything is placed, so the requester reads the reason rather than a runner's late error; the code SHALL be the same string in every SDK, read from the SDK's one error type. A failed read SHALL carry the controller's own code for every other refusal (for example `authz.role_insufficient` or `quota.exceeded`), and `spawn.timeout` or `spawn.unavailable` when no answer came. The success outcomes `spawned`, `delivered` and `skipped_on_replay` are not errors and keep their words.

#### Scenario: First request spawns, second delivers

- **WHEN** two tasklets issue a spawn of factory `worker` with id `w1` and `deliver`, carrying `p1` then `p2`, in sequence
- **THEN** exactly one steps instance `w1` SHALL exist, its inbox SHALL have received `p1` then `p2`, and the two results SHALL be `{id: "w1", outcome: spawned}` and `{id: "w1", outcome: delivered}`

#### Scenario: error policy reports instead of delivering

- **WHEN** a spawn of factory `worker` with id `w1` and `error` is issued while `w1` runs
- **THEN** no delivery SHALL occur and the requester's read SHALL fail with `relay.already_running`

#### Scenario: A spawn without an id starts a new instance every time

- **WHEN** a tasklet issues `ctx.spawn_steps("worker", p1)` and then `ctx.spawn_steps("worker", p2)` with no id
- **THEN** two steps instances SHALL exist, each with a generated id beginning `worker-`, the first holding `p1` and the second `p2`, and both results SHALL have outcome `spawned` and differing ids

#### Scenario: A once steps runs on the payload it was spawned with

- **WHEN** a tasklet issues `ctx.spawn_steps("paper_order", ticket)` and `paper_order` is a `once` steps relay whose entry channel decodes the ticket's type
- **THEN** the spawned instance's first tasklet SHALL receive the decoded ticket, the pass SHALL complete and the instance SHALL end with status `ok`, on every SDK

#### Scenario: A host SDK's payload is readable by a text entry

- **WHEN** a Python, Ruby, Node or PHP tasklet issues `ctx.spawn_steps("place", "o-1")` and `place`'s entry channel is declared with the `text` codec
- **THEN** the spawned instance's tasklet SHALL receive the string `o-1`, and a payload that is a dict, hash, object or array SHALL arrive as its JSON

#### Scenario: if_running without an id is refused at the call

- **WHEN** a Python tasklet calls `ctx.spawn_steps("worker", p, if_running="error")` with no id
- **THEN** the call SHALL raise an argument error naming `if_running` and `id`, and no request SHALL leave the runner

#### Scenario: Factory on a different runner than the requester

- **WHEN** a tasklet on the streaming runner requests spawn of a factory registered only by a separate analyzer runner (each runner holds its own sealed relay registry)
- **THEN** the controller SHALL route the spawn to a runner that has registered `factory_id`, and SHALL fail the read with `relay.factory_unknown` if no connected runner registers it

#### Scenario: Factory on a runner connected to a different controller node

- **WHEN** a tasklet whose runner is connected to controller node A requests spawn of a factory whose only runner is connected to controller node B
- **THEN** the controller SHALL start the instance on node B and the requester's async result SHALL be `spawned` (or `delivered` if the id already exists), not `relay.factory_unknown`

#### Scenario: A flow factory is named by the answer

- **WHEN** a tasklet issues `ctx.spawn_steps("prices", p)` and `prices` is a factory its runner announced as a flow relay
- **THEN** the requester's read SHALL fail with `relay.factory_not_steps`, no instance SHALL be placed or started, and the code SHALL NOT be `relay.factory_unknown`

#### Scenario: Every SDK exposes the call with one meaning

- **WHEN** the capability ships in an SDK
- **THEN** that SDK SHALL expose the call in the shape listed above with the semantics above, and its own suite SHALL cover the outcomes with and without an id, the error policy, `if_running` without an id, the refusal, the stream-factory answer, the timeout, the replay skip and the no-control-plane case

### Requirement: Spawn requests SHALL be idempotent under concurrency on (namespace, instance_id)

Concurrent `deliver`-mode requests that carry the same `(namespace, instance_id)` SHALL resolve to exactly one spawned instance, with every other request's payload delivered to it. A completed (torn-down) instance id SHALL count as absent for subsequent requests. A request that carries no id SHALL never be merged with another request: each such request SHALL spawn its own instance.

#### Scenario: Concurrent burst yields one instance

- **WHEN** N tasklets across different runners request a spawn of factory `worker` with id `w1` and `deliver`, each carrying its own payload, concurrently
- **THEN** exactly one `w1` SHALL spawn and all N payloads SHALL reach its inbox exactly once each

#### Scenario: Concurrent requests without an id are not merged

- **WHEN** N tasklets request a spawn of factory `worker` with no id concurrently
- **THEN** N instances SHALL spawn, each with its own generated id, and each result SHALL be `spawned`

#### Scenario: Id reuse after completion

- **WHEN** `w1` completes teardown and a new request for `w1` arrives
- **THEN** a fresh instance SHALL spawn (no delivery to the finished run)

### Requirement: Spawn failures SHALL be isolated from the requesting tasklet

The call SHALL return immediately with a local async result handle in every SDK. Transport, quota, or controller failures SHALL surface only via that handle (or one warning log if unobserved) and SHALL NOT fault the requesting tasklet's `run`, block its emit path, or affect its journaling/replay. No SDK SHALL wait for the controller's answer inside the call or raise a controller refusal from it.

A replayed invocation, the correction re-run the controller marks as a replay, SHALL issue no request and its handle SHALL resolve at once with outcome `skipped_on_replay`. Its `id` SHALL be the caller's id when the call gave one, and empty when it did not, because a generated id exists only in the controller's answer to the live run.

A crash re-run is not a replay: an input that was not durably consumed runs live, and its spawn SHALL be issued again. Every spawn requested from a tasklet invocation SHALL carry an identity made of the requesting instance, the input's `flz.offset`, the call's position within the invocation, the factory and a hash of the payload. A request whose identity was already resolved SHALL resolve to the same relay the first request did and SHALL NOT start a second instance or deliver its payload a second time: without an id, the generated id SHALL be derived from the identity; with an id and `if_running: :deliver`, the delivered payload SHALL carry the identity as its `flz.msg_id`. A re-run whose request differs in factory or payload SHALL get a new identity and a new spawn, never another request's answer. Spawns requested from outside a tasklet (the CLI, the admin API, a schedule) SHALL carry no identity and are unchanged.

#### Scenario: Controller unreachable

- **WHEN** a tasklet calls the spawn while the control plane is down
- **THEN** the tasklet's `run` SHALL complete normally and the failure SHALL appear only on the async result / warning log

#### Scenario: Replay does not re-issue side effects unexpectedly

- **WHEN** a tasklet that called the spawn is replayed from the WAL
- **THEN** no spawn request SHALL be issued during replay, and the handle SHALL resolve with outcome `skipped_on_replay`

#### Scenario: A replayed spawn without an id answers an empty id

- **WHEN** a tasklet that called `ctx.spawn_steps("worker", p)` with no id is replayed, after the live run's result was `{id: "worker-<suffix>", outcome: spawned}`
- **THEN** the replayed handle SHALL resolve to `{id: "", outcome: skipped_on_replay}` and no second instance SHALL spawn

#### Scenario: A replayed spawn with an id answers that id

- **WHEN** a tasklet that spawned with id `w1` is replayed
- **THEN** the replayed handle SHALL resolve to `{id: "w1", outcome: skipped_on_replay}`

#### Scenario: A crash re-run does not double a worker

- **WHEN** a tasklet spawns `worker` with no id while taking offset 7, the spawn is answered `{id: "worker-<suffix>", outcome: spawned}`, and the runner dies before offset 7 is durably consumed
- **THEN** the re-run's identical request SHALL resolve to `worker-<suffix>` and no second instance SHALL exist

#### Scenario: A crash before the request reached the controller loses no worker

- **WHEN** a tasklet taking offset 7 dies before its spawn request reaches the controller
- **THEN** the re-run's request SHALL spawn the worker once

#### Scenario: A re-run that asks for something else spawns anew

- **WHEN** the re-run of offset 7 requests a different factory or payload at the same call position
- **THEN** it SHALL be answered as a new spawn and SHALL NOT resolve to the first request's relay

#### Scenario: A re-delivered payload to a named worker is absorbed

- **WHEN** a tasklet delivers payload `p` to the running worker `w1` while taking offset 7, and the re-run delivers the same request again
- **THEN** `w1`'s input log SHALL hold `p` once and `w1` SHALL receive it once

### Requirement: A spawn request SHALL be judged as the act it performs

The controller SHALL judge each act a spawn request performs as the identity the request's connection proved, at the same policy point as the user-issued equivalent and in the connection's namespace: a start as `relay.start` through the `relay_start` row, and a delivery to a running instance as `relay.send`, the right the `relay_send` row judges. A permitted start SHALL write the `relay_start` row's audit record naming the connection's principal, the SDK as the surface, and the instance id the start resolved to, which for a request that carried no id is the generated one. A permitted delivery SHALL write no record of its own, because a delivery happens once per event. A refused act SHALL start nothing and deliver nothing, its denial SHALL be audited, and the requester's result SHALL be `refused` with the refusal as its detail.

The judgment SHALL be made on the controller node that received the request, and no start and no delivery SHALL be forwarded to another node or performed before it. While authorization is disabled every act SHALL be permitted, and a start SHALL still write its record, as every other start does.

#### Scenario: A spawn by an identity that may not start is refused

- **WHEN** authorization is enforced and a tasklet on a runner whose
  `runner@<namespace>` principal has no role binding requests
  a spawn of factory `worker` with id `w1` and `deliver` while `w1` is free
- **THEN** no instance SHALL start, the requester's read SHALL fail with the policy's
  own code, `authz.no_role_binding`, and the denial SHALL be audited

#### Scenario: A delivery by an identity that may not send is refused

- **WHEN** authorization is enforced and the same unbound runner's request names
  `w1` while it runs
- **THEN** the payload SHALL NOT be delivered, the requester's read SHALL fail with
  `authz.no_role_binding`, and the denial SHALL be audited

#### Scenario: A permitted spawn is recorded like a user's start

- **WHEN** `runner@team-orders` is bound to `spawner` on `team-orders` and a
  tasklet on that connection spawns `w1`
- **THEN** `w1` SHALL start, and a `start` record on `relay_service` SHALL name
  `w1`, the namespace `team-orders`, the principal `runner@team-orders` and the
  surface `sdk`

#### Scenario: A permitted spawn without an id is recorded under the generated id

- **WHEN** the same bound runner's tasklet spawns factory `worker` with no id and
  the result is `{id: "worker-<suffix>", outcome: spawned}`
- **THEN** the `start` record on `relay_service` SHALL name `worker-<suffix>`,
  the same id the result carries

#### Scenario: A permitted delivery is judged and writes no record

- **WHEN** the same bound runner's request names `w1` while it runs
- **THEN** the payload SHALL be delivered, and no `send` record SHALL be written
  on `relay_service`

#### Scenario: A start another node would run is judged where it was asked

- **WHEN** the only runner registering the factory is connected to another
  controller node and the requesting identity may not start a relay
- **THEN** the request SHALL be refused on the node that received it, and no start
  SHALL be forwarded

#### Scenario: Authorization disabled

- **WHEN** authorization is disabled and a runner whose principal has no role
  binding requests a spawn of a free id
- **THEN** the instance SHALL start and its `start` record SHALL be written

### Requirement: Every SDK SHALL return the spawn handle at once and read it under its own names

Every SDK's spawn call SHALL return a handle before the controller answers, and SHALL offer one
read that waits for the answer and one that never waits. The read names SHALL be: Rust `wait()` and
`try_result()`; Java `await()` and `tryResult()`; Elixir `Flezha.SDK.Spawn.await/2` and
`Flezha.SDK.Spawn.try_result/1`; Python and Ruby `wait` and `try_result`; PHP and Node `wait()` and
`tryResult()`. Node's `spawnSteps` SHALL NOT be `async`: it SHALL return the handle
synchronously, and its `wait()` SHALL return a promise. A read that never waits SHALL answer that
the result is pending until the answer has arrived. A controller refusal, a timeout or an unavailable
control plane SHALL be reported by a read, in the form that SDK reports an error (a returned error,
a thrown exception or a rejected promise), and never by the spawn call itself. A handle SHALL be
read inside the invocation that made it; a failure no read collected by the end of that invocation
SHALL be logged once as a warning naming the instance id (empty when none was given), the factory id
and the reason.

#### Scenario: Python returns before the controller answers

- **WHEN** a Python tasklet calls `ctx.spawn_steps("worker", p)` and the controller has not answered
- **THEN** the call SHALL have returned a handle, and `try_result()` SHALL answer pending

#### Scenario: A Ruby refusal does not fault run

- **WHEN** the controller refuses a spawn a Ruby tasklet issued, and the tasklet emits a value and returns without reading the handle
- **THEN** the emit SHALL ship, the invocation SHALL NOT be retried, and one warning SHALL name the refusal

#### Scenario: PHP reads the refusal only when it asks

- **WHEN** a PHP tasklet calls `$ctx->spawnSteps('worker', $p)`, the controller refuses it, and the tasklet then calls `wait()` on the handle
- **THEN** the spawn call SHALL have returned normally, and `wait()` SHALL raise the refusal

#### Scenario: Node spawn is synchronous and its wait is a promise

- **WHEN** a Node tasklet calls `const h = ctx.spawnSteps('worker', p)` without `await`
- **THEN** `h` SHALL be a handle rather than a promise, `h.tryResult()` SHALL answer pending until the answer arrives, and `await h.wait()` SHALL resolve to `{ id, outcome }`

#### Scenario: A successful read carries the id

- **WHEN** any SDK's waiting read resolves a spawn that carried no id
- **THEN** the result SHALL carry the generated id and the outcome `spawned`

### Requirement: A spawn's wire exchange SHALL carry the id it resolved to

`SpawnStepsRequest.instance_id` SHALL be empty when the call gave no id, and the controller SHALL
read an empty id as a request to generate one and SHALL ignore `if_running` for it.
`SpawnStepsResponse` SHALL carry `instance_id`, the id the request resolved to, beside `outcome`
and `error`, on every answer that names an instance: the caller's id for a named request, the
generated id for a spawn without one. A refusal that started nothing SHALL carry the caller's id, or
an empty id when the request carried none.

#### Scenario: A request without an id is answered with the generated id

- **WHEN** a runner sends `SpawnStepsRequest{instance_id: "", factory_id: "worker"}`
- **THEN** the controller SHALL start one instance under a generated id and answer `SpawnStepsResponse{outcome: "spawned", instance_id: <that id>}`

#### Scenario: if_running is ignored without an id

- **WHEN** a runner sends `SpawnStepsRequest{instance_id: "", factory_id: "worker", if_running: "error"}`
- **THEN** the controller SHALL start a new instance and SHALL NOT answer `relay.already_running`

### Requirement: The dangers of a spawn without an id SHALL be documented

`docs/topics/core/workflows.md` and every SDK page that shows the spawn call SHALL state, next
to the call, that a spawn without an id always starts a new instance and can never deliver into a
running one; that a caller who reissues a spawn without an id after a lost or timed-out answer starts
a second instance; that a spawn without an id skipped on replay answers an empty id; and that a caller
who needs at most one instance, or the id on replay, passes its own id.

#### Scenario: The contract page states the dangers

- **WHEN** a reader opens `docs/topics/core/workflows.md`
- **THEN** it SHALL state each of the three dangers and that passing an id avoids them

#### Scenario: Every SDK page carries the same warning

- **WHEN** a reader opens any SDK page that shows `spawn_steps` or `spawnSteps`
- **THEN** the page SHALL carry the same three dangers beside the example

