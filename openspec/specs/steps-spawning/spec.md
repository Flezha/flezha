# steps-spawning Specification

## Purpose

Starting steps relays from inside the cluster. A running tasklet can ask for a steps relay with one
SDK call that spawns or delivers on a single id in its own namespace: concurrent requests resolve to
exactly one instance, failures resolve the caller's handle and never fault the tasklet, and the
request is authorized as the act it performs and names the relay that asked. The controller also
keeps a durable registry of cron schedules: each slot fires exactly once across the cluster through
the same spawn path, missed slots are skipped, failed slots are retried until the next slot
supersedes them, and the started instance records its schedule.

## Requirements

### Requirement: A tasklet SHALL be able to request spawn-or-deliver of a steps relay

Every SDK SHALL expose a context call that takes a steps relay `factory_id` and a `payload` first,
and an optional `id` and an optional `if_running` as named arguments in the form its language
has, usable from inside any tasklet: Rust, Java, Node, Python, Ruby, PHP, Go and Elixir through the
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
- Go: `ctx.SpawnSteps(factoryID, payload, flezha.SpawnID(id), flezha.IfRunning("error"))`

With an `id` and `if_running: deliver` (the default when an `id` is given), the controller SHALL
spawn a steps instance of `factory_id` under that id if absent, or deliver `payload` to the
existing instance as one more message if present. With an `id` and `if_running: error`, an existing instance
SHALL fail the requester's read with the code `relay.already_running`. Without an `id`, the controller SHALL
generate one as the `relay-model` capability defines and SHALL always spawn a new
instance: there is no running instance to deliver to, so `if_running` has no meaning. Rust and
Java SHALL construct `SpawnOptions` only from an id, so `if_running` without an `id` cannot be
written there; in Elixir, Python, Ruby, PHP, Node and Go the core SHALL refuse the request at the call
with `spawn.if_running_without_id` when `if_running` is given without an `id`, the SDK SHALL raise
that refusal as its one error type, and nothing SHALL be sent. No SDK SHALL check the rule itself.
Every SDK SHALL wait for a result for the core's bound, `spawn.timeout` after it, and SHALL carry no
wait of its own.

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

- **WHEN** a Python, Ruby, Node, PHP or Go tasklet issues `ctx.spawn_steps("place", "o-1")` and `place`'s entry channel is declared with the `text` codec
- **THEN** the spawned instance's tasklet SHALL receive the string `o-1`, and a payload that is a dict, hash, object or array SHALL arrive as its JSON

#### Scenario: if_running without an id is refused at the call

- **WHEN** a Python tasklet calls `ctx.spawn_steps("worker", p, if_running="error")` with no id
- **THEN** the call SHALL raise the SDK's error with code `spawn.if_running_without_id`, and no request SHALL leave the runner

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

Whether an identity was already resolved SHALL be judged from the live registry and, for an instance that is no longer live, from its kept completion record or the relay history's lookup (`seen`, `keep-a-relay-history`). A generated id is judged by `{:relay, id}`. A named id whose input log no longer exists is judged by `{:spawn_identity, identity}`. The judgement SHALL hold while the instance is live or kept, and for the relay history's dedup window after it ended. A lookup answered `{:unknown, nodes}` SHALL refuse the request as retryable (`relay.history_unreachable`, naming those controllers) and start nothing. A lookup answered `:disabled` SHALL be judged from the registry and the kept record alone, and the controller SHALL log once that ended instances are not deduplicated.

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

#### Scenario: A crash re-run after the worker finished and kept nothing

- **WHEN** a tasklet spawns `worker` with no id while taking offset 7, `worker-<suffix>` ends `ok` and keeps nothing, and the re-run of offset 7 issues the identical request within the history's dedup window
- **THEN** the request SHALL resolve to `worker-<suffix>` with outcome `spawned` and no instance SHALL start

#### Scenario: The history cannot answer

- **WHEN** an identity's lookup misses locally and one controller does not answer it
- **THEN** the request SHALL be refused as `relay.history_unreachable` naming that controller, and no instance SHALL start

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
`tryResult()`; Go `Wait()` and `TryResult()`. Node's `spawnSteps` SHALL NOT be `async`: it SHALL
return the handle synchronously, and its `wait()` SHALL return a promise. A read that never waits SHALL answer that
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

`docs/public/topics/core/workflows.md` and every SDK page that shows the spawn call SHALL state, next
to the call, that a spawn without an id always starts a new instance and can never deliver into a
running one; that a caller who reissues a spawn without an id after a lost or timed-out answer starts
a second instance; that a spawn without an id skipped on replay answers an empty id; and that a caller
who needs at most one instance, or the id on replay, passes its own id.

#### Scenario: The contract page states the dangers

- **WHEN** a reader opens `docs/public/topics/core/workflows.md`
- **THEN** it SHALL state each of the three dangers and that passing an id avoids them

#### Scenario: Every SDK page carries the same warning

- **WHEN** a reader opens any SDK page that shows `spawn_steps` or `spawnSteps`
- **THEN** the page SHALL carry the same three dangers beside the example

### Requirement: A spawn request SHALL name the relay that asked for it

`SpawnStepsRequest` SHALL carry `parent_relay_id`, the instance whose tasklet invocation made
the request, set by the runtime from the invocation it is serving. The controller SHALL accept
it only when the requesting session hosts that instance in the request's namespace, and SHALL
then record it as the spawned instance's origin (`kind: spawn`, `parent_relay`). An empty or
unverifiable value SHALL leave the origin without a parent and SHALL NOT refuse the spawn. A
relay-to-relay call that creates its callee SHALL record the calling relay as the origin's
parent (`kind: call`).

#### Scenario: A verified parent is recorded

- **WHEN** a tasklet of relay `ma_fan` hosted by the requesting runner spawns `fan_trade`
- **THEN** the spawned instance's origin SHALL name `parent_relay: "ma_fan"`

#### Scenario: A parent the session does not host is not trusted

- **WHEN** a spawn request names a `parent_relay_id` the requesting session does not host
- **THEN** the spawn SHALL proceed and its origin SHALL carry no `parent_relay`

#### Scenario: An older runner sends no parent

- **WHEN** a runner built against an older runtime sends a request without `parent_relay_id`
- **THEN** the spawn SHALL proceed as before and its origin SHALL carry no `parent_relay`

### Requirement: The controller SHALL maintain a durable registry of cron schedules for steps spawns

`flz schedule add <name> --cron "<expr>" --tz <iana> --factory <factory_id> [--namespace ns] [--init-values json] [--text payload] [--channel ch] [--plugins json]` SHALL create a durable schedule entry; `flz schedule list` SHALL show the entries of every controller (with the node holding each definition, next fire time, the node running each schedule as `fires_on`, the last fired instance and its result) and `flz schedule remove <name>` SHALL delete them, on whichever controller holds the definition. A schedule name SHALL be unique across the cluster: `add` SHALL refuse a name any controller holds, naming that controller. The listed next fire SHALL be the slot the schedule's running job is armed for; a job that has gone dormant SHALL be listed with no next fire and as dormant, and only an entry no job answers for SHALL have its next fire computed from its cron. Entries SHALL persist across controller restarts and warm restarts. Only a steps factory SHALL be schedulable, and the kind is the factory's own, announced by its runner: `add` SHALL take no `--kind`, SHALL reject a factory the namespace has established as a flow relay, and SHALL accept a factory no runner has yet announced; every fire SHALL resolve the factory's established kind through the standard start path, where a flow factory fails the slot as any refused start does. Cron syntax, timezone, and the plugin spec SHALL be validated at `add`. `--init-values` SHALL be the initial state per tasklet the fired instance starts with, as it is for `flz relay start`; `--text` SHALL be the first message every fired instance receives, on the entry channel `--channel` names or on the first declared one, stored verbatim; an entry stored without `--text` SHALL fire an instance that waits for its first message. `--channel` without `--text` SHALL be refused at `add`. The plugin spec SHALL use the same JSON array `flz relay start --plugins` accepts and SHALL be stored verbatim; an entry stored without one SHALL fire with no plugins.

#### Scenario: Schedule survives restart

- **WHEN** a schedule is added and every controller restarts
- **THEN** `flz schedule list` SHALL still show it and subsequent slots SHALL fire

#### Scenario: A dormant schedule lists no next fire

- **WHEN** a schedule's job has gone dormant, for example because its cron has no future slot
- **THEN** `flz schedule list` SHALL show no next fire time for it and its JSON form SHALL carry `next_fire: null` and `dormant?: true`

#### Scenario: Invalid entry rejected

- **WHEN** `schedule add` is issued with a malformed cron expression or unknown timezone
- **THEN** the command SHALL fail with a message naming the invalid field and no entry SHALL be stored

#### Scenario: A flow factory is rejected at add

- **WHEN** `schedule add` names a factory a connected runner has announced as a flow relay
- **THEN** the command SHALL fail stating that only a steps factory is schedulable, naming the factory, and no entry SHALL be stored

#### Scenario: A factory that changed kind fails its slot, not the schedule

- **GIVEN** a schedule added for a factory announced as a steps relay
- **WHEN** at a later fire the only runner announcing that id has established it as a flow relay
- **THEN** the slot SHALL fail with a result naming the kind, SHALL be retried and listed as any failed slot is, and the entry SHALL remain

#### Scenario: A first message is stored with the entry

- **WHEN** `flz schedule add nightly --cron "0 2 * * *" --tz UTC --factory report --text '{"day":"today"}' --channel requests` is issued
- **THEN** the stored entry SHALL hold the text and the channel verbatim, and `flz schedule list` SHALL show the entry

#### Scenario: Unusable plugin spec rejected at add time

- **WHEN** `schedule add --plugins` is issued with JSON that is not an array, names an unknown provider, or carries a malformed hook
- **THEN** the command SHALL fail naming the fault and no entry SHALL be stored

#### Scenario: A failing schedule is visible in the list

- **WHEN** a schedule's most recent slot failed to spawn and is being retried
- **THEN** `flz schedule list` SHALL show that slot's instance id as the last fire with a non-ok last result, without the user reading logs

#### Scenario: A schedule whose job does not answer still lists

- **WHEN** a schedule's job process does not answer the status query within its bounded timeout
- **THEN** `flz schedule list` SHALL still render the entry (placement known, last fire unknown) rather than failing the command

#### Scenario: A schedule added on another controller is listed

- **GIVEN** a schedule added through controller B
- **WHEN** a user connected to controller A runs `flz schedule list`
- **THEN** the schedule SHALL be listed with B as the node holding it

### Requirement: Each slot SHALL fire exactly once cluster-wide via the standard steps spawn path

At each matching slot (evaluated in the entry's IANA timezone by a single schedule process that exists on exactly one node cluster-wide, hosted in the dedicated `:flz_schedules` hub), the controller SHALL spawn a fresh steps instance with deterministic id `<name>@<slot>`, the stored `init_values` as its initial state and the stored first message, when one is stored, delivered as the instance's first input on the mechanism a tasklet's spawn uses, through the same start path as `flz relay start` (quota, plugins, teardown, retention inherited). The fire SHALL succeed whenever any controller node in the cluster holds a runner advertising the entry's factory in the entry's namespace: the node hosting the schedule process SHALL NOT affect the outcome, and the spawn SHALL be routed to a capable node at fire time against the fleet state current at that moment. A fire SHALL be refused as unsatisfiable only when no node in the cluster holds such a runner, and the refusal SHALL say so cluster-wide (not "no runners" on the deciding node alone). The entry's stored plugin spec SHALL be decoded at each firing and attached at spawn, with the fire's namespace and instance id stamped on every plugin and each plugin's id prefixed by the instance id so concurrent slots never share one. A firing whose plugin spec cannot be decoded SHALL NOT spawn an instance and SHALL be logged and reported on telemetry as a spawn failure. A duplicate fire attempt for the same slot SHALL no-op on the deterministic id. DST: nonexistent local times skip; ambiguous local times fire once.

#### Scenario: The runner lives on a different node than the schedule process

- **WHEN** a slot fires on a controller node holding no runner for the entry's namespace while another cluster node holds a connected runner advertising the factory
- **THEN** the instance SHALL be started on the capable node and the fire SHALL be reported as fired, not failed

#### Scenario: A rolling update moves the runner between nodes

- **WHEN** the namespace's only runner disconnects from node A and reconnects on node B between two slots, with the schedule process staying where it is
- **THEN** the next slot SHALL fire successfully on node B with no user action

#### Scenario: A fired instance runs on the stored first message

- **WHEN** a schedule storing `--text '{"day":"today"}'` fires a `once` steps relay
- **THEN** the spawned instance SHALL run its pass over that message on the stored channel or the first declared entry channel, and SHALL end with status `ok`

#### Scenario: A fired instance produces output through its scheduled plugins

- **WHEN** a schedule carrying a producer plugin spec fires
- **THEN** the spawned instance SHALL have that plugin attached from the start of its run, with `relay_id` equal to the slot's instance id

#### Scenario: A spec that no longer decodes does not fire a plugin-less run

- **WHEN** a slot fires while the spec's provider is no longer enabled
- **THEN** no instance SHALL be started and the failure SHALL be logged and emitted on the spawn-failure telemetry event

#### Scenario: One instance per slot despite duplicate fire attempts

- **WHEN** two fire attempts for the same slot race while the first instance is alive
- **THEN** exactly one instance `<name>@<slot>` SHALL exist and the losing attempt SHALL no-op

#### Scenario: Exactly one node runs a given schedule

- **WHEN** several controllers each hold the same schedule definition in their node-local store and all assert it
- **THEN** exactly one schedule process SHALL exist cluster-wide and the losing assertions SHALL no-op

#### Scenario: The owning node leaves the cluster

- **WHEN** the node running a schedule leaves the cluster
- **THEN** a surviving node SHALL take the schedule over, subsequent slots SHALL fire there, and no duplicate schedule process SHALL exist

#### Scenario: Manual immediate run

- **WHEN** `flz schedule run <name>` is issued
- **THEN** one instance SHALL spawn immediately with the stored configuration, its stored first message included, and an id distinct from cron slots, whether or not the schedule hub is running on that node — routed to a capable node exactly like a cron fire

### Requirement: Missed slots SHALL be skipped, and overlapping runs SHALL be permitted by distinct slot ids

Slots that pass while no schedule process is running SHALL NOT be back-filled, except that on process start the single most recent slot inside `misfire_grace_ms` SHALL fire — covering controller restart, migration and node-takeover gaps — floored at the entry's creation time so a newly added schedule never fires history. Older slots SHALL be dropped and the skip SHALL be logged. A grace of `0` SHALL disable the look-back entirely. If a previous slot's instance is still running at the next slot, the new slot SHALL still spawn (ids differ); bounding runtime is the author's responsibility (`max_duration`).

#### Scenario: Down cluster skips cleanly

- **WHEN** the cluster is down across two slots older than `misfire_grace_ms` and then recovers
- **THEN** no catch-up spawns SHALL occur for those slots and the skip SHALL be logged

#### Scenario: A restart inside the grace window resumes the latest slot

- **WHEN** a schedule process restarts and exactly one slot passed inside `misfire_grace_ms`
- **THEN** that slot SHALL fire once and no earlier slot SHALL be back-filled

#### Scenario: Long-running previous slot does not block

- **WHEN** `<name>@<t1>` is still running at slot `t2`
- **THEN** `<name>@<t2>` SHALL spawn normally

### Requirement: A failed slot SHALL be retried with backoff until its next slot supersedes it

A slot whose spawn fails SHALL be retried by the schedule process with exponential backoff (capped at one minute, the finest cron granularity) using the SAME deterministic instance id `<name>@<slot>`, so a retry racing any other fire attempt no-ops on the registry's duplicate-id refusal. A `duplicate` answer SHALL count as success. When the next slot fires, any pending retry for an earlier slot SHALL be cancelled — at most one slot is ever in flight, extending the misfire-grace rule ("only the most recent slot") from process start to steady state. The schedule SHALL NOT park, go dormant, or require user intervention because of spawn failures; a schedule for a factory whose runner has not connected yet SHALL simply keep probing. A schedule process restarting or migrating mid-retry SHALL recover through the existing misfire-grace look-back, which re-fires the most recent slot inside the grace window. Each failed attempt SHALL be reported on the existing spawn-failure telemetry event carrying the attempt number.

#### Scenario: A runner appearing between slots is picked up by the retry

- **WHEN** a slot fails because no cluster node holds a capable runner, and such a runner connects before the next slot
- **THEN** a retry SHALL spawn that slot's instance under its original id, and the schedule's last result SHALL report the fire as successful

#### Scenario: The next slot supersedes a still-failing retry

- **WHEN** a slot is still being retried when the schedule's next slot fires
- **THEN** the pending retry SHALL be cancelled, only the new slot SHALL be attempted, and no back-fill for the abandoned slot SHALL ever occur

#### Scenario: A retry racing a duplicate fire no-ops

- **WHEN** a retry fires for a slot whose instance was concurrently started by another fire attempt
- **THEN** the retry SHALL treat the duplicate-id answer as success and stop retrying

#### Scenario: Spawn failures never park the schedule

- **WHEN** every slot of a schedule fails for days because its factory's runner is never deployed
- **THEN** the schedule SHALL remain registered and probing on backoff, `flz schedule list` SHALL show the failing last result, and no user action SHALL be needed for it to fire once the runner appears

### Requirement: A manual fire SHALL be an authorized relay start

`flz schedule run <name>` SHALL authorize `relay.start` in the namespace the
schedule was added under before it spawns an instance, and SHALL accept the
caller's credential the way every other gated schedule verb does. It SHALL stay
hub-free: the authorization is judged from the stored row, so a manual fire still
works on a node whose schedule hub is down.

#### Scenario: A manual fire carries a credential

- **WHEN** `flz schedule run <name> --token <t>` is issued
- **THEN** the token SHALL reach the controller with the request, and the spawn SHALL
  be judged as `relay.start` for that token in the schedule's namespace

#### Scenario: A manual fire with no credential is refused under enforcement

- **WHEN** authorization is enforced and `schedule run` is issued with no credential
- **THEN** no instance SHALL spawn and the refusal SHALL name the action

### Requirement: The schedule verbs SHALL carry their optional flags as one options document

A schedule verb's optional flags SHALL travel to the controller as one options
document (the namespace, the initial values, the first message's text and channel, the
plugin spec and the caller's credential), the keyword list every other gated schedule verb already
carries its credential in, rather than as positional arguments with defaults.
There SHALL be no `kind` entry in that document, because a schedule names a factory and
the factory's runner names its kind. This rule governs the request the CLI builds, not the
spelling of the user-facing flags. A value that must reach the controller as text, such as
the plugin spec the decoder reads verbatim and the first message, SHALL be carried as text whatever it
parses as.

#### Scenario: An optional flag is absent

- **WHEN** `flz schedule add` is issued without `--namespace`, `--init-values`,
  `--text`, `--channel` or `--plugins`
- **THEN** the controller SHALL apply the same defaults it applies today, and the
  request SHALL carry no positional placeholder for the absent flag

#### Scenario: The plugin spec arrives as the text the user wrote

- **WHEN** `flz schedule add --plugins '[{"provider": …}]'` is issued
- **THEN** the controller SHALL receive the JSON text, and the stored entry SHALL
  hold it verbatim as the registry requirement demands

#### Scenario: The first message arrives as the text the user wrote

- **WHEN** `flz schedule add --text 'hello' --channel requests` is issued
- **THEN** the request SHALL carry `text` and `channel` in the options document as text, and the stored entry SHALL hold both verbatim

#### Scenario: No schedule verb exceeds the arity ceiling

- **WHEN** the lint gate reads `lib/flezha/cli/schedule.ex`
- **THEN** no function SHALL take more parameters than the ceiling the gate enforces

### Requirement: A scheduled instance SHALL record which schedule started it

An instance a schedule slot spawns SHALL carry the origin `%{kind: "schedule", schedule: <name>,
slot: <slot>}`. An instance a manual fire spawns SHALL carry `%{kind: "schedule", schedule:
<name>, slot: "manual", principal: ..., source: ...}`, taken from the caller context the manual
fire was authorized with.

#### Scenario: A slot names its schedule

- **WHEN** schedule `analyzers` fires its 10:15 slot
- **THEN** the spawned instance's origin SHALL be `%{kind: "schedule", schedule: "analyzers",
  slot: "2026-10-28T10:15:00Z"}`

#### Scenario: A manual fire names its caller

- **WHEN** principal `alice` runs `flz schedule run analyzers`
- **THEN** the spawned instance's origin SHALL name `slot: "manual"` and `principal: "alice"`
