# procedure-requested-spawn Specification

## Purpose

The procedure-requested-spawn capability lets a running procedure ask the cluster for a workflow operative, closing the gap that made a workflow instance startable only from outside the cluster (`flz operative start`, `flz schedule`) and forced every pipeline needing a worker to bolt an external dispatcher onto the controller. One SDK call — `ctx.spawn_workflow` (Rust) / `ctx.spawnWorkflow` (Java) — spawns-or-delivers on a single `(namespace, instance_id)` in the requester's own namespace: a free id starts the factory with the payload as its first input, a taken id receives the payload as one more input or refuses. Concurrent requests race-resolve at the operative registry to exactly one instance. The call is fire-and-forget with an async result: quota, transport, and controller failures resolve the caller's handle and never fault the requesting procedure, its emit path, or its journaling. Spawning is a side effect of live processing only, so a replay never doubles the workers a run created. Everything else — quota, the durable inbox, exit triggers, `WorkflowCompleted` teardown, retention, `flz operative list` — is the existing workflow lifecycle, inherited unchanged.
## Requirements
### Requirement: A procedure SHALL be able to request spawn-or-deliver of a workflow operative

Every SDK SHALL expose a context call `spawn_workflow(instance_id, factory_id, payload, if_running)`
usable from inside any procedure: Rust, Java, Node, Python, Ruby and PHP through the shared
core, and Elixir in its own core. With `if_running: deliver` (default), the controller SHALL
spawn a workflow instance of `factory_id` under `instance_id` if absent, or deliver `payload` to
the existing instance's inbox if present. With `if_running: error`, an existing instance SHALL
yield an `already_running` result to the requester. The spawned instance SHALL receive `payload`
as its first inbox frame and SHALL follow all existing workflow lifecycle semantics (quota, exit
triggers, `WorkflowCompleted`, teardown, retention).

The request SHALL NOT carry a namespace: the controller resolves it against the control channel the request arrived on, so a request cannot reach another namespace.

Factory resolution SHALL be cluster-wide: the controller SHALL route the spawn to a runner registering `factory_id` connected to ANY controller node in the cluster, not only runners connected to the node that received the request. `factory_unknown` SHALL be yielded only when no connected runner on any cluster node registers the factory, resolved against the fleet state current at request time.

#### Scenario: First request spawns, second delivers

- **WHEN** two procedures issue `spawn_workflow("w1", "worker", p1/p2, deliver)` in sequence
- **THEN** exactly one workflow instance `w1` SHALL exist and its inbox SHALL have received `p1` then `p2`

#### Scenario: error policy reports instead of delivering

- **WHEN** `spawn_workflow("w1", "worker", p, error)` is issued while `w1` runs
- **THEN** no delivery SHALL occur and the requester's async result SHALL be `already_running`

#### Scenario: Factory on a different runner than the requester

- **WHEN** a procedure on the streaming runner requests spawn of a factory registered only by a separate analyzer runner (each runner holds its own sealed operative registry)
- **THEN** the controller SHALL route the spawn to a runner that has registered `factory_id`, and SHALL yield a `factory_unknown` async result if no connected runner registers it

#### Scenario: Factory on a runner connected to a different controller node

- **WHEN** a procedure whose runner is connected to controller node A requests spawn of a factory whose only runner is connected to controller node B
- **THEN** the controller SHALL start the instance on node B and the requester's async result SHALL be `spawned` (or `delivered` if the id already exists), not `factory_unknown`

#### Scenario: Every SDK exposes the call with one meaning

- **WHEN** the capability ships in an SDK
- **THEN** that SDK SHALL expose the call with the semantics above, and its own suite SHALL cover the outcomes, the error policy, the refusal, the timeout, the replay skip and the no-control-plane case

### Requirement: Spawn requests SHALL be idempotent under concurrency on (namespace, instance_id)

Concurrent `deliver`-mode requests for the same `(namespace, instance_id)` SHALL resolve to exactly one spawned instance, with every other request's payload delivered to it. A completed (torn-down) instance id SHALL count as absent for subsequent requests.

#### Scenario: Concurrent burst yields one instance

- **WHEN** N procedures across different runners request `spawn_workflow("w1", "worker", pi, deliver)` concurrently
- **THEN** exactly one `w1` SHALL spawn and all N payloads SHALL reach its inbox exactly once each

#### Scenario: Id reuse after completion

- **WHEN** `w1` completes teardown and a new request for `w1` arrives
- **THEN** a fresh instance SHALL spawn (no delivery to the finished run)

### Requirement: Spawn failures SHALL be isolated from the requesting procedure

The call SHALL return immediately with a local async result handle. Transport, quota, or controller failures SHALL surface only via that handle (or one warning log if unobserved) and SHALL NOT fault the requesting procedure's `run`, block its emit path, or affect its journaling/replay.

#### Scenario: Controller unreachable

- **WHEN** a procedure calls `spawn_workflow` while the control plane is down
- **THEN** the procedure's `run` SHALL complete normally and the failure SHALL appear only on the async result / warning log

#### Scenario: Replay does not re-issue side effects unexpectedly

- **WHEN** a procedure that called `spawn_workflow` is replayed from the WAL
- **THEN** the replay semantics SHALL be documented and deterministic: spawn requests are side effects executed on live processing only, not during replay

### Requirement: A spawn request SHALL be judged as the act it performs

The controller SHALL judge each act a spawn request performs as the identity the request's connection proved, at the same policy point as the operator-issued equivalent and in the connection's namespace: a start as `operative.start` through the `operative_start` row, and a delivery to a running instance as `operative.send`, the right the `operative_send` row judges. A permitted start SHALL write the `operative_start` row's audit record naming the connection's principal and the SDK as the surface. A permitted delivery SHALL write no record of its own, because a delivery happens once per event. A refused act SHALL start nothing and deliver nothing, its denial SHALL be audited, and the requester's result SHALL be `refused` with the refusal as its detail.

The judgment SHALL be made on the controller node that received the request, and no start and no delivery SHALL be forwarded to another node or performed before it. While authorization is disabled every act SHALL be permitted, and a start SHALL still write its record, as every other start does.

#### Scenario: A spawn by an identity that may not start is refused

- **WHEN** authorization is enforced and a procedure on a runner whose
  `runner@<namespace>` principal has no role binding requests
  `spawn_workflow("w1", "worker", p, deliver)` while `w1` is free
- **THEN** no instance SHALL start, the requester's result SHALL be `refused`
  naming the refusal, and the denial SHALL be audited

#### Scenario: A delivery by an identity that may not send is refused

- **WHEN** authorization is enforced and the same unbound runner's request names
  `w1` while it runs
- **THEN** the payload SHALL NOT be delivered, the requester's result SHALL be
  `refused`, and the denial SHALL be audited

#### Scenario: A permitted spawn is recorded like an operator's start

- **WHEN** `runner@team-orders` is bound to `spawner` on `team-orders` and a
  procedure on that connection spawns `w1`
- **THEN** `w1` SHALL start, and a `start` record on `operative_service` SHALL name
  `w1`, the namespace `team-orders`, the principal `runner@team-orders` and the
  surface `sdk`

#### Scenario: A permitted delivery is judged and writes no record

- **WHEN** the same bound runner's request names `w1` while it runs
- **THEN** the payload SHALL be delivered, and no `send` record SHALL be written
  on `operative_service`

#### Scenario: A start another node would run is judged where it was asked

- **WHEN** the only runner registering the factory is connected to another
  controller node and the requesting identity may not start an operative
- **THEN** the request SHALL be refused on the node that received it, and no start
  SHALL be forwarded

#### Scenario: Authorization disabled

- **WHEN** authorization is disabled and a runner whose principal has no role
  binding requests a spawn of a free id
- **THEN** the instance SHALL start and its `start` record SHALL be written

