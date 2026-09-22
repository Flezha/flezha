# operative-model Specification

## Purpose

The operative-model capability is the canonical vocabulary and kind model for Flezha's compute workloads. It defines the **Operative** umbrella (the generic top-level unit of work a Runner runs at a Controller's direction), its two kinds today, **Stream** (runs-forever) and **Workflow** (runs-to-completion) — and the per-Operative concepts of **Tasklet** (a typed stage) and **Cell** (the per-key, state-bearing slice). It mandates the no-synonyms rule (no `relay`, `shard`, `partition`, or `task` survives in any shipped surface). What an instance does is decided by its class's settings, never by its kind; the class's persistence switch gates per-Cell state introspection. The unified `flz operative` CLI verb tree manages every kind's lifecycle.
## Requirements

<!-- kind-naming-record:begin -->

### Requirement: Canonical operative vocabulary

The system SHALL use exactly these terms for compute workloads, and code, modules, protobuf, wire payload-ids, CLI, config keys, env vars, documentation, and on-disk path components SHALL NOT introduce or retain the synonyms `relay`, `shard`, `partition`, `task`, `continuous`, or — as the name of an operative kind — `bounded` or `unbounded`:

- **Operative** — the generic top-level unit of work that a Runner runs at a Controller's direction; the thing operators start and stop. Every Operative has a `kind`.
- **Stream Operative** — the operative kind that runs with no inherent end, folding an ongoing stream of input until it is stopped. Formerly called "Unbounded Operative", and before that "Relay".
- **Workflow Operative** — the operative kind that runs an ordered, journaled workflow to a terminal state, with saga compensation on abort. It may iterate — once, per inbox item, or chained — and may park between iterations. Formerly called "Bounded Operative", and before that "Task".
- **Tasklet** — a typed stage within an Operative that processes inputs from a Channel. Formerly "Shard".
- **Cell** — the per-key, state-bearing slice of a Tasklet; durable state lives at the Cell level. Formerly "Partition".
- **Controller**, **Runner**, and **Channel** are unchanged. "Operator" remains reserved for ProcessHub-internal ring-node jargon and is never used for an Operative.

The prohibition on `bounded` and `unbounded` applies to the **kind sense only**. Both words remain correct English for describing a run — a workflow operative runs a *bounded* iteration; a stream operative folds an *unbounded* input — and remain correct for unrelated capacity concepts such as a bounded mailbox. See the `operative-kind-naming` capability for the sense-disambiguation rule.

#### Scenario: Documentation and glossary use operative vocabulary

- **WHEN** any spec, doc under `docs/`, `docs/glossary.md`, or `CLAUDE.md` references the top-level unit of work, its stage, or its per-key slice
- **THEN** it SHALL use "Operative" (with kind "Stream" or "Workflow"), "Tasklet", and "Cell" respectively, and SHALL NOT use "Relay", "Shard", "Partition", "Task", "Continuous", or "Bounded"/"Unbounded" as a kind name

#### Scenario: No synonym survives in shipped surfaces

- **WHEN** the controller, SDKs, protobuf, CLI, config, and env-var surfaces are inspected after this change
- **THEN** no identifier, message name, field name, wire payload-id string, CLI verb, config key, or env var SHALL contain `relay`, `shard`, `partition`, `task`, `continuous`, or a kind-sense `bounded`/`unbounded` as the name of a workload concept
<!-- kind-naming-record:end -->

<!-- kind-naming-record:begin -->

### Requirement: An operative declares a kind

Every Operative SHALL carry a `kind` atom. The kind SHALL be the kind its class declares, read from the factory announcement the runner shipped for the factory id the start names, and SHALL be frozen for the operative's lifetime — it cannot change without a stop + start cycle. No start path (`flz operative start`, a schedule fire, a tasklet-requested spawn, the service verb) SHALL take a kind from its caller; a caller that asserts one (a spawn or a schedule, which are workflow-only) SHALL be judged against the announced kind and refused by name when they disagree. The recognized kinds are `:stream` and `:workflow`; a factory whose announced kind is any other literal, or empty, SHALL NOT be placeable, and a start naming it SHALL fail with a clear error naming the factory and the missing or unknown kind, and no operative process SHALL be created. On the wire from controller to runner, the accepted set of `kind` string literals on `AddOperative` SHALL be exactly `""` (empty ⇒ the `:stream` default), `"stream"`, and `"workflow"`; the SDK SHALL reject any other literal with an error naming the unknown kind. The former literals `"unbounded"` and `"bounded"` SHALL NOT be accepted.

#### Scenario: The kind is the class's

- **WHEN** a runner announces factory `prices` as a stream and `paper_order` as a workflow, and an operator starts one of each naming only the factory
- **THEN** the first instance's kind SHALL be `:stream` and the second's `:workflow`

#### Scenario: Workflow is a recognized kind

- **WHEN** an operative is started for a factory announced with kind `:workflow`
- **THEN** the start SHALL be accepted and the operative's kind SHALL be `:workflow`

#### Scenario: Kind is frozen for the operative's lifetime

- **WHEN** an operative is running with a given kind
- **THEN** there SHALL be no tasklet that mutates the kind of a running operative; changing kind requires stopping and re-starting it

#### Scenario: Unknown kind is rejected at start

- **WHEN** an operative is started for a factory whose announcement carries a kind that is not recognized (any literal other than `"stream"` or `"workflow"`, or none)
- **THEN** the start SHALL fail with an error that names the factory and the unknown or missing kind, and no operative process SHALL be created

#### Scenario: A retired kind literal is rejected

- **WHEN** an operative is started with the wire literal `"unbounded"` or `"bounded"`
- **THEN** the start SHALL fail with an error naming the unknown kind, exactly as for any other unrecognized literal, and no compatibility mapping to `:stream` or `:workflow` SHALL be applied
<!-- kind-naming-record:end -->

<!-- kind-naming-record:begin -->

### Requirement: The capability model is extensible without affecting existing kinds

Introducing a new operative kind SHALL be additive: it SHALL require registering the new kind
and nothing else. Adding a third (or later) kind SHALL NOT force edits to the existing
`:stream` or `:workflow` kinds, to user-authored computation code, or to the routing boundary
beyond registering the new kind. Kind names SHALL be independent words rather than variations
on a shared pattern, so that a third kind joins as a peer without renaming the existing two.
There SHALL be no per-kind capability table: what an instance can do SHALL follow from what
its class declares (its persistence switch, its sleep setting, the tier) and from its own state
(sleeping, bound, journaling), never from a flag attached to its kind.

#### Scenario: A future kind is additive

- **WHEN** a new operative kind is registered
- **THEN** the existing kinds SHALL be unaffected, and the new kind SHALL be selectable at start without changing the existing kinds' behavior

#### Scenario: A third kind requires no rename of the existing two

- **WHEN** a third operative kind is introduced
- **THEN** `:stream` and `:workflow` SHALL keep their names, and no shared naming pattern SHALL have to be extended or abandoned to accommodate it

#### Scenario: No gate reads a kind's capability

- **WHEN** the controller's time-travel query, retention sweep, dynamic-cell delete, output-stall watch and input-silence sweep are inspected
- **THEN** none SHALL consult a kind-to-capability map, and `Flezha.Operative.Kind` and `ClassRegistry.capable?/2` SHALL NOT exist

### Requirement: Unified `flz operative` CLI verb

The CLI SHALL expose a single `flz operative` verb tree for lifecycle management of every operative kind: `flz operative start <factory> [--id|-i <id>]`, `flz operative stop`, `flz operative list`, and `flz operative info`. The kind SHALL be the one the factory's runner announced; `flz operative start` SHALL carry no `--kind` option, and a written `--kind` SHALL be refused as an unknown option, never read and never ignored. The factory SHALL be the start verb's only positional argument, and the id SHALL be an option: a start with no `--id` SHALL be given a generated id. A successful start SHALL print `Started <id>`, naming the id it started under whether the operator wrote it or the controller generated it, and SHALL print no progress line before it. With `--output json` (`-o json`) or `--json` it SHALL print one JSON object `{"id": <id>, "outcome": <outcome>}` and nothing else, the way `flz operative list` and `flz operative info` render JSON. There SHALL be no `flz relay` verb, and `--kind unbounded` / `--kind bounded` SHALL NOT be accepted, as no `--kind` is.

#### Scenario: Start a workflow operative

- **WHEN** the operator runs `flz operative start <factory>` for a factory its runner announced as a workflow
- **THEN** a workflow operative SHALL be started

#### Scenario: A start without an id prints the generated id

- **WHEN** the operator runs `flz operative start echo` and the start succeeds
- **THEN** the command SHALL exit 0 and print `Started <id>`, where `<id>` is the generated id `flz operative list` shows and `flz operative stop` accepts

#### Scenario: A start with an id uses it

- **WHEN** the operator runs `flz operative start echo -i my_echo` and the start succeeds
- **THEN** an operative with id `my_echo` SHALL run and the command SHALL print `Started my_echo`, identically to `--id my_echo`

#### Scenario: JSON output carries the id and the outcome

- **WHEN** the operator runs `flz operative start echo -o json` and the start succeeds
- **THEN** standard output SHALL be exactly one JSON object with `id` set to the started id and `outcome` set to `started`, and no other line

#### Scenario: A retired kind argument is rejected

- **WHEN** the operator runs `flz operative start <factory> --kind workflow` or `flz operative start <factory> --kind bounded`
- **THEN** the CLI SHALL reject `--kind` as an unknown option and SHALL NOT start an operative

#### Scenario: The legacy verb is gone

- **WHEN** the operator runs `flz relay start`
- **THEN** the CLI SHALL reject it as an unknown command
<!-- kind-naming-record:end -->

### Requirement: Operative start requests SHALL be location-transparent across controller nodes

A workflow operative start request (`flz operative start`/`add`, a schedule fire, or a tasklet-requested spawn) submitted on any controller node SHALL succeed whenever any node in the cluster holds a connected runner advertising the requested factory in the requested namespace. When the receiving node cannot satisfy the request locally, the controller SHALL resolve the set of capable nodes against fleet state current at request time and execute the same start call on one capable node, chosen deterministically, forwarded at most once. A request that carries no id SHALL be given its generated id on the receiving node before capable nodes are resolved, so the forwarded call carries that id and the executing node generates none of its own. A routed request SHALL be validated on the executing node (namespace, factory, quota, duplicate id) exactly as a local request, and its verdict SHALL be returned to the origin unchanged. When no cluster node is capable, the refusal SHALL state that no runner exists on any cluster node: a node-local "no runners" answer SHALL NOT be reported for a cluster that holds one. Resolution of capable peers SHALL be bounded in time; an unreachable or unresponsive peer SHALL be treated as not capable rather than stalling the request. The local fast path SHALL be unchanged: a node that can satisfy the request itself SHALL perform no cluster round.

#### Scenario: CLI start against a runner-less controller node

- **WHEN** `flz operative start` is issued against a controller node holding no runner for the target namespace while a peer node holds a connected runner advertising the factory
- **THEN** the operative SHALL be started on the capable peer and the command SHALL report success naming the started id

#### Scenario: A routed start without an id keeps the origin's id

- **WHEN** a start with no id is issued against a controller node holding no runner for the target namespace while a peer node holds a capable runner
- **THEN** the operative SHALL run on the peer under the id the receiving node generated, and that id SHALL be the one reported to the caller

#### Scenario: Honest cluster-wide refusal

- **WHEN** a start request names a factory no connected runner on any cluster node advertises
- **THEN** the refusal SHALL state that no runner in the namespace advertises the factory on any cluster node

#### Scenario: A routed request is not forwarded twice

- **WHEN** a start request is routed to a capable node and that node's runner disconnects before validation completes
- **THEN** the request SHALL fail with that node's verdict rather than being forwarded again

#### Scenario: An unreachable peer cannot stall a start

- **WHEN** a start request is submitted while one cluster peer is unreachable
- **THEN** capable-node resolution SHALL complete within its bounded timeout, treating the unreachable peer as not capable

### Requirement: Operative commands SHALL be location-transparent across controller nodes

Every controller node SHALL be a valid entry point for every namespace-scoped operative command — sending input, requesting a stop, reading live cell state, reading overwatch info, creating a snapshot, resolving an operative's pid or namespace, and re-injecting a dead-letter payload. When the receiving node is not a member of the target namespace's hub, the controller SHALL forward the command once to a hub-member node resolved against membership current at request time, and SHALL return that node's verdict unchanged. A forwarded command SHALL NOT be forwarded again. Commands with node-local side effects (plugin disable, state-directory deletion, quota release) SHALL execute those side effects on the node hosting the operative, never on the receiving node. When no cluster node holds the namespace, the existing local refusal SHALL stand. Namespace and operative enumeration SHALL include namespaces hosted only on peer nodes. The local fast path SHALL be unchanged: a node that is a member of the namespace's hub SHALL perform no forwarding.

#### Scenario: Sending input through a non-member controller node

- **WHEN** `flz operative send` is issued against a controller node that is not a member of the target namespace's hub while a peer node runs the operative
- **THEN** the payload SHALL be delivered to the operative and the confirming verdict SHALL be reported to the caller unchanged

#### Scenario: Stopping through a non-member node cleans up on the hosting node

- **WHEN** `flz operative stop` is issued against a non-member node
- **THEN** the operative SHALL be stopped and its plugin disable, state-directory deletion, and quota release SHALL run on the node that hosted it

#### Scenario: Live state read from any node

- **WHEN** `flz state get` is issued against a non-member node for a live cell on a peer node
- **THEN** the state JSON and its change seq SHALL be returned exactly as a member-node read would return them

#### Scenario: A forwarded command is not forwarded twice

- **WHEN** a command is forwarded to a hub-member node whose hub dies before the command executes
- **THEN** the command SHALL fail with that node's refusal rather than being forwarded again

#### Scenario: DLQ replay resolves the operative cluster-wide

- **WHEN** `flz dlq replay` is issued on the node holding the parked entry while the target operative runs on a peer node
- **THEN** the payload SHALL be re-injected into the operative's input stream on the peer node

### Requirement: The controller cluster SHALL maintain a shared directory of runner capability and hub placement

Each controller node SHALL advertise into a cluster-replicated directory which namespaces it holds active runners for, which factories those runners register, and which namespace hubs it is a member of. The directory SHALL be readable on every node without a network round per read. Advertisements SHALL follow the node-local fleet truth: a runner connecting, disconnecting, being evicted, or replacing its factory set SHALL be reflected, and a node leaving the cluster SHALL have its advertisements removed without operator action. Directory answers MAY lag the fleet truth; every routed request SHALL therefore be re-validated on the executing node, and a stale answer SHALL cost at most one failed forward. The directory SHALL NOT be used for recovery-time placement, which continues to ride hub membership.

#### Scenario: A runner's factories appear cluster-wide on connect

- **WHEN** a runner connects to controller node B advertising factory `f`
- **THEN** a directory read on controller node A SHALL name node B as capable of `f` in that namespace without A performing a cluster round

#### Scenario: A dead node's advertisements disappear

- **WHEN** a controller node leaves the cluster without cleanly disconnecting its runners
- **THEN** directory reads on surviving nodes SHALL stop naming that node

#### Scenario: A stale directory answer degrades to one failed forward

- **WHEN** a request is routed to a node whose last capable runner disconnected after the directory was read
- **THEN** the executing node SHALL refuse with its local verdict and no further forwarding SHALL occur

### Requirement: A duplicate start against a live winner SHALL be a successful no-op

`flz operative start` (and the manager call it wraps) losing the duplicate-id claim to a **live** instance SHALL report success: exit code 0, with output `<id> is already running, left untouched`, or with `--output json` / `--json` the object `{"id": <id>, "outcome": "already_running"}`. No comparison of factory, init values, or opts is performed and none SHALL be implied. Only a start that names its own id can reach this outcome: a generated id that loses the claim is drawn again rather than reported as already running. A claim refused with **no live winner** behind it (a stale registry row) SHALL remain a non-zero error naming the id, so a wedged id is never silently accepted. This is the CLI-facing rendering of the concession semantics `convergence-self-healing` already requires of every starter; the underlying `:duplicate` verdict and its live-winner verification are unchanged.

#### Scenario: Re-running a cold-start script is quiet

- **WHEN** `flz operative start f --id demo` is issued while a live instance already holds `demo`
- **THEN** the command exits 0, prints `demo is already running, left untouched`, and the running instance observes no stop, restart, or rebind

#### Scenario: JSON output names the outcome

- **WHEN** `flz operative start f --id demo -o json` is issued while a live instance already holds `demo`
- **THEN** the command exits 0 and prints exactly one JSON object with `id` set to `demo` and `outcome` set to `already_running`

#### Scenario: A stale registry row still fails loudly

- **WHEN** the id claim is refused but no live instance holds `demo`
- **THEN** the command exits non-zero with the id-in-use error, unchanged from today

### Requirement: `flz operative info` SHALL report the operative's phase and the loop's health verdict

`flz operative info` SHALL report each live operative's lifecycle phase as one of `running`, `suspended`, `unbound` or `parked`, and its health as the state of the one verdict record the convergence loop publishes for it, both read from that record — the phase is the record's own fact, read from the operative's state, and the health is the record's state, one of `healthy`, `degraded`, `failed`, `unbound`, `parked`, `suspended` or `unknown`. It SHALL report `parked` — never `running` — for an operative the Controller has parked, whatever the park reason, and `unbound` — never `running` and never `parked` — for an operative that holds no runner binding at all. Phase SHALL be derived in the same order as health: suspended first, then unbound, then parked, so the two words cannot describe one operative differently. `flz operative list`, `flz cluster status`, the published `Flezha.Service.Operative.operative_rows/2` and `/healthz` SHALL read the same record, so no two operator surfaces describe one operative in contradictory words. `info` SHALL NOT derive a phase or a health verdict of its own, and no surface SHALL keep a second vocabulary or a second rank order: the worst row of a listing is chosen by the record module's one rank. Both the text and the JSON rendering SHALL carry both fields. A finished workflow operative, surfaced from its retained completion record rather than from a live Overwatch, SHALL keep reporting phase `finished`.

#### Scenario: A parked operative is not reported as running

- **WHEN** the operator runs `flz operative info` for a namespace holding an operative the Controller has parked
- **THEN** the text and the JSON rendering SHALL both report its phase as `parked`, and neither SHALL report `running`

#### Scenario: An operative holding no binding is reported unbound, not parked

- **WHEN** the operator runs `flz operative info` for a declared operative whose bind found no runner, so it is held alive with no binding
- **THEN** the text and the JSON rendering SHALL both report its phase as `unbound` and its health as `unbound`, and neither SHALL report `running` or `parked`

#### Scenario: A park that named a runner is still parked

- **WHEN** an operative parked after a failed initialisation on a runner it had already resolved
- **THEN** its phase SHALL be reported `parked`, because it holds that runner's binding

#### Scenario: A suspended instance is still suspended

- **WHEN** a durable workflow instance evicted at a committed iteration boundary also carries a park reason
- **THEN** its phase SHALL be reported `suspended`, the phase the convergence loop classifies it under

#### Scenario: The health reported is the loop's own verdict

- **WHEN** the convergence loop has classified an operative `degraded` and `flz operative list` shows that word
- **THEN** `flz operative info` SHALL report `degraded` for the same operative

#### Scenario: An unobserved operative reports unknown health, and a phase all the same

- **WHEN** the convergence loop has not yet observed the operative
- **THEN** its health SHALL be reported `unknown` rather than invented, and its phase SHALL still be reported from the operative's own state

#### Scenario: Every surface reports one word for one operative

- **WHEN** an operative is parked, or its runner is dead, or it is declared and unbound
- **THEN** `flz operative info`, `flz operative list`, `operative_rows/2` and the health verdict event on the observability bus SHALL all report the same state — `parked`, `failed` or `unbound` — and `/healthz` SHALL fold that state to `degraded` or `recovering` and never to `healthy`

### Requirement: A park is reported, not only logged

The Controller SHALL emit `[:flezha, :operative, :parked]` with measurements `%{count: 1}` whenever an operative enters the parked state, from the single point every park funnels through, so a bind that could not complete, a restore the SDK refused and a migration that failed are each reported. The event's metadata SHALL carry the operative's `namespace` and `operative_id`, the whole park `reason` term, and `plugin_id` — the id of the plugin that caused the park, or `nil` when no plugin did, never a plugin id the reason does not name. A park caused by a plugin SHALL carry the failing plugin's provider module in its reason, so the event names both the row that refused and the module behind it. The event SHALL be lifted onto the observability bus as a `:metric` signal so a park reaches the configured exporters and not only a `:telemetry` handler.

Reporting a park SHALL NOT change when one happens: an unloadable provider module still parks the operative, and the plugin fold still halts on it, leaving the rows after it unattempted.

#### Scenario: A park caused by a plugin names the plugin

- **WHEN** an operative parks because a plugin's provider module could not be loaded
- **THEN** `[:flezha, :operative, :parked]` SHALL be emitted once, its metadata `plugin_id` SHALL be the failing plugin's id, and its `reason` SHALL name that plugin's provider module

#### Scenario: A park with no plugin behind it is reported too

- **WHEN** an operative parks for a reason no plugin caused — a restore the SDK refused, or a failed migration
- **THEN** `[:flezha, :operative, :parked]` SHALL still be emitted once for it, carrying that reason, and its metadata `plugin_id` SHALL be `nil`

#### Scenario: A park reaches the observability bus

- **WHEN** an operative parks while the observability subsystem is running
- **THEN** a `:metric` signal SHALL appear on the bus carrying the park's operative id, reason and plugin id

#### Scenario: Reporting does not change the parking policy

- **WHEN** one plugin in an operative's list has an unloadable provider and healthy plugins follow it
- **THEN** the operative SHALL park exactly as before and the plugins after the failing one SHALL still never be attempted

### Requirement: Cell-state introspection is gated by the class's persistence switch

A request for an operative's live per-Cell state SHALL succeed for an instance of either kind
whose class's persistence switch is on and which is resident on a runner. When the class has
opted out of persistence, the request SHALL fail with the structured error
`{:error, :persistence_off}` rather than querying a computation whose state is held in memory
alone, for a stream and a workflow alike; the CLI SHALL render it as a clear, non-zero outcome
naming the class's declaration. A suspended or unbound instance SHALL be refused as
`{:not_resident, :suspended}` or `{:not_resident, :unbound}` as today. Point-in-time
`flz state at` queries SHALL serve a Cell of either kind from its retained snapshots and WAL
records, under the same retention rule; no kind SHALL be refused time travel.

#### Scenario: Cell-state query succeeds for a durable operative

- **WHEN** the live per-Cell state of a bound instance whose class's persistence switch is on is requested, for a stream or a workflow
- **THEN** the request SHALL return the serialized current state

#### Scenario: Cell-state query is rejected for a stateless operative

- **WHEN** the live per-Cell state of an instance whose class declares persistence off is requested, for a stream or a workflow
- **THEN** the request SHALL return `{:error, :persistence_off}` and SHALL NOT contact the computation

#### Scenario: Time travel serves a workflow Cell

- **WHEN** `flz state at <instance/tasklet/cell> <seq>` targets a Cell of a durable `per_input` workflow whose iteration-boundary snapshots and WAL records cover `<seq>`
- **THEN** the query SHALL answer the Cell's state at that sequence exactly as it would for a stream Cell

