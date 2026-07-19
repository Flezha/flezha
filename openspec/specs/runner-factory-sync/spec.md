# runner-factory-sync Specification

## Purpose
TBD - created by archiving change sdk-runner-entrypoint-redesign. Update Purpose after archive.
## Requirements
### Requirement: UpdateFactories control message

The wire protocol SHALL define an SDK→controller control message `UpdateFactories` (proto tag 33 in the `CommandRequest` oneof) carrying the runner's **full current factory set** as `repeated FactoryDecl { id, description }`. The message SHALL be fire-and-forget: the controller SHALL NOT reply with an ack, and the SDK SHALL NOT block on it. Full-set semantics make delivery idempotent; the connect/reconnect init frame remains the resync of record, so a lost `UpdateFactories` frame is corrected by the next frame or the next reconnect. A stale placement racing a removal fails at the SDK's `AddOperative` handler and is re-placed by convergence — the same failure mode as a runner dying mid-placement.

#### Scenario: A post-start define reaches the controller
- **GIVEN** a connected runner that declared factory A at connect time
- **WHEN** the SDK ships `UpdateFactories{factories: [A, C]}`
- **THEN** the controller's stored factory list for that runner SHALL become exactly {A, C}, and a subsequent operative placement for C on that runner SHALL be possible

#### Scenario: Redelivery is idempotent
- **WHEN** the same `UpdateFactories{factories: [A, C]}` frame is processed twice
- **THEN** the controller's stored factory list for that runner SHALL be {A, C} after each processing, with no duplicate entries and no error

#### Scenario: Reconnect resyncs the factory set
- **GIVEN** an `UpdateFactories` frame was lost before a disconnect
- **WHEN** the runner reconnects and its init frame carries the current factory set
- **THEN** the controller's stored factory list SHALL match the init frame's set — the init frame is the resync of record

### Requirement: Controller applies UpdateFactories as a full-set replacement

`Flezha.Runner.ControlChannel` SHALL decode `UpdateFactories` on the existing SDK-initiated-command seam (alongside `OpCommit`/`IterCommit`/`IterPark`) and dispatch it as an asynchronous cast to FleetManager — decoding SHALL NOT block the control channel. FleetManager SHALL replace the runner's stored factory list with the message's set by composing its existing removal and store paths (remove-then-store), scoped to that namespace + runner id; other runners' factory lists SHALL be untouched. An empty set SHALL be legal and SHALL leave the runner connected with zero placeable factories.

#### Scenario: Full-set replacement drops removed factories
- **GIVEN** the controller's stored factory list for runner R is {A, B}
- **WHEN** `UpdateFactories{factories: [A]}` arrives from R
- **THEN** the stored list SHALL become exactly {A}; factory B SHALL no longer be placeable on R

#### Scenario: Other runners are untouched
- **GIVEN** runners R1 with factories {A} and R2 with factories {B} in the same namespace
- **WHEN** `UpdateFactories{factories: [C]}` arrives from R1
- **THEN** R1's stored list SHALL become {C} and R2's SHALL remain {B}

#### Scenario: Empty set is legal
- **WHEN** `UpdateFactories{factories: []}` arrives from a connected runner
- **THEN** the runner SHALL remain connected with an empty stored factory list, and no error SHALL be raised

### Requirement: Dropped factory with live controller-known operatives logs a warning

When a full-set replacement drops a factory that still has controller-known operatives bound to that runner (e.g. a parked/suspended durable bounded instance that is absent from the SDK's live-instance registry and therefore escaped the SDK-side removal check), FleetManager SHALL log a warning naming the namespace, runner, factory id, and the affected operatives. The warning is a tripwire, not enforcement — rehydration of such an operative will subsequently fail placement, and convergence surfaces it; a blocking policy is explicitly deferred.

#### Scenario: Suspended-instance removal hole is surfaced
- **GIVEN** a durable bounded operative of factory B is suspended (evicted from the SDK registry but registered on the controller as bound to runner R)
- **WHEN** `UpdateFactories` from R drops factory B
- **THEN** FleetManager SHALL apply the replacement AND log a warning naming B and the suspended operative

#### Scenario: No warning when nothing is affected
- **WHEN** `UpdateFactories` drops a factory with no controller-known operatives bound to that runner
- **THEN** the replacement SHALL apply silently (no warning)

