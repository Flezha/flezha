# runner-factory-sync Specification

## Purpose
TBD - created by archiving change sdk-runner-entrypoint-redesign. Update Purpose after archive.
## Requirements
### Requirement: UpdateFactories control message

The wire protocol SHALL define an SDK→controller control message `UpdateFactories` (proto tag 33 in the `CommandRequest` oneof) carrying the runner's **full current factory set** as `repeated FactoryDecl { id, description, sdk_lang }`. `sdk_lang` SHALL be the language byte of the class registered under that factory id — the same value the SDK declares to the poll core as `class.sdk_lang` and stamps at byte 0 of every durable record it writes. The connect/reconnect init frame's factory entries SHALL carry the same key, since the init frame is the resync of record. The message SHALL be fire-and-forget: the controller SHALL NOT reply with an ack, and the SDK SHALL NOT block on it. Full-set semantics make delivery idempotent; the connect/reconnect init frame remains the resync of record, so a lost `UpdateFactories` frame is corrected by the next frame or the next reconnect. A stale placement racing a removal fails at the SDK's `AddOperative` handler and is re-placed by convergence — the same failure mode as a runner dying mid-placement.

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

#### Scenario: The advertised language rides both halves of the seam
- **WHEN** a runner advertises factory A in its connect init frame and later ships `UpdateFactories{factories: [A, C]}`
- **THEN** both the init frame's entry for A and the `UpdateFactories` entries for A and C SHALL carry the language byte of the class registered under each id, and the controller SHALL store it beside the id and description

### Requirement: Controller applies UpdateFactories as a full-set replacement

`Flezha.Runner.ControlChannel` SHALL decode `UpdateFactories` on the existing SDK-initiated-command seam (alongside `ProcCommit`/`IterCommit`/`IterPark`) and dispatch it as an asynchronous cast to FleetManager — decoding SHALL NOT block the control channel. FleetManager SHALL replace the runner's stored factory list with the message's set by composing its existing removal and store paths (remove-then-store), scoped to that namespace + runner id; other runners' factory lists SHALL be untouched. An empty set SHALL be legal and SHALL leave the runner connected with zero placeable factories.

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

When a full-set replacement drops a factory that still has controller-known operatives bound to that runner (e.g. a parked/suspended durable workflow instance that is absent from the SDK's live-instance registry and therefore escaped the SDK-side removal check), FleetManager SHALL log a warning naming the namespace, runner, factory id, and the affected operatives. The warning is a tripwire, not enforcement — rehydration of such an operative will subsequently fail placement, and convergence surfaces it; a blocking policy is explicitly deferred.

#### Scenario: Suspended-instance removal hole is surfaced
- **GIVEN** a durable workflow operative of factory B is suspended (evicted from the SDK registry but registered on the controller as bound to runner R)
- **WHEN** `UpdateFactories` from R drops factory B
- **THEN** FleetManager SHALL apply the replacement AND log a warning naming B and the suspended operative

#### Scenario: No warning when nothing is affected
- **WHEN** `UpdateFactories` drops a factory with no controller-known operatives bound to that runner
- **THEN** the replacement SHALL apply silently (no warning)

### Requirement: A factory is homogeneous in its SDK language, and placement enforces it

The controller SHALL record, per **namespace and factory id**, one **established language**, in the existing class-declaration store and in the same shape as the other factory-keyed declaration (`keyless`). A factory id names a class within a deployment, not across the cluster — the conformance fleet alone gives the same reference ids to every SDK — so a cluster-global record would let one namespace's runner refuse another namespace's, and SHALL NOT be used.

The record MAY be in-memory: the durable header re-establishes it at the next bind and the SDK's own refusal is the backstop, so a restarted controller simply learns again, which is the path a never-bound factory takes anyway. It SHALL be established and corrected as follows:

- **The first runner to advertise the factory establishes it**, at its advertised language, held for as long as any runner advertises it.
- **The `sdk_lang` byte stamped in the factory's durable state outranks that**, and a recorded value that disagrees with the bytes on disk SHALL be corrected from them. The controller SHALL read that byte where it holds both the factory id and the operative's on-disk class — the operative **bind** — and SHALL do so *before* attempting the bind's restore, so a factory established at the wrong language is corrected rather than parking every attempt at it. The controller SHALL NOT read durable state on the runner-connect path: that path is served by a single actor whose stall blocks every concurrent placement lookup.
- **A lost record is not a conflict.** Where no language is recorded, the controller SHALL resolve it from the live advertisements for that factory and record the result — the value if every advertising runner agrees, and no established language (so nothing places) if they disagree. Both halves of the record are losable, so a missing one SHALL NOT read as a refusal.

Resolving the established language SHALL NOT require reading state on each placement.

Between a wrong establishment and its correction at the bind, the SDK-side language gate is the backstop: the bound runner refuses the other language's records, the restore fails, and the operative is left unbound rather than cold-started (see `operative-state-persistence`).

The single placement filter (`Flezha.Runner.FleetManager.placeable_runners/3`, read by `runners_for_factory/3`, the convergence rebind, and the dead-runner reroute) SHALL return only runners whose advertised language for that factory equals the established language. Every placement path SHALL obtain candidates through that filter; no placement path SHALL construct a candidate set of its own.

The homogeneous unit is the factory id. A namespace SHALL be free to hold factories of different languages, and a runner of one language SHALL remain a valid candidate for every factory established in its own language.

#### Scenario: A dead runner's operatives never reroute across languages
- **GIVEN** a namespace with a Rust runner and a Java runner, both advertising factory `orders`, whose durable state was written with `sdk_lang = 1`
- **WHEN** the Rust runner dies and its operatives are rerouted
- **THEN** the Java runner SHALL NOT be a placement candidate for `orders`, the operatives SHALL remain unbound with a reason naming the language mismatch, and no `RestoreInitState` SHALL be sent to the Java runner

#### Scenario: A mixed-language namespace still places each factory
- **GIVEN** a namespace where a Rust runner advertises `orders` and a Java runner advertises `billing`
- **WHEN** operatives of both factories are started
- **THEN** each SHALL be placed on the runner of its own language and both SHALL run

#### Scenario: The durable header outranks the recorded value
- **GIVEN** a factory recorded at `sdk_lang = 1` and an operative of that factory whose WAL headers carry `sdk_lang = 2`
- **WHEN** that operative binds
- **THEN** the recorded established language SHALL become `2`, read from the header, and a runner advertising `1` SHALL NOT be a candidate thereafter

#### Scenario: A lost record is re-derived, not read as a refusal
- **GIVEN** a factory two active runners advertise at `sdk_lang = 1`, whose established language is no longer recorded (the store was cleared with the factory's state directory)
- **WHEN** a placement resolves the factory's candidates
- **THEN** the established language SHALL be re-derived as `1` from the agreeing advertisements and re-recorded, and both runners SHALL be candidates

#### Scenario: Disagreeing advertisements with nothing recorded place nowhere
- **GIVEN** a factory with no recorded established language, advertised by one runner at `1` and another at `2`
- **WHEN** a placement resolves the factory's candidates
- **THEN** there SHALL be no candidate, because the controller cannot tell which language owns the factory

#### Scenario: A stateless factory establishes on first advertisement
- **GIVEN** a factory with no durable state and no recorded language
- **WHEN** a runner advertising language `3` connects and advertises it
- **THEN** the established language SHALL become `3`, and a runner advertising `1` for the same id SHALL NOT be a candidate while any runner still advertises it

#### Scenario: One namespace's language does not bind another's
- **GIVEN** factory `orders` established at `1` in namespace `alpha`
- **WHEN** a runner advertising `orders` at `2` connects to namespace `beta`, where nothing is established
- **THEN** it SHALL establish `orders` at `2` in `beta` and SHALL be a placement candidate there, and `alpha` SHALL remain established at `1`

### Requirement: An unspecified or conflicting language never places, and says so

A factory entry carrying no language (absent, or `sdk_lang = 0`) SHALL NOT make its runner a placement candidate for that factory, under any circumstances. There SHALL be no tolerant mode and no inference from transport, binary or namespace.

A runner advertising a language that conflicts with a factory's established language SHALL be stored (so operator tooling can see it) and SHALL be excluded from placement. The controller SHALL log a warning naming the namespace, runner, factory, established language and advertised language, and SHALL emit a telemetry event for it.

A placement that finds no candidate because of a language refusal SHALL report a distinct reason from a factory that no runner advertises at all: the existing "factory does not exist" message SHALL NOT be used for a factory that exists in another language.

#### Scenario: A runner advertising no language is not placeable
- **WHEN** a runner advertises factory `orders` with no `sdk_lang`
- **THEN** it SHALL NOT be a placement candidate for `orders`, and a start request for `orders` with no other candidate SHALL fail with an error naming the missing language declaration

#### Scenario: A conflicting advertisement is visible, not silent
- **GIVEN** factory `orders` established at language `1`
- **WHEN** a runner advertising `orders` at language `2` connects
- **THEN** the controller SHALL store the runner's factory set, SHALL exclude it from `orders` placement, SHALL log a warning naming both languages, and SHALL emit a telemetry event

#### Scenario: The refusal reason is distinguishable
- **GIVEN** factory `orders` established at language `1` and only a language-`2` runner connected
- **WHEN** an operative of `orders` is started
- **THEN** the error SHALL name the language mismatch and SHALL NOT claim the factory does not exist

