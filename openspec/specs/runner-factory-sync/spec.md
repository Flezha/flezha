# runner-factory-sync Specification

## Purpose
TBD - created by archiving change sdk-runner-entrypoint-redesign. Update Purpose after archive.
## Requirements
### Requirement: UpdateFactories control message

The wire protocol SHALL define an SDK→controller control message `UpdateFactories` (proto tag 33 in the `CommandRequest` oneof) carrying the runner's **full current factory set** as `repeated FactoryDecl { id, description, sdk_lang, kind }`. `sdk_lang` SHALL be the language byte of the class registered under that factory id — the same value the SDK declares to the poll core as `class.sdk_lang` and stamps at byte 0 of every durable record it writes. `kind` SHALL be exactly `"flow"` or `"steps"`, the kind of the class registered under that id: the Rust and Elixir SDKs SHALL fill it from the registered class, and the shared core SHALL fill it for every host SDK from the registry the class declaration landed in, where it fills `sdk_lang`. The connect/reconnect init frame's factory entries SHALL carry the same keys, since the init frame is the resync of record. The message SHALL be fire-and-forget: the controller SHALL NOT reply with an ack, and the SDK SHALL NOT block on it. Full-set semantics make delivery idempotent; the connect/reconnect init frame remains the resync of record, so a lost `UpdateFactories` frame is corrected by the next frame or the next reconnect. A stale placement racing a removal fails at the SDK's `AddRelay` handler and is re-placed by convergence — the same failure mode as a runner dying mid-placement.

#### Scenario: A post-start define reaches the controller
- **GIVEN** a connected runner that declared factory A at connect time
- **WHEN** the SDK ships `UpdateFactories{factories: [A, C]}`
- **THEN** the controller's stored factory list for that runner SHALL become exactly {A, C}, and a subsequent relay placement for C on that runner SHALL be possible

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

#### Scenario: The advertised kind rides both halves of the seam
- **WHEN** a runner advertises a flow factory A in its connect init frame and later ships `UpdateFactories{factories: [A, W]}` where W is a steps class
- **THEN** the init frame's entry for A SHALL carry `kind` `"flow"`, the `UpdateFactories` entries SHALL carry `"flow"` for A and `"steps"` for W, and the controller SHALL store each beside the id, description and language

### Requirement: Controller applies UpdateFactories as a full-set replacement

`Flezha.Runner.ControlChannel` SHALL decode `UpdateFactories` on the existing SDK-initiated-command seam (alongside `TaskCommit`/`IterCommit`/`IterPark`) and dispatch it as an asynchronous cast to FleetManager — decoding SHALL NOT block the control channel. FleetManager SHALL replace the runner's stored factory list with the message's set by composing its existing removal and store paths (remove-then-store), scoped to that namespace + runner id; other runners' factory lists SHALL be untouched. An empty set SHALL be legal and SHALL leave the runner connected with zero placeable factories.

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

### Requirement: Dropped factory with live controller-known relays logs a warning

When a full-set replacement drops a factory that still has controller-known relays bound to that runner (e.g. a parked/suspended durable steps instance that is absent from the SDK's live-instance registry and therefore escaped the SDK-side removal check), FleetManager SHALL log a warning naming the namespace, runner, factory id, and the affected relays. The warning is a tripwire, not enforcement — rehydration of such a relay will subsequently fail placement, and convergence surfaces it; a blocking policy is explicitly deferred.

#### Scenario: Suspended-instance removal hole is surfaced
- **GIVEN** a durable steps relay of factory B is suspended (evicted from the SDK registry but registered on the controller as bound to runner R)
- **WHEN** `UpdateFactories` from R drops factory B
- **THEN** FleetManager SHALL apply the replacement AND log a warning naming B and the suspended relay

#### Scenario: No warning when nothing is affected
- **WHEN** `UpdateFactories` drops a factory with no controller-known relays bound to that runner
- **THEN** the replacement SHALL apply silently (no warning)

### Requirement: A factory is homogeneous in its SDK language, and placement enforces it

The controller SHALL record, per **namespace and factory id**, one **established language**, in the existing class-declaration store and in the same shape as the other factory-keyed declaration (`keyless`). A factory id names a class within a deployment, not across the cluster — the conformance fleet alone gives the same reference ids to every SDK — so a cluster-global record would let one namespace's runner refuse another namespace's, and SHALL NOT be used.

The record MAY be in-memory: the durable header re-establishes it at the next bind and the SDK's own refusal is the backstop, so a restarted controller simply learns again, which is the path a never-bound factory takes anyway. It SHALL be established and corrected as follows:

- **The first runner to advertise the factory establishes it**, at its advertised language, held for as long as any runner advertises it.
- **The `sdk_lang` byte stamped in the factory's durable state outranks that**, and a recorded value that disagrees with the bytes on disk SHALL be corrected from them. The controller SHALL read that byte where it holds both the factory id and the relay's on-disk class — the relay **bind** — and SHALL do so *before* attempting the bind's restore, so a factory established at the wrong language is corrected rather than parking every attempt at it. The controller SHALL NOT read durable state on the runner-connect path: that path is served by a single actor whose stall blocks every concurrent placement lookup.
- **A lost record is not a conflict.** Where no language is recorded, the controller SHALL resolve it from the live advertisements for that factory and record the result — the value if every advertising runner agrees, and no established language (so nothing places) if they disagree. Both halves of the record are losable, so a missing one SHALL NOT read as a refusal.

Resolving the established language SHALL NOT require reading state on each placement.

Between a wrong establishment and its correction at the bind, the SDK-side language gate is the backstop: the bound runner refuses the other language's records, the restore fails, and the relay is left unbound rather than cold-started (see `relay-state-persistence`).

The single placement filter (`Flezha.Runner.FleetManager.Factories.placeable_runners/3`, read by `runners_for_factory/3`, the convergence rebind, and the dead-runner reroute) SHALL return only runners whose advertised language for that factory equals the established language. Every placement path SHALL obtain candidates through that filter; no placement path SHALL construct a candidate set of its own.

The homogeneous unit is the factory id. A namespace SHALL be free to hold factories of different languages, and a runner of one language SHALL remain a valid candidate for every factory established in its own language.

#### Scenario: A dead runner's relays never reroute across languages
- **GIVEN** a namespace with a Rust runner and a Java runner, both advertising factory `orders`, whose durable state was written with `sdk_lang = 1`
- **WHEN** the Rust runner dies and its relays are rerouted
- **THEN** the Java runner SHALL NOT be a placement candidate for `orders`, the relays SHALL remain unbound with a reason naming the language mismatch, and no `RestoreInitState` SHALL be sent to the Java runner

#### Scenario: A mixed-language namespace still places each factory
- **GIVEN** a namespace where a Rust runner advertises `orders` and a Java runner advertises `billing`
- **WHEN** relays of both factories are started
- **THEN** each SHALL be placed on the runner of its own language and both SHALL run

#### Scenario: The durable header outranks the recorded value
- **GIVEN** a factory recorded at `sdk_lang = 1` and a relay of that factory whose WAL headers carry `sdk_lang = 2`
- **WHEN** that relay binds
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

A runner advertising a language that conflicts with a factory's established language SHALL be stored (so user tooling can see it) and SHALL be excluded from placement. The controller SHALL log a warning naming the namespace, runner, factory, established language and advertised language, and SHALL emit a telemetry event for it.

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
- **WHEN** a relay of `orders` is started
- **THEN** the error SHALL name the language mismatch and SHALL NOT claim the factory does not exist

### Requirement: A factory is homogeneous in its run, and placement enforces it

The controller SHALL record, per **namespace and factory id**, one **established kind**, beside the established language and by the same rule: the first runner to advertise the factory establishes its kind, held for as long as any runner advertises it; a runner advertising the same id at the other kind SHALL be stored, excluded from placement for that factory by the single placement filter (`Flezha.Runner.FleetManager.Factories.placeable_runners/3`), warned about naming the namespace, runner, factory, established kind and advertised kind, and counted by a telemetry event. A factory entry carrying no kind, or a kind other than `flow` or `steps`, SHALL NOT make its runner a placement candidate for that factory, and a start naming such a factory with no other candidate SHALL fail with an error naming the missing or unknown kind declaration, distinct from "factory does not exist".

Every start path SHALL resolve the relay's kind from this record before admission: `flz relay start` and the service verb take it as the instance's kind; a schedule fire and a tasklet-requested spawn, which are steps-only, SHALL be refused by name when the record says `flow`. Resolving the established kind SHALL NOT require reading state on each placement.

#### Scenario: The first advertisement establishes the kind

- **GIVEN** a namespace with no record for `paper_order`
- **WHEN** a runner connects announcing `paper_order` with kind `steps`
- **THEN** the established kind SHALL be `steps`, and `flz relay start paper_order` SHALL start a steps instance

#### Scenario: A conflicting kind is visible and not placeable

- **GIVEN** factory `orders` established as a flow relay
- **WHEN** a second runner announces `orders` as a steps relay
- **THEN** the controller SHALL store its factory set, SHALL exclude it from `orders` placement, SHALL log a warning naming both kinds, and SHALL emit a telemetry event

#### Scenario: An announcement with no kind is not placeable

- **WHEN** a runner announces factory `orders` with an empty `kind`
- **THEN** it SHALL NOT be a placement candidate for `orders`, and a start of `orders` with no other candidate SHALL fail naming the missing kind declaration

#### Scenario: A spawn naming a flow factory is refused by name

- **WHEN** a tasklet requests a spawn of factory `prices`, which its runner announced as a flow relay
- **THEN** the controller SHALL answer the code `relay.factory_not_steps` without placing anything, and SHALL NOT answer `relay.factory_unknown`

### Requirement: The factory announcement carries the run and refuses a kind literal

`UpdateFactories` SHALL carry each factory's run, `flow` or `steps` with `each` and
`on_failure`, in place of the retired `kind` field. Two runners announcing the same
factory id with different runs SHALL be refused as today's kind conflict was, and a runner
whose announcement carries `steps`, `flow` or an empty run SHALL be refused naming
the literal; an unspecified run SHALL never place.

#### Scenario: Two runs for one factory id

- **WHEN** one runner announces `checkout` as `flow` and another as `steps`
- **THEN** the controller SHALL refuse the second announcement naming both runs, and SHALL place `checkout` only on runners announcing the first

#### Scenario: A kind literal never places

- **WHEN** a runner announces a factory whose run field reads `flow`
- **THEN** the controller SHALL log the refusal naming the literal and SHALL not place that factory

### Requirement: The factory announcement SHALL carry the factory's API

Every `FactoryDecl` a runner sends, at connect and in `UpdateFactories`, SHALL carry one `ApiEntry`
per entry channel of the class: the channel, its codec name, its input schema and description when
declared, and, when an output that answers is reachable from the channel, the answer's codec and schema when known
(`bytes` with no schema when the reachable outputs disagree). A runner without this
change SHALL be accepted, and its factories SHALL have no API.

#### Scenario: The announcement lists the entries

- **WHEN** a runner registers a class with entry channels `place` (json, answering) and `audit`
  (text)
- **THEN** its `FactoryDecl` carries two `ApiEntry` values, `place` with an `answer_codec` and
  `audit` without one

#### Scenario: An old runner is still placed

- **WHEN** a runner sends a `FactoryDecl` with no `api` field
- **THEN** its factory is placed as before and `Relay.api` lists it with no channels

