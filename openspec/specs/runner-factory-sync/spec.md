# runner-factory-sync Specification

## Purpose
TBD - created by archiving change sdk-runner-entrypoint-redesign. Update Purpose after archive.
## Requirements
### Requirement: UpdateFactories control message

The wire protocol SHALL define an SDK→controller control message `UpdateFactories` (proto tag 33 in the `CommandRequest` oneof, carried as the `request` of a `ControlFrame`) carrying the runner's **full current factory set** as `repeated FactoryDecl { id, description, sdk_lang, run, each, on_failure, api, graph }`. `sdk_lang` SHALL be the language byte of the class registered under that factory id — the same value the SDK declares to the poll core as `class.sdk_lang` and stamps at byte 0 of every durable record it writes. `run` SHALL be exactly `"flow"` or `"steps"`, the run of the class registered under that id, and the shared core SHALL fill every field for every SDK from the registry the class declaration landed in. The connect/reconnect init frame's `factories` SHALL be the same `FactoryDecl` values serialized once by serde, under the proto field names, so the init frame and `UpdateFactories` carry one struct in one form and the controller reads both with one reader; the init frame is the resync of record. The message SHALL be fire-and-forget: the controller SHALL NOT reply with an ack, and the SDK SHALL NOT block on it. Full-set semantics make delivery idempotent; the connect/reconnect init frame remains the resync of record, so a lost `UpdateFactories` frame is corrected by the next frame or the next reconnect. A stale placement racing a removal fails at the SDK's `AddRelay` handler and is re-placed by convergence — the same failure mode as a runner dying mid-placement.

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
- **THEN** the init frame's entry for A SHALL carry `run` `"flow"`, the `UpdateFactories` entries SHALL carry `"flow"` for A and `"steps"` for W, and the controller SHALL store each beside the id, description and language

#### Scenario: Both frames serialize one struct
- **WHEN** the init frame's `factories` entry for A is compared with the `FactoryDecl` the core builds for A
- **THEN** the entry SHALL be that struct's serde form, key for key, with no separate announcement type in the core

### Requirement: Controller applies UpdateFactories as a full-set replacement

`Flezha.Runner.ControlChannel` SHALL decode `UpdateFactories` from the `request` half of a control frame, beside the other runner-initiated commands (`TaskCommit`/`IterCommit`/`IterPark`), and dispatch it as an asynchronous cast to FleetManager — decoding SHALL NOT block the control channel. FleetManager SHALL replace the runner's stored factory list with the message's set by composing its existing removal and store paths (remove-then-store), scoped to that namespace + runner id; other runners' factory lists SHALL be untouched. An empty set SHALL be legal and SHALL leave the runner connected with zero placeable factories. The entries SHALL be read by the same reader the connect init frame's entries are read by.

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

### Requirement: The factory announcement SHALL carry the class's graph

Every factory a runner announces SHALL carry a `graph`, in the connect init frame's `factories`
list and in each `FactoryDecl` of `UpdateFactories`, built by `flz_core` from the registered
class's topology for every SDK.
The graph SHALL list every channel of the class with its kind (`entry`, `inter_tasklet` or
`plugin_bound`), the tasklet that produces it (none for an entry), the tasklets that consume it, its
plugin end (`none`, `output` or `call`), the target of a `call` end, its overflow policy, its retry
policy (`retry_max`, the total attempts, and `retry_backoff_ms`, the base backoff) and, for a `call`
end, its deadline (`call_deadline_ms`) and whether it creates the callee (`call_create`). It SHALL
list every tasklet with its name, its cell mode (`single`, `static` or `dynamic`), its declared cell
keys, its mailbox capacity, whether it compensates (`compensates`: true exactly when the class runs
in steps and the tasklet declares a `compensate` hook) and, for a `dynamic` tasklet, its idle
eviction window (`idle_ttl_ms`, absent when it never evicts) and its cap on live Cells
(`max_cells`, absent when it has no cap). Tasklets and channels SHALL be named as the class
names them. Every scalar field
this requirement adds SHALL be `optional` on the wire, so a runner that does not send it is read as
having announced nothing, never as zero or false. The graph SHALL be built only when the factory is
announced. A runner without the graph SHALL be accepted, and its factories SHALL have no graph.

#### Scenario: The announcement lists tasklets and channels

- **WHEN** a runner registers a class with entry `trades_in` consumed by tasklets `ohlc` and `trades`, and `ohlc` producing `ohlc_out`, consumed by tasklet `atr` and ending at a plugin output
- **THEN** its announced graph lists channel `trades_in` as an entry with consumers `ohlc` and `trades`, channel `ohlc_out` as produced by `ohlc` with consumer `atr` and plugin end `output`, and tasklets `ohlc`, `trades` and `atr` with their cell modes

#### Scenario: A call end names its target

- **WHEN** a class has a channel whose end is a `call(...)` to factory `pricing` with a 2000 ms deadline
- **THEN** that channel's plugin end is `call`, its call target is `factory:pricing` and its `call_deadline_ms` is 2000

#### Scenario: A compensating tasklet is marked

- **WHEN** a steps class's tasklet `reserve` declares `compensate` and its tasklet `notify` does not
- **THEN** the announced graph marks `reserve` with `compensates: true` and `notify` with `compensates: false`

#### Scenario: Every runtime announces the fixture's graph

- **WHEN** each runtime registers the classes of `test/fixtures/reference_topologies.txt`
- **THEN** each announced graph lists the channels, kinds, producers, consumers and plugin ends the fixture records

#### Scenario: An old runner is still placed

- **WHEN** a runner announces a factory with no `graph`
- **THEN** its factory is placed as before and reads as having no announced graph

#### Scenario: A runner that omits the new fields

- **WHEN** a runner announces a graph without `compensates`, `idle_ttl_ms`, `max_cells`, `retry_max`, `retry_backoff_ms`, `call_deadline_ms` or `call_create`
- **THEN** the controller keeps each of them as not announced (nil), not as zero or false

### Requirement: A factory change after start SHALL reach the controller from every SDK

Every SDK's register and unregister of a factory SHALL work before and after the runner starts,
through the core's one session. After start, a register or an unregister SHALL ship
`UpdateFactories` with the full current set. An unregister after start SHALL be checked on the
core's coordinator, serialized with `AddRelay`, and SHALL be refused with
`runtime.relay_type_in_use` while live instances of the factory exist on the runner, and only then.
The unregister SHALL wait for the coordinator's answer holding no session lock, so a register from
another thread proceeds meanwhile, and for at most `FLEZHA_RUNNER_START_WAIT_MS` (`0` waits for
ever); past it the unregister SHALL fail with `runtime.runner_unavailable`, the factory SHALL stay
registered and declared, and the coordinator SHALL NOT apply that removal later.
No SDK SHALL refuse an unregister after start for any other reason, and no SDK SHALL accept a
register after start that the controller never learns of. The one exception is PHP's Swoole
executor, whose threads each serve the classes registered before start: under it a register or
an unregister after start SHALL be refused with `runtime.executor_unavailable` naming `swoole`. The SDK's executor SHALL serve the change too:
a factory registered after start SHALL run its activations, and one unregistered after start SHALL
no longer be served.

#### Scenario: Ruby registers after start

- **WHEN** a started Ruby runner registers factory C
- **THEN** the controller's stored factory list for that runner SHALL include C, and a relay of C SHALL be placeable on it

#### Scenario: Node unregisters an idle factory after start

- **WHEN** a started Node runner unregisters factory B with no live instance of B
- **THEN** the call SHALL succeed and the controller's stored list SHALL no longer include B

#### Scenario: A factory with a live instance is kept

- **WHEN** any SDK unregisters factory B after start while an instance of B runs on that runner
- **THEN** the call SHALL fail with `runtime.relay_type_in_use` naming B and the instance count, and the controller's stored list SHALL still include B

#### Scenario: PHP's Swoole executor refuses a change after start

- **WHEN** a PHP runner started under the Swoole executor registers or unregisters a factory
- **THEN** the call SHALL fail with `runtime.executor_unavailable` naming `swoole`, and the runner's registered classes SHALL be unchanged

#### Scenario: An unanswered unregister gives up and is never applied

- **WHEN** a started runner unregisters factory B while its coordinator answers nothing (a controller outage it is reconnecting through)
- **THEN** a register of factory C on another thread SHALL complete meanwhile
- **AND** the unregister SHALL fail with `runtime.runner_unavailable` within `FLEZHA_RUNNER_START_WAIT_MS`
- **AND** B SHALL stay registered when the coordinator reaches the removal

#### Scenario: A factory removed after start is no longer served

- **WHEN** a started PHP runner unregisters an idle factory B and an activation for B then reaches it
- **THEN** the runner SHALL refuse the activation as an unregistered class rather than run it

### Requirement: The controller SHALL read a steps class's loop shape from the announced `each` alone

The controller SHALL decide whether a steps class reads one input only and whether it may run
as a partition group from the `each` its factory announcement already carries, through one
predicate over that established attribute. The announcement SHALL carry no second field that
restates what `each` says, and no controller code other than that predicate SHALL name the `once`
or `chained` loop shapes.

#### Scenario: A once class reads one input

- **WHEN** a runner announces a steps class whose `each` is `once`
- **THEN** its `FactoryDecl` SHALL carry `each` `once` and no separate partitioning field, and the controller's one-input rule SHALL hold for its instances, decided from the established `each`

#### Scenario: The partition refusal and the one-input rule agree

- **WHEN** the controller's one predicate answers that a class reads one input
- **THEN** a partitioned start of that class SHALL be refused with `topology.partitions_without_per_input_loop`, and the one-input rule SHALL hold for its instances, both from that one answer

### Requirement: The factory announcement SHALL carry the class's declarations

Every factory a runner announces SHALL carry `declarations`, a map from name to text, in the
connect init frame's `factories` list and in each `FactoryDecl` of `UpdateFactories`. It SHALL
hold exactly the entries the `AddRelay` answer's metadata holds for that class, produced by one
`flz_core` function that both the announcement and the `AddRelay` answer call, so the two can
never differ. The controller SHALL decode it with the decoder it applies to `AddRelay` metadata
and SHALL keep the decoded map per runner announcement beside the API and the graph, replaced by
every full-set announcement. Declarations decide no placement. An empty map SHALL be read as "not
announced": a class from a current runner always declares its durability, so a current runner
never sends an empty map.

#### Scenario: The announcement and the answer agree

- **WHEN** a runner registers a class declaring `durable: {local: {sleep_after: 2000}}` and `max_duration_ms: 120000`
- **THEN** its announcement's declarations and the `AddRelay` answer's metadata for an instance of it both carry `durable: local`, `sleep_after_ms: 2000` and `max_duration_ms: 120000`

#### Scenario: A class changes after start

- **WHEN** a started runner re-registers factory `ma_fan` with `durable: remote` and ships `UpdateFactories`
- **THEN** the controller's kept declarations for that runner's `ma_fan` carry `durable: remote`

#### Scenario: An old runner announces none

- **WHEN** a runner announces factory `legacy` with no declarations
- **THEN** the controller keeps no declarations for it and places it as before

