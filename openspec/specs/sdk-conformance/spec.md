# sdk-conformance Specification

## Purpose
TBD - created by archiving change sdk-conformance-suite. Update Purpose after archive.
## Requirements
### Requirement: The runtime under test is selected per run, from the environment

The end-to-end harness SHALL resolve the SDK runtime for a run from `FLEZHA_E2E_RUNTIME` (one of the runtimes that carry a conformance manifest; default `rust`). `start_runner/2` and `start_runner_port/2` SHALL use that runtime when the caller passes no `:runtime` option. A test under `test/system/` SHALL NOT pass an explicit `:runtime` unless the module or test is tagged `interop: true` — a scenario that deliberately places more than one SDK. One run SHALL exercise one runtime; a full cross-SDK check is one run per runtime.

#### Scenario: The whole suite runs against a chosen SDK with no test edited
- **WHEN** `FLEZHA_E2E_RUNTIME=java mix test --include system` is run
- **THEN** every runner the suite spawns is the Java test app, and no file under `test/system/` differs from the run that used the default

#### Scenario: The default is unchanged
- **WHEN** `mix test --include system` is run with `FLEZHA_E2E_RUNTIME` unset
- **THEN** every runner the suite spawns is the Rust test app

#### Scenario: An unknown runtime fails before any test runs
- **WHEN** `FLEZHA_E2E_RUNTIME` names a runtime with no manifest under `test/apps/`
- **THEN** `test_helper.exs` raises naming the value and the manifests that exist, and no test starts

### Requirement: Every SDK test app carries a conformance manifest


Each SDK's test app directory `test/apps/<sdk>_runner/` SHALL contain `conformance.json` declaring: `runtime` (the name `FLEZHA_E2E_RUNTIME` selects it by); `command` (a launch template over `{artifact}`, `{app_dir}`, `{repo}`, `{namespace}` and the absolute-path placeholders `{socket_dir}`, `{ca_cert}`, `{client_cert}`, `{client_key}`); `artifacts` with a `test` and a `bench` profile, each an ordered list of paths relative to the app directory; `build` with the one command that produces each profile's artifact; `connect_timeout_ms`; `transports` (the subset of `uds`, `quic` the SDK's standalone runner supports); `dispatch` (how the SDK's host executes activations — see below); `toolchain` (the shell command that prints its toolchain's version); `deferred` (capabilities from the shared vocabulary the SDK does not carry); `fixtures` (a map from capability to the factory ids the app registers for it); and `docs` (the path of the page that documents the SDK's scope). Every vocabulary capability SHALL appear in a manifest exactly once — under `deferred` or as a `fixtures` key — so a capability can be neither forgotten nor both; the required capabilities (`runner_lifecycle`, `graph_builder`, `liveness`, `retry_ladder`, `partitioned`, `pool_wedge` — the surface every SDK carries) SHALL NOT appear under `deferred`. The page named by `docs` SHALL exist and SHALL reference the manifest by path rather than restate its scope. The Rust test app's manifest SHALL be the reference: a fixture id exists for a capability if and only if the reference lists it there. Both spawners — the test harness (`test` profile) and the bench fleet (`bench` profile) — SHALL render the launch line from the manifest and SHALL carry no per-runtime launch clause of their own. The first artifact in a profile's list that exists SHALL be used; using a later one SHALL log a warning naming the first; none existing SHALL raise with the profile's `build` command.

`pool_wedge` becomes required because a host that cannot be wedged on demand cannot prove it recovers: the fixture is what exercises the abandon path, the worker replacement and the classifier's verdict on that SDK rather than on Rust alone.

`dispatch` SHALL declare `model` — a slug from a closed vocabulary held in one place in the harness (`thread-pool`, `event-loop`, `process-per-entity`) — `detail`, one sentence describing how activations are executed, and `env`, a map from each environment variable that selects the SDK's dispatch topology to a description of what it selects. A runtime whose topology is fixed SHALL declare an empty `env`. Every SDK's manifest SHALL carry it, because a throughput number taken under one dispatch topology and one taken under another are different measurements of the same fixture, and only the manifest knows which. `toolchain` SHALL be the shell command that prints the version of the toolchain that runs the SDK, so a bench header can name it. A command that does not run SHALL report the toolchain as unavailable rather than fail the caller.

A consumer that reports a runtime's dispatch topology or toolchain version SHALL read it from the manifest and SHALL carry no per-runtime clause of its own — the same rule the launch line already follows. A per-runtime clause is a list a new SDK has to remember to join; the manifest is where a runtime already says what it is.

#### Scenario: Adding an SDK touches no harness code
- **WHEN** a new `test/apps/<sdk>_runner/` directory with a valid manifest is added and `FLEZHA_E2E_RUNTIME=<sdk> mix test --include sdk_smoke` is run
- **THEN** the harness launches the new app from its manifest without any change to `test/helpers/`, `test/system/` or `test/test_helper.exs`

#### Scenario: A missing artifact names its build command
- **WHEN** no path in the selected profile's `artifacts` exists
- **THEN** the spawn raises a message containing that profile's `build` command

#### Scenario: A capability left out of a manifest fails the gate
- **WHEN** the vocabulary gains `entry_idle` and a manifest lists it neither under `deferred` nor under `fixtures`
- **THEN** the static gate fails naming the manifest and `entry_idle`

#### Scenario: A required capability cannot be deferred
- **WHEN** a manifest lists `retry_ladder` under `deferred`
- **THEN** the static gate fails naming the manifest and the required set

#### Scenario: pool_wedge cannot be deferred either
- **WHEN** a manifest lists `pool_wedge` under `deferred`
- **THEN** the static gate fails naming the manifest and the required set

#### Scenario: The bench and the test harness launch the same app the same way
- **WHEN** `mix bench --runtime java` and `FLEZHA_E2E_RUNTIME=java mix test --include sdk_smoke` each spawn a runner
- **THEN** both launch lines are rendered from `test/apps/java_runner/conformance.json`, differing only in the artifact profile

#### Scenario: A manifest without a dispatch declaration fails the gate
- **WHEN** a manifest omits `dispatch`, or declares a `model` outside the vocabulary
- **THEN** the static gate fails naming the manifest, the missing or unknown value, and the vocabulary

#### Scenario: A manifest without a toolchain command fails the gate
- **WHEN** a manifest omits `toolchain`
- **THEN** the static gate fails naming the manifest and what the field is for

#### Scenario: The dispatch topology is reported from the manifest, not from a clause
- **WHEN** a bench is run against a runtime whose manifest declares a `dispatch.env` entry
- **THEN** the run header names the declared model and the current value of that variable, and no bench task contains a per-runtime dispatch clause

### Requirement: A scenario has one fixture id in every SDK

A fixture that implements the same scenario SHALL register the same factory id in every SDK test app, and its sink channel names, output bytes and declared options SHALL match the reference. No test app SHALL register a fixture id prefixed with its SDK's name, and no test under `test/system/` SHALL select a fixture by runtime. A test app SHALL NOT register a fixture id absent from the reference.

#### Scenario: A generic test drives the same fixture on every SDK
- **WHEN** `entity_timers_e2e_test.exs` starts the `session_timeout` fixture under `FLEZHA_E2E_RUNTIME=rust` and again under `FLEZHA_E2E_RUNTIME=java`
- **THEN** both runs bind a fixture registered as `session_timeout` and both pass the same assertions

#### Scenario: A prefixed id fails the gate
- **WHEN** a manifest lists a fixture id such as `java_session_timeout` that the reference does not list
- **THEN** the static gate fails naming the manifest, the capability and the id

### Requirement: Scenarios are tagged by capability; deferred capabilities and unsupported transports skip by name

The capability vocabulary SHALL be one list in the harness (`runner_lifecycle`, `graph_builder`, `liveness`, `pool_wedge`, `retry_ladder`, `state_persistence`, `effectively_once`, `entity_timers`, `correction_window`, `keyed_channels`, `workflow`, `dynamic_entity`, `entry_idle`, `partitioned`). A capability MAY carry no fixtures in any manifest when the scenarios that prove it need no fixture of their own — `pool_wedge` is the classifier signature a host with a shared dispatch pool produces and a host that runs a process per Entity cannot, and is therefore optional rather than required. Every module under `test/system/` SHALL carry `@moduletag capability: <atom>` from the vocabulary; a test MAY override it with `@tag capability:`. For the selected runtime, `test_helper.exs` SHALL exclude every test whose capability the runtime's manifest defers and every test tagged with a transport the manifest does not list, and SHALL print one line naming the runtime, the manifest path, the deferred capabilities and the skipped transports. A capability the manifest does not defer SHALL run and SHALL pass; there is no third state.

#### Scenario: A deferred capability skips visibly
- **WHEN** `FLEZHA_E2E_RUNTIME=elixir mix test --include system` is run and the Elixir manifest defers `entity_timers`
- **THEN** no test tagged `capability: :entity_timers` runs, and the run's output contains a line naming `elixir`, `test/apps/elixir_runner/conformance.json` and `entity_timers`

#### Scenario: A capability that is neither deferred nor passing fails the run
- **WHEN** a runtime's manifest does not defer `retry_ladder` and its runner does not park a poison payload
- **THEN** `dlq_e2e_test.exs` fails for that runtime; nothing in the harness converts the failure into a skip

#### Scenario: An unsupported transport skips by name
- **WHEN** a manifest lists `transports: ["uds"]` and a test is tagged `transport: :quic`
- **THEN** that test is excluded and the printed line names `quic` as skipped

### Requirement: A static drift gate runs in the default test suite

An untagged ExUnit module SHALL run under plain `mix test` — on every pull request, with no runner spawned — and SHALL assert: every manifest under `test/apps/*/conformance.json` parses and declares every field of the manifest requirement; every capability named under `deferred` or `fixtures` is in the vocabulary; for every capability an SDK does not defer, its fixture set equals the reference's; every module under `test/system/` carries a vocabulary capability tag; every string literal in a `test/system/` file that equals a reference fixture id belongs to a capability that file is tagged with; no `runtime:` literal appears in `test/system/` outside a module or test tagged `interop: true`; every directory matching `test/apps/*_runner/` carries a manifest; every manifest's `runtime` is an option of `nightly-e2e.yml`'s `workflow_dispatch` `runtime` input; every manifest's `docs` page exists and contains the manifest's path. The gate SHALL print, not fail on, every vocabulary capability with no tagged module. Every gate failure message SHALL end with the path of the "Adding an SDK to the conformance suite" documentation section.

#### Scenario: A fixture added to one SDK and not the others fails the PR
- **WHEN** the Rust manifest gains `new_probe` under `liveness` and the Java manifest, which does not defer `liveness`, does not
- **THEN** `mix test` fails in the gate naming `java`, `liveness` and `new_probe`

#### Scenario: An untagged e2e module fails the PR
- **WHEN** a module is added under `test/system/` without a `capability:` tag
- **THEN** `mix test` fails in the gate naming the file

#### Scenario: A stray runtime literal fails the PR
- **WHEN** a test under `test/system/` passes `runtime: :java` without an `interop: true` tag
- **THEN** `mix test` fails in the gate naming the file and the line

#### Scenario: An untested capability is visible
- **WHEN** no module under `test/system/` is tagged `capability: :keyed_channels`
- **THEN** the gate's output names `keyed_channels` as untested and the gate passes

### Requirement: Rolling out a new SDK is enforced by the gate, not remembered

The first pull request that adds a directory `test/apps/<sdk>_runner/` SHALL NOT pass `mix test` until, in that same pull request: the directory carries a manifest that accounts for every vocabulary capability and defers none of the required set; the manifest's non-deferred fixture sets equal the reference's; the manifest's `runtime` is a `workflow_dispatch` option of `nightly-e2e.yml`; and the manifest's `docs` page exists and points at the manifest. The suite SHALL then admit the SDK with no change to `test/helpers/`, `test/system/`, `test/test_helper.exs`, the formatter or the gate. The reverse direction SHALL hold with the same mechanism: a change that adds a vocabulary capability, a reference fixture, a required capability or a declaration snapshot SHALL fail the gate or the smoke tier for every existing SDK that neither carries the addition nor defers it by name in its manifest, so a lockstep omission is a red PR, not a backlog item. The documentation section "Adding an SDK to the conformance suite" SHALL list exactly the items the gate enforces, followed by the items it cannot (the SDK's own unit-test workflow, `docs/sdk_naming_conventions.md` compliance, the dev note and its `CLAUDE.md` index line), and SHALL be the path every gate failure names.

#### Scenario: A new SDK directory without a manifest is a red PR
- **WHEN** `test/apps/ruby_runner/` is added with a test app and no `conformance.json`
- **THEN** `mix test` fails in the gate naming the directory and the documentation section

#### Scenario: A new SDK is admitted by the gate and the smoke tier alone
- **WHEN** `test/apps/ruby_runner/` is added with a manifest that defers every non-required capability, registers the required capabilities' reference fixtures, names `ruby` in the nightly dispatch options and points `docs` at a page that references the manifest
- **THEN** `mix test` passes the gate, and `FLEZHA_E2E_RUNTIME=ruby mix test --include sdk_smoke` runs the smoke tier and the required capabilities' snapshots with every deferred capability skipped by name

#### Scenario: A new capability cannot skip an existing SDK silently
- **WHEN** a change adds `new_cap` to the vocabulary with reference fixtures and does not touch the Java manifest
- **THEN** the gate fails naming `java` and `new_cap`; the change must add the Java fixtures or list `new_cap` under the Java manifest's `deferred`

#### Scenario: A new declaration key cannot skip an existing SDK silently
- **WHEN** the reference gains a declaration key, its golden is regenerated, and the Java builder does not emit it
- **THEN** the next `FLEZHA_E2E_RUNTIME=java` smoke run fails the snapshot naming the key

### Requirement: The live factory set matches the manifest

The smoke tier SHALL assert that the factory ids the controller lists for a freshly connected runner (`FleetManager.list_factories/1`) equal the union of the runtime's manifest `fixtures`.

#### Scenario: The manifest and the app disagree
- **WHEN** a manifest lists `session_timeout` under `entity_timers` and the app registers `java_session_timeout`
- **THEN** the smoke tier fails naming the id present in the manifest and absent from the runner, and vice versa

### Requirement: The declared surface matches a golden snapshot

The smoke tier SHALL bind one fixture per declaration key the controller reads (`static_entity`, `rolling_window_twin`, `tier_store`, `checkpoint_batched_store`, `candle`, `commit_block_workflow`, `saga_block_workflow`, `store_input`), capture the `AddOperativeResponse.metadata` map as decoded at `ClassRegistry.decode_declarations/1`, and assert it equals the committed golden file `test/fixtures/declarations/<fixture>.json` — key set and values. It SHALL further bind one keyed fixture, deliver one input, and assert the set of identity-stamp keys on the sink output (`flz.entity`, `flz.seq`, `flz.key`) equals the committed golden. Each snapshot test SHALL carry the capability of the declaration it pins. Goldens SHALL be regenerated only with `FLEZHA_E2E_SNAPSHOT=update` under the reference runtime; any other runtime SHALL refuse the update.

#### Scenario: A differing default is a diff
- **WHEN** an SDK's `tier_store` declares `effectively_once` with a value or key spelling that differs from `test/fixtures/declarations/tier_store.json`
- **THEN** the snapshot test fails for that runtime showing the differing key

#### Scenario: A key one SDK adds is a diff on every other
- **WHEN** the reference's `checkpoint_batched_store` gains a declaration key and the golden is regenerated
- **THEN** the next Java run fails the same snapshot until the Java builder emits the key

#### Scenario: A non-reference runtime cannot rewrite a golden
- **WHEN** `FLEZHA_E2E_SNAPSHOT=update` is set and `FLEZHA_E2E_RUNTIME` is not `rust`
- **THEN** the snapshot tests fail naming the reference runtime, and no golden file changes

### Requirement: Every SDK test app honours the test-app contract

A test app SHALL take its namespace from the first command-line argument; SHALL exit when its standard input reaches end-of-file; SHALL honour the harness-injected environment (`FLEZHA_SOCKET_FILES_PATH`, `FLEZHA_TRANSPORT`, `FLEZHA_CA_CERT_PATH`, `FLEZHA_QUIC_ADDRESS`, and whatever its own manifest template renders); SHALL, in every workflow fixture that participates in a crash-resume scenario, park a pool worker — never the driver thread — until `FLEZHA_WORKFLOW_E2E_RELEASE` is present in the environment; and SHALL cap its own logging at error level unless overridden. Its manifest's `build` commands SHALL be the only commands needed to produce its artifacts.

#### Scenario: Closing stdin terminates the runner
- **WHEN** the harness closes the port it launched a test app through
- **THEN** the app's OS process exits and the controller observes the runner disconnect

#### Scenario: The release gate holds a workflow mid-run
- **WHEN** a crash-resume scenario starts a workflow fixture without `FLEZHA_WORKFLOW_E2E_RELEASE` set
- **THEN** the fixture's blocking procedure does not complete until the runner is respawned with the variable set, and the runner keeps answering control pings meanwhile

### Requirement: The suite has a smoke tier and a full tier

`test/system/sdk_smoke_e2e_test.exs` (tags `:system`, `:sdk_smoke`, capability `runner_lifecycle`) SHALL exercise, for the selected runtime across every transport its manifest lists: handshake and `AssignRunnerId`, `ConnectSocket`, add/remove operative lifecycle, end-to-end data flow, `PipeToOperative` routing, in-runtime migration, reconnect under controller restart, the live-factory-set assertion and the golden snapshots. `FLEZHA_E2E_RUNTIME=<sdk> mix test --include sdk_smoke` SHALL complete without any other tag. `--include system` SHALL run the full suite including the smoke tier.

#### Scenario: The smoke tier is the inner loop for a new SDK
- **WHEN** `FLEZHA_E2E_RUNTIME=<sdk> mix test --include sdk_smoke` is run against an SDK whose manifest defers every capability but `runner_lifecycle`
- **THEN** the smoke tier runs to completion and every other `:system` module is skipped by name

### Requirement: CI runs two runtimes nightly and any runtime on demand

`nightly-e2e.yml` SHALL run the `:system` and `:multi_node` suites as a matrix over `runtime: [rust, java]` with `fail-fast: false`, one job per runtime, `FLEZHA_E2E_RUNTIME` set from the matrix. It SHALL accept a `workflow_dispatch` input `runtime` with values `rust`, `java`, `elixir`, `all` (default `all`) that selects the matrix. Per-pull-request workflows SHALL NOT spawn runners for more than one runtime; the static gate SHALL be their only addition. `java-sdk.yml` SHALL run `FLEZHA_E2E_RUNTIME=java mix test --include sdk_smoke` in place of the removed `mix test.java_sdk`.

#### Scenario: A nightly Java failure is attributed
- **WHEN** the nightly `java` job fails and the `rust` job passes
- **THEN** the run shows one red job named by runtime and the Rust job's result is unaffected

#### Scenario: Any runtime on demand
- **WHEN** the workflow is dispatched with `runtime: elixir`
- **THEN** one job runs the suite with `FLEZHA_E2E_RUNTIME=elixir` after pre-building `test/apps/elixir_runner`

### Requirement: Every run produces a conformance report

When `FLEZHA_E2E_RUNTIME` is set or any `:system` test ran, the suite's formatter SHALL print, at suite end, the runtime and a table of every vocabulary capability with its outcome — `pass`, `fail`, `deferred`, `transport-skipped` or `untested` — and SHALL write the same table as JSON to the path in `CONFORMANCE_OUT` when set. The nightly SHALL upload that file per runtime. `docs/architecture/runner.md` SHALL NOT carry a hand-maintained per-SDK capability table; it SHALL point at the manifests and the report.

#### Scenario: The report answers "is this SDK working"
- **WHEN** `FLEZHA_E2E_RUNTIME=java CONFORMANCE_OUT=java.json mix test --include system` completes
- **THEN** the console ends with a capability table for `java` and `java.json` contains one entry per vocabulary capability with its outcome

### Requirement: A reference fixture proves schema evolution per runtime

The conformance suite SHALL carry a reference fixture that persists per-Entity
state under one schema version and restores it under the next, and every runtime
that persists state SHALL advertise it. A runtime that persists nothing SHALL
declare the fixture unsupported rather than silently omitting it.

The fixture SHALL assert both directions of the guard: that a declared migration
restores the accumulated value, and that a version mismatch with no declared
migration refuses instead of producing state.

#### Scenario: the evolution fixture runs on every persisting runtime

- **GIVEN** the conformance gate running against a runtime that advertises per-Entity state persistence
- **WHEN** the fixture set is resolved for that runtime
- **THEN** the schema-evolution fixture SHALL be among the fixtures the runtime advertises, and the gate SHALL fail if it is absent

#### Scenario: a mismatch with no migration refuses

- **GIVEN** the fixture's Procedure at the next schema version with no migration declared
- **WHEN** it restores an Entity persisted at the previous version
- **THEN** the restore SHALL fail with a schema error and the Entity SHALL NOT resume with cold state presented as restored

### Requirement: The wedge scenario proves detection and recovery on every SDK

The suite SHALL carry a scenario, driven by each SDK's `pool_wedge` fixture, that occupies the runner's dispatch capacity and asserts the full chain on the runtime under test: the runner's heartbeat reports handout liveness; the classifier mints the expected verdict; the planner issues the expected remedy; and the affected operative resumes producing afterwards.

The scenario SHALL additionally cover the abandon path — a fixture whose Procedure aborts its worker rather than returning an error — and assert that the entity is served again without operator action and without a duplicate execution of the item that killed the worker.

#### Scenario: A wedged pool is detected and remedied on each runtime
- **WHEN** the wedge fixture occupies the runner's dispatch capacity under `FLEZHA_E2E_RUNTIME=<sdk>`
- **THEN** the heartbeat SHALL report the outstanding activation, the classifier SHALL mint the wedge verdict, the plan SHALL issue its remedy, and the operative SHALL produce again once the remedy lands

#### Scenario: A worker killed mid-dispatch recovers without losing the entity
- **WHEN** the fixture aborts the worker holding an activation
- **THEN** the pool SHALL return to its configured size, the entity SHALL be served again, and the aborting item SHALL NOT have been executed twice

### Requirement: Every conformance manifest declares its runtime's language byte

Each SDK test app's `conformance.json` SHALL declare `sdk_lang`: the allocated language byte that runtime stamps at byte 0 of its durable records and advertises on every factory. The gate SHALL assert that:

- every manifest declares one, and it is a value from the allocation table (never `0`);
- no two manifests declare the same value;
- the declared value equals what its SDK actually stamps — the gate is static (it spawns no runner), so it SHALL compare each manifest against the constant that SDK compiles in, and fail naming both. That a runner then *advertises* that value on connect is proved by the `interop` scenario below.

An SDK that persists nothing SHALL still declare a value: the byte identifies the runtime at the placement seam, not only in a record header.

#### Scenario: A new SDK cannot join untagged
- **WHEN** a new `test/apps/<sdk>_runner/conformance.json` omits `sdk_lang`
- **THEN** the gate SHALL fail naming the manifest and the allocation table

#### Scenario: A duplicated language byte fails the gate
- **WHEN** two manifests declare the same `sdk_lang`
- **THEN** the gate SHALL fail naming both runtimes and both manifests

#### Scenario: A manifest that disagrees with its SDK fails
- **GIVEN** a manifest declaring `sdk_lang: 2` for an SDK whose source stamps `1`
- **WHEN** the gate runs
- **THEN** it SHALL fail naming the declared and the stamped value, and the file each came from

#### Scenario: A persistence-free SDK still declares one
- **GIVEN** a runtime whose manifest defers `state_persistence`
- **WHEN** the gate runs
- **THEN** the manifest SHALL still be required to declare a distinct `sdk_lang`

### Requirement: An interop scenario proves the placement guard

The suite SHALL carry an `interop`-tagged scenario that starts two SDK runners of different languages in one namespace, both advertising a factory id the reference manifest gives to both, and asserts that placement never crosses languages: an operative of that factory established at one language SHALL be placed only on the runner of that language, and killing that runner SHALL leave the operative unbound with a language reason rather than rebinding onto the other runner.

The scenario SHALL be the only place two SDKs coexist in a run; the one-runtime-per-run rule stands everywhere else, and the shared factory ids that exist for cross-runtime measurement SHALL NOT be changed to avoid the collision — the guard is what makes them safe.

#### Scenario: Placement does not cross languages
- **GIVEN** a Rust runner and a Java runner in one namespace, both advertising `bench_echo`, with `bench_echo` established at the Rust language
- **WHEN** an operative of `bench_echo` is started
- **THEN** it SHALL be placed on the Rust runner

#### Scenario: A dead runner does not hand its factory to the other language
- **GIVEN** the operative above running on the Rust runner
- **WHEN** the Rust runner is killed while the Java runner stays connected
- **THEN** the operative SHALL NOT bind to the Java runner, and the refusal reason SHALL name the language mismatch

