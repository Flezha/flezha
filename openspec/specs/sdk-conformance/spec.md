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

Each SDK's test app directory `test/apps/<sdk>_runner/` SHALL contain `conformance.json` declaring: `runtime` (the name `FLEZHA_E2E_RUNTIME` selects it by); `command` (a launch template over `{artifact}`, `{app_dir}`, `{repo}`, `{namespace}` and the absolute-path placeholders `{socket_dir}`, `{ca_cert}`, `{client_cert}`, `{client_key}`); `artifacts` with a `test` and a `bench` profile, each an ordered list of paths relative to the app directory; `build` with the one command that produces each profile's artifact; `connect_timeout_ms`; `transports` (the subset of `uds`, `quic` the SDK's standalone runner supports); `dispatch` (how the SDK's host executes activations — see below); `toolchain` (the shell command that prints its toolchain's version); `deferred` (capabilities from the shared vocabulary the SDK does not carry); `fixtures` (a map from capability to the factory ids the app registers for it); and `docs` (the path of the page that documents the SDK's scope). Every vocabulary capability SHALL appear in a manifest exactly once — under `deferred` or as a `fixtures` key — so a capability can be neither forgotten nor both; the required capabilities (`runner_lifecycle`, `connect_discovery`, `graph_builder`, `liveness`, `retry_ladder`, `partitioned`, `pool_wedge`, `state_persistence`, `keyed_channels`, `dynamic_cell`, `schema_evolution`, `cell_timers`, `effectively_once`, `correction_window` — the surface every SDK carries) SHALL NOT appear under `deferred`, and the required set SHALL contain every capability that no manifest defers. The page named by `docs` SHALL exist and SHALL reference the manifest by path rather than restate its scope. The Rust test app's manifest SHALL be the reference: a fixture id exists for a capability if and only if the reference lists it there. Both spawners — the test harness (`test` profile) and the bench fleet (`bench` profile) — SHALL render the launch line from the manifest and SHALL carry no per-runtime launch clause of their own. The first artifact in a profile's list that exists SHALL be used; using a later one SHALL log a warning naming the first; none existing SHALL raise with the profile's `build` command.

`pool_wedge` becomes required because a host that cannot be wedged on demand cannot prove it recovers: the fixture is what exercises the abandon path, the worker replacement and the classifier's verdict on that SDK rather than on Rust alone.

`state_persistence` joins the required set the same way: the record stream, the restore fold, the compaction snapshot, the live read and the credit window live once in the shared core for six SDKs and are re-expressed in the Elixir SDK's own core against the kernel's rules, so the last deferrer carries it and the gate's inverse direction promotes it. `keyed_channels`, `dynamic_cell` and `schema_evolution` join it the same way: the identity stamps, the idle reaper with its cap and trace, and the per-Tasklet schema declaration with its migrations live once in the shared core for six SDKs and are re-expressed in the Elixir SDK's own core, so the last deferrer carries them and the gate's inverse direction promotes them. `cell_timers` joins the same way: the pending set, its ops as records in the Cell's own log, the two firing paths and the clock kinds live once in the shared core for six SDKs and are re-expressed in the Elixir SDK's own core. `effectively_once` joins the same way: the staged interval, the effect record, the checkpoint close, the tier-on park and the deterministic-effects contract live once in the shared core for six SDKs and are re-expressed in the Elixir SDK's own core. `correction_window` joins the same way, and it is the smallest of the five because the controller half is SDK-blind and already complete: the lateness classification against the Cell's event-time high-water, the correction request, the replay-slice buffer and its event-time ordering live once in the shared core for six SDKs and are re-expressed in the Elixir SDK's own core, so the last deferrer carries it and the gate's inverse direction promotes it.

The required set SHALL be a declaration a person writes, never a value computed from the manifests, and the gate SHALL enforce the two directions separately because their remedies differ: a manifest that defers a required capability SHALL fail naming that manifest and the required set, and a capability no manifest defers that is absent from the required set SHALL fail naming the capability and the declaration it belongs in. A capability therefore cannot leave the required set as a side effect of a manifest edit — an SDK that genuinely cannot carry one defers it by name and removes it from the required set in the same change, and both halves are read by a reviewer.

`dispatch` SHALL declare `model` — a slug from a closed vocabulary held in one place in the harness (`thread-pool`, `event-loop`, `process-per-cell`) — `detail`, one sentence describing how activations are executed, and `env`, a map from each environment variable that selects the SDK's dispatch topology to a description of what it selects. A runtime whose topology is fixed SHALL declare an empty `env`. Every SDK's manifest SHALL carry it, because a throughput number taken under one dispatch topology and one taken under another are different measurements of the same fixture, and only the manifest knows which. `toolchain` SHALL be the shell command that prints the version of the toolchain that runs the SDK, so a bench header can name it. A command that does not run SHALL report the toolchain as unavailable rather than fail the caller.

A consumer that reports a runtime's dispatch topology or toolchain version SHALL read it from the manifest and SHALL carry no per-runtime clause of its own — the same rule the launch line already follows. A per-runtime clause is a list a new SDK has to remember to join; the manifest is where a runtime already says what it is.

#### Scenario: Adding an SDK touches no harness code
- **WHEN** a new `test/apps/<sdk>_runner/` directory with a valid manifest is added and `FLEZHA_E2E_RUNTIME=<sdk> mix test --include sdk_smoke` is run
- **THEN** the harness launches the new app from its manifest without any change to `test/helpers/`, `test/system/` or `test/test_helper.exs`

#### Scenario: A missing artifact names its build command
- **WHEN** no path in the selected profile's `artifacts` exists
- **THEN** the spawn raises a message containing that profile's `build` command

#### Scenario: A capability left out of a manifest fails the gate
- **WHEN** the vocabulary gains a capability and a manifest lists it neither under `deferred` nor under `fixtures`
- **THEN** the static gate fails naming the manifest and the capability

#### Scenario: A required capability cannot be deferred
- **WHEN** a manifest lists `retry_ladder` under `deferred`
- **THEN** the static gate fails naming the manifest and the required set

#### Scenario: pool_wedge cannot be deferred either
- **WHEN** a manifest lists `pool_wedge` under `deferred`
- **THEN** the static gate fails naming the manifest and the required set

#### Scenario: A capability every manifest carries joins the required set
- **WHEN** the last manifest that deferred `state_persistence` starts carrying it, and the required set is not updated in the same change
- **THEN** the static gate fails naming `state_persistence` and the required-set declaration it is missing from

#### Scenario: state_persistence is required once every manifest carries it
- **WHEN** the Elixir manifest — the last to defer `state_persistence` — lists it under `fixtures` with the reference's six ids
- **THEN** the static gate fails until `state_persistence` is added to the required-set declaration in the same change, and a later manifest that lists `state_persistence` under `deferred` fails the gate naming that manifest and the required set

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

#### Scenario: keyed_channels, dynamic_cell and schema_evolution are required once every manifest carries them
- **WHEN** the Elixir manifest — the last to defer each — lists `keyed_channels: ["partition_relay"]`, `dynamic_cell: ["dynamic_cell"]` and `schema_evolution: ["schema_evolution_v1", "schema_evolution_v2"]` under `fixtures`
- **THEN** the static gate fails three times, once per capability, until each is added to the required-set declaration in the same change, and a later manifest that lists any of the three under `deferred` fails the gate naming that manifest and the required set

#### Scenario: cell_timers is required once every manifest carries it
- **WHEN** the Elixir manifest — the last to defer it — lists `cell_timers: ["session_timeout"]` under `fixtures`
- **THEN** the static gate fails until `cell_timers` is added to the required-set declaration in the same change, and a later manifest that lists `cell_timers` under `deferred` fails the gate naming that manifest and the required set

#### Scenario: effectively_once is required once every manifest carries it
- **WHEN** the Elixir manifest — the last to defer `effectively_once` — lists it under `fixtures` with the reference's four ids
- **THEN** the static gate fails until `effectively_once` is added to the required-set declaration in the same change, and a later manifest that lists `effectively_once` under `deferred` fails the gate naming that manifest and the required set

#### Scenario: correction_window is required once every manifest carries it
- **WHEN** the Elixir manifest — the last to defer `correction_window` — lists `correction_window: ["candle"]` under `fixtures`
- **THEN** the static gate fails until `correction_window` is added to the required-set declaration in the same change, and a later manifest that lists `correction_window` under `deferred` fails the gate naming that manifest and the required set

### Requirement: A scenario has one fixture id in every SDK

A fixture that implements the same scenario SHALL register the same factory id in every SDK test app, and its sink channel names, output bytes and declared options SHALL match the reference. No test app SHALL register a fixture id prefixed with its SDK's name, and no test under `test/system/` SHALL select a fixture by runtime. A test app SHALL NOT register a fixture id absent from the reference.

#### Scenario: A generic test drives the same fixture on every SDK
- **WHEN** `cell_timers_e2e_test.exs` starts the `session_timeout` fixture under `FLEZHA_E2E_RUNTIME=rust` and again under `FLEZHA_E2E_RUNTIME=java`
- **THEN** both runs bind a fixture registered as `session_timeout` and both pass the same assertions

#### Scenario: A prefixed id fails the gate
- **WHEN** a manifest lists a fixture id such as `java_session_timeout` that the reference does not list
- **THEN** the static gate fails naming the manifest, the capability and the id

### Requirement: Scenarios are tagged by capability; deferred capabilities and unsupported transports skip by name

The capability vocabulary SHALL be one list in the harness (`runner_lifecycle`, `connect_discovery`, `graph_builder`, `liveness`, `pool_wedge`, `retry_ladder`, `state_persistence`, `effectively_once`, `cell_timers`, `correction_window`, `keyed_channels`, `bounded`, `dynamic_cell`, `partitioned`, `schema_evolution`, `shorthand`). A capability MAY carry no fixtures in any manifest, when the scenarios that prove it need no fixture of their own; whether a capability may be deferred is a separate question from whether it carries fixtures, and `pool_wedge` is both required and fixture-carrying — a host that cannot be wedged on demand cannot prove it recovers. Every module under `test/system/` SHALL carry `@moduletag capability: <atom>` from the vocabulary; a test MAY override it with `@tag capability:`. For the selected runtime, `test_helper.exs` SHALL exclude every test whose capability the runtime's manifest defers and every test tagged with a transport the manifest does not list, and SHALL print one line naming the runtime, the manifest path, the deferred capabilities and the skipped transports. A capability the manifest does not defer SHALL run and SHALL pass; there is no third state. A capability no module or test under `test/system/` is tagged for SHALL be deferred by every manifest, so that a capability carried under `fixtures` is one at least one scenario proves: claiming a capability and covering it cannot diverge, and an SDK that carries the mechanism proves it rather than resting on the SDK's own unit tests.

#### Scenario: A deferred capability skips visibly
- **WHEN** `FLEZHA_E2E_RUNTIME=<sdk> mix test --include system` is run and that SDK's manifest defers `bounded`
- **THEN** no test tagged `capability: :bounded` runs, and the run's output contains a line naming `<sdk>`, `test/apps/<sdk>_runner/conformance.json` and `bounded`

#### Scenario: A capability that is neither deferred nor passing fails the run
- **WHEN** a runtime's manifest does not defer `retry_ladder` and its runner does not park a poison payload
- **THEN** `dlq_e2e_test.exs` fails for that runtime; nothing in the harness converts the failure into a skip

#### Scenario: An unsupported transport skips by name
- **WHEN** a manifest lists `transports: ["uds"]` and a test is tagged `transport: :quic`
- **THEN** that test is excluded and the printed line names `quic` as skipped

#### Scenario: A capability carried by a manifest is covered by a scenario
- **WHEN** a manifest carries `retry_ladder` under `fixtures`
- **THEN** at least one module or test under `test/system/` is tagged `capability: :retry_ladder`, and a manifest that carries a capability no scenario is tagged for is a defect in that manifest or a missing scenario, never a silent gap

#### Scenario: An SDK without the mechanism defers rather than claiming
- **WHEN** an SDK carries the shared core's mechanism for a capability but not the runner-handle surface its scenarios drive
- **THEN** its manifest lists that capability under `deferred`, naming the follow-up that owes it, rather than under `fixtures` with an empty fixture list

### Requirement: A static drift gate runs in the default test suite

An untagged ExUnit module SHALL run under plain `mix test` — on every pull request, with no runner spawned — and SHALL assert: every manifest under `test/apps/*/conformance.json` parses and declares every field of the manifest requirement; every capability named under `deferred` or `fixtures` is in the vocabulary; for every capability an SDK does not defer, its fixture set equals the reference's; every module under `test/system/` carries a vocabulary capability tag; every string literal in a `test/system/` file that equals a reference fixture id belongs to a capability that file is tagged with; no `runtime:` literal appears in `test/system/` outside a module or test tagged `interop: true`; every directory matching `test/apps/*_runner/` carries a manifest; every manifest's `runtime` is an option of `nightly-e2e.yml`'s `workflow_dispatch` `runtime` input; every manifest's `docs` page exists and contains the manifest's path; and no `@moduledoc` of a bench task whose `--runtime` resolves against the manifests names two or more of those runtimes on one line. The gate SHALL fail on every vocabulary capability that no module or test under `test/system/` is tagged for, naming the capability: a capability every manifest defers is still proved by the scenario that would run were it carried, and a capability a manifest carries under `fixtures` cannot be one nothing exercises. Every gate failure message SHALL end with the path of the "Adding an SDK to the conformance suite" documentation section.

#### Scenario: A fixture added to one SDK and not the others fails the PR
- **WHEN** the Rust manifest gains `new_probe` under `liveness` and the Java manifest, which does not defer `liveness`, does not
- **THEN** `mix test` fails in the gate naming `java`, `liveness` and `new_probe`

#### Scenario: An untagged e2e module fails the PR
- **WHEN** a module is added under `test/system/` without a `capability:` tag
- **THEN** `mix test` fails in the gate naming the file

#### Scenario: A stray runtime literal fails the PR
- **WHEN** a test under `test/system/` passes `runtime: :java` without an `interop: true` tag
- **THEN** `mix test` fails in the gate naming the file and the line

#### Scenario: An untested capability fails the PR
- **WHEN** no module under `test/system/` is tagged `capability: :keyed_channels`
- **THEN** `mix test` fails in the gate naming `keyed_channels`

#### Scenario: A documented runtime list fails the PR
- **WHEN** `mix bench.roundtrip`'s `@moduledoc` advertises `--runtime rust|java|elixir` while `--runtime` accepts every runtime with a manifest
- **THEN** `mix test` fails in the gate naming the task, the runtimes the line lists and the set the manifests hold

### Requirement: Rolling out a new SDK is enforced by the gate, not remembered

The first pull request that adds a directory `test/apps/<sdk>_runner/` SHALL NOT pass `mix test` until, in that same pull request: the directory carries a manifest that accounts for every vocabulary capability and defers none of the required set; the manifest's non-deferred fixture sets equal the reference's; the manifest's `runtime` is a `workflow_dispatch` option of `nightly-e2e.yml`; and the manifest's `docs` page exists and points at the manifest. The suite SHALL then admit the SDK with no change to `test/helpers/`, `test/system/`, `test/test_helper.exs`, the formatter or the gate. The reverse direction SHALL hold with the same mechanism: a change that adds a vocabulary capability, a reference fixture, a required capability or a declaration snapshot SHALL fail the gate or the smoke tier for every existing SDK that neither carries the addition nor defers it by name in its manifest, so a lockstep omission is a red PR, not a backlog item. The documentation section "Adding an SDK to the conformance suite" SHALL list exactly the items the gate enforces, followed by the items it cannot (the SDK's own unit-test workflow, `docs/sdk/naming_conventions.md` compliance, the dev note and its `CLAUDE.md` index line), and SHALL be the path every gate failure names.

Where this requirement names a document, it SHALL name the path that document occupies. A specification naming a file the repository does not carry is read and acted on by whoever is following it, and cannot be caught by compilation, by the suite, or by the naming gates, which police retired identifiers rather than paths.

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

#### Scenario: The requirement's own documentation path resolves
- **WHEN** the paths this requirement names are resolved against the repository
- **THEN** each SHALL exist, and the naming-conventions page SHALL be the one `docs/sdk/conformance.md`'s "Adding an SDK to the conformance suite" section links

### Requirement: The live factory set matches the manifest

The smoke tier SHALL assert that the factory ids the controller lists for a freshly connected runner (`FleetManager.list_factories/1`) equal the union of the runtime's manifest `fixtures`, and that the kind the controller recorded for each id is `bounded` for every id the manifest lists under a bounded capability (`bounded` and the bounded half of `shorthand`, `sugar_bounded`) and `unbounded` for every other id.

#### Scenario: The manifest and the app disagree
- **WHEN** a manifest lists `session_timeout` under `cell_timers` and the app registers `java_session_timeout`
- **THEN** the smoke tier fails naming the id present in the manifest and absent from the runner, and vice versa

#### Scenario: A runtime announcing the wrong kind fails the smoke tier
- **WHEN** a runtime announces `commit_block_bounded` as an unbounded operator, or announces any factory with an empty kind
- **THEN** the smoke tier fails naming the runtime, the id and the kind it expected

### Requirement: The declared surface matches a golden snapshot

`test/system/topology_refusal_e2e_test.exs` SHALL bind every fixture that has a golden under `test/fixtures/declarations/`, in every runtime whose manifest lists it (the selected runtime in the ordinary `:system` pass, every other runtime under `:interop`), capture the `AddOperatorResponse.metadata` map as decoded at `ClassRegistry.decode_declarations/1`, and assert it equals the committed golden file `test/fixtures/declarations/<fixture>.json` — key set and values. The smoke tier SHALL bind one keyed fixture, deliver one input, and assert the set of identity-stamp keys on the sink output (`flz.cell`, `flz.seq`, `flz.key`) equals the committed golden. Each snapshot test SHALL carry the capability of the declaration it pins. Goldens SHALL be regenerated only with `FLEZHA_E2E_SNAPSHOT=update` under the reference runtime; any other runtime SHALL refuse the update.

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

A test app SHALL take its namespace from the first command-line argument; SHALL end its own OS process when the port the harness manages it by goes away, whether that reaches it as end-of-file on standard input or as a standard-input read that can no longer answer one — a read error is the close arriving by another road, never a reason to stop watching; SHALL honour the harness-injected environment (`FLEZHA_SOCKET_FILES_PATH`, `FLEZHA_TRANSPORT`, `FLEZHA_CA_CERT_PATH`, `FLEZHA_QUIC_ADDRESSES`, and whatever its own manifest template renders); SHALL, in every bounded fixture that participates in a crash-resume scenario, suspend the parked activation **without occupying the host's dispatch capacity** until `FLEZHA_BOUNDED_E2E_RELEASE` is present in the environment; and SHALL cap its own logging at error level unless overridden. Its manifest's `build` commands SHALL be the only commands needed to produce its artifacts.

The parked activation's obligation is the same on every host and its spelling is not: a host whose manifest declares a worker-pool dispatch model SHALL park a pool worker and SHALL NOT park the driver thread; a host whose manifest declares an event-loop dispatch model SHALL suspend the activation and yield the loop; and a host whose manifest declares a process-per-cell dispatch model SHALL block the parked Cell's own process, which occupies no shared capacity because the host schedules one process per Cell and the driver is a process of its own. A synchronous hold on an event-loop host is a violation of this requirement, not an implementation of it: it freezes every other activation the loop owns, which is the wedge signature a different capability's fixture exists to produce. In every case the runner SHALL keep answering control pings while a fixture is parked, and a sibling tasklet the driver has not yet reached SHALL remain dispatchable.

An app whose runtime reads standard input through a device it also writes standard output through — the Elixir runner's `user` process is the only one — SHALL address its watch at that device rather than at a relay in front of it, so the device's death is reported to the read instead of leaving it unanswered, and SHALL treat that death as the close. It SHALL do so only while the runtime reports itself started: during start-up and during shutdown the device dies as part of that, and ending the process there would race the shutdown and replace the report and exit status the runtime had chosen with its own.

A test app SHALL NOT read `FLEZHA_QUIC_ADDRESS`; the harness renders `FLEZHA_QUIC_ADDRESSES` and an app honouring the retired name is a gate failure.

#### Scenario: Closing stdin terminates the runner
- **WHEN** the harness closes the port it launched a test app through
- **THEN** the app's OS process exits and the controller observes the runner disconnect

#### Scenario: A broken standard output ends the app just the same
- **WHEN** the close breaks a running app's standard output first, killing the device its standard-input read is waiting on, so no end-of-file ever arrives
- **THEN** the app's OS process still exits, and none survives the harness by the time it checks

#### Scenario: A device that dies with the runtime leaves the exit status alone
- **WHEN** the same read error reaches an app whose runtime is already starting or stopping
- **THEN** the app does not end the process itself, and the runtime's own exit status and report stand

#### Scenario: The release gate holds a bounded operator mid-run
- **WHEN** a crash-resume scenario starts a bounded fixture without `FLEZHA_BOUNDED_E2E_RELEASE` set
- **THEN** the fixture's blocking tasklet does not complete until the runner is respawned with the variable set, and the runner keeps answering control pings meanwhile

#### Scenario: An event-loop host parks without holding its loop
- **WHEN** a test app whose manifest declares an event-loop dispatch model reaches the release gate in a bounded fixture and the variable is absent
- **THEN** the activation suspends and the loop is yielded, so the runner answers control pings and dispatches other activations, and an app that instead holds the loop synchronously fails this requirement

#### Scenario: A host with a selectable dispatch model owes the obligation under every member
- **WHEN** a test app's manifest names a `dispatch.env` variable that selects between dispatch models — `FLEZHA_PYTHON_EXECUTOR` and `FLEZHA_RUBY_EXECUTOR` are the two — and a bounded fixture reaches the release gate under any value that variable takes
- **THEN** the same fixture spelling satisfies the parking obligation for the model actually in force, parking one pool worker under a pool member and yielding under a loop or fiber member, and a spelling that satisfies only the model the manifest names by default fails this requirement

#### Scenario: A thread-pool host parks a worker rather than a timer
- **WHEN** a test app whose manifest declares the `thread-pool` dispatch model reaches the release gate in a bounded fixture and the variable is absent
- **THEN** the calling pool worker parks off the processor with no timer and no spin, the runner keeps answering control pings from the threads the core owns, and the pool grows on the core's starvation signal so a sibling the driver has not yet reached is dispatched

#### Scenario: The retired QUIC variable is not honoured
- **WHEN** a test app's connect path is checked against the pinned environment fixture
- **THEN** an app reading `FLEZHA_QUIC_ADDRESS` fails the gate naming the app and the replacement

#### Scenario: A process-per-cell host parks the Cell's own process

- **WHEN** a test app whose manifest declares the `process-per-cell` dispatch model reaches the release gate in a bounded fixture and the variable is absent
- **THEN** the parked Cell's own process blocks with no timer and no spin, and the runner keeps answering control pings while its driver dispatches a sibling tasklet the parked one does not feed — a host that instead parked the driver would stall every stage of the chain

### Requirement: The suite has a smoke tier and a full tier

`test/system/sdk_smoke_e2e_test.exs` (tags `:system`, `:sdk_smoke`, capability `runner_lifecycle`) SHALL exercise, for the selected runtime across every transport its manifest lists: handshake and `AssignRunnerId`, `ConnectSocket`, add/remove operator lifecycle, end-to-end data flow, `PipeToOperator` routing, in-runtime migration, reconnect under controller restart, the live-factory-set assertion and the golden snapshots. `FLEZHA_E2E_RUNTIME=<sdk> mix test --include sdk_smoke` SHALL complete without any other tag. `--include system` SHALL run the full suite including the smoke tier.

#### Scenario: The smoke tier is the inner loop for a new SDK
- **WHEN** `FLEZHA_E2E_RUNTIME=<sdk> mix test --include sdk_smoke` is run against an SDK whose manifest defers every capability but `runner_lifecycle`
- **THEN** the smoke tier runs to completion and every other `:system` module is skipped by name

### Requirement: CI runs two runtimes nightly and any runtime on demand

`nightly-e2e.yml` SHALL run the `:system` and `:multi_node` suites as a matrix over `runtime: [rust, java]` with `fail-fast: false`, one job per runtime, `FLEZHA_E2E_RUNTIME` set from the matrix. It SHALL accept a `workflow_dispatch` input `runtime` with values `rust`, `java`, `elixir`, `all` (default `all`) that selects the matrix. Per-pull-request bounded operators SHALL NOT spawn runners for more than one runtime; the static gate SHALL be their only addition. `java-sdk.yml` SHALL run `FLEZHA_E2E_RUNTIME=java mix test --include sdk_smoke` in place of the removed `mix test.java_sdk`.

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

The conformance suite SHALL carry a reference fixture that persists per-Cell
state under one schema version and restores it under the next, and every runtime
that persists state SHALL advertise it. A runtime that persists nothing SHALL
declare the fixture unsupported rather than silently omitting it.

The fixture SHALL assert both directions of the guard: that a declared migration
restores the accumulated value, and that a version mismatch with no declared
migration refuses instead of producing state.

#### Scenario: the evolution fixture runs on every persisting runtime

- **GIVEN** the conformance gate running against a runtime that advertises per-Cell state persistence
- **WHEN** the fixture set is resolved for that runtime
- **THEN** the schema-evolution fixture SHALL be among the fixtures the runtime advertises, and the gate SHALL fail if it is absent

#### Scenario: a mismatch with no migration refuses

- **GIVEN** the fixture's Tasklet at the next schema version with no migration declared
- **WHEN** it restores a Cell persisted at the previous version
- **THEN** the restore SHALL fail with a schema error and the Cell SHALL NOT resume with cold state presented as restored

### Requirement: The wedge scenario proves detection and recovery on every SDK

The suite SHALL carry a scenario, driven by each SDK's `pool_wedge` fixture, that occupies the runner's dispatch capacity and asserts the full chain on the runtime under test: the runner's heartbeat reports handout liveness; the classifier mints the expected verdict; the planner issues the expected remedy; and the affected operator resumes producing afterwards.

The scenario SHALL additionally cover the abandon path — a fixture whose Tasklet aborts its worker rather than returning an error — and assert that the cell is served again without user action and without a duplicate execution of the item that killed the worker.

#### Scenario: A wedged pool is detected and remedied on each runtime
- **WHEN** the wedge fixture occupies the runner's dispatch capacity under `FLEZHA_E2E_RUNTIME=<sdk>`
- **THEN** the heartbeat SHALL report the outstanding activation, the classifier SHALL mint the wedge verdict, the plan SHALL issue its remedy, and the operator SHALL produce again once the remedy lands

#### Scenario: A worker killed mid-dispatch recovers without losing the cell
- **WHEN** the fixture aborts the worker holding an activation
- **THEN** the pool SHALL return to its configured size, the cell SHALL be served again, and the aborting item SHALL NOT have been executed twice

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

The suite SHALL carry an `interop`-tagged scenario that starts two SDK runners of different languages in one namespace, both advertising a factory id the reference manifest gives to both, and asserts that placement never crosses languages: an operator of that factory established at one language SHALL be placed only on the runner of that language, and killing that runner SHALL leave the operator unbound with a language reason rather than rebinding onto the other runner.

The scenario SHALL be the only place two SDKs coexist in a run; the one-runtime-per-run rule stands everywhere else, and the shared factory ids that exist for cross-runtime measurement SHALL NOT be changed to avoid the collision — the guard is what makes them safe.

#### Scenario: Placement does not cross languages
- **GIVEN** a Rust runner and a Java runner in one namespace, both advertising `bench_echo`, with `bench_echo` established at the Rust language
- **WHEN** an operator of `bench_echo` is started
- **THEN** it SHALL be placed on the Rust runner

#### Scenario: A dead runner does not hand its factory to the other language
- **GIVEN** the operator above running on the Rust runner
- **WHEN** the Rust runner is killed while the Java runner stays connected
- **THEN** the operator SHALL NOT bind to the Java runner, and the refusal reason SHALL name the language mismatch

### Requirement: The connect-discovery contract is a capability and a pinned fixture

The capability vocabulary SHALL gain `connect_discovery`, so that every conformance manifest —
those that exist and every one a future SDK adds — accounts for it under `fixtures` or
`deferred`, and the existing rollout gate makes an omission a red pull request rather than a
backlog item. It SHALL be in the required set for as long as no manifest defers it, by the general rule above. The
freedom the required set once reserved for it survives as a deferral rather than as a standing exemption: an
SDK whose transport is not the shared `flz_transport` — the live case is the Elixir SDK's in-node `:native`
mode, which passes BEAM messages inside the controller's own node and walks no socket ladder, and which its
conformance app does not exercise because that manifest declares `transports: ["uds", "quic"]` — SHALL defer
`connect_discovery` by name in its own manifest, and the same change SHALL remove it from the required set.
The gate then names the SDK and the line to edit, and the required set carries no hole for a case no manifest
has.

`test/fixtures/runner_connect_env.txt` SHALL pin the environment-variable contract every SDK's
connect path reads — the socket-directory variable, the QUIC address-list variable, the group
variable and the ladder candidates in order. The static gate SHALL compare each SDK's connect
path against it, in the manner `test/fixtures/host_class_decl_keys.txt` is compared for
declaration keys, so a variable renamed, added or spelled differently in one SDK is a failure on
every other.

#### Scenario: A new SDK cannot omit the capability
- **WHEN** a `test/apps/<sdk>_runner/conformance.json` is added listing `connect_discovery` neither under `fixtures` nor under `deferred`
- **THEN** the static gate fails naming the manifest and `connect_discovery`

#### Scenario: A variable spelled differently in one SDK is caught
- **WHEN** one SDK's connect path reads a socket-directory variable under a name the pinned fixture does not list
- **THEN** the static gate fails naming the SDK and the variable

#### Scenario: The ladder order is pinned, not just its members
- **WHEN** an SDK resolves the ladder candidates in an order other than the pinned one
- **THEN** the gate fails naming the SDK and the expected order

### Requirement: A scenario obtains its runner through the harness

A test under `test/system/` SHALL spawn every runner it drives through the harness's runtime-selecting spawner (`start_runner/2` or `start_runner_port/2` on `NodeTestHelper`), never by opening a port on a hand-written command. A scenario that proves controller mechanics rather than the runner SHALL say so with `reference_runtime: true` and a reason, which is how it asks the harness for the reference; it is not a reason to bypass the harness. The static gate SHALL fail a `test/system/` module that names a test-app path (`test/apps/<sdk>_runner/`) outside an `interop: true` scenario, naming the file — because a hard-coded command is invisible to the runtime-literal check and makes a capability-tagged scenario green on every runtime for the reference's reasons.

#### Scenario: A capability-tagged scenario proves the selected runtime
- **WHEN** `state_freshness_e2e_test.exs` runs under `FLEZHA_E2E_RUNTIME=node` with `state_persistence` carried by the Node manifest
- **THEN** the runner it SIGTERMs is the Node runner, or the module is tagged `reference_runtime: true` with a stated reason and the runner is the reference — and in neither case is a `cargo run` command written in the test

#### Scenario: A hard-coded runner command fails the gate
- **WHEN** a `test/system/` module not tagged `interop: true` contains the literal `test/apps/rust_runner/`
- **THEN** `mix test test/sdk_conformance/gate_test.exs` fails naming that file, in the same test that refuses a `runtime:` literal

### Requirement: Every SDK's version constant agrees with its own package metadata

Each SDK SHALL derive the version it stamps at bytes 1:5 of a durable record from its own package metadata, and a static gate SHALL assert that it does. The gate SHALL spawn no runner: it reads the SDK's source and the SDK's package file and compares them, in the manner the language-byte gate beside it already compares a conformance manifest against the constant its SDK compiles in.

The gate SHALL, for each runtime the manifest set names:

- resolve the version the SDK sends or stamps, from the file that SDK spells it in;
- resolve the version that SDK's package metadata declares — the Cargo manifest, the Maven POM, `package.json`, the gemspec's source of `VERSION`, `pyproject.toml`, the Mix project;
- fail naming both files and both values when they disagree.

An SDK whose reader takes the version directly from its own build system — where a compiler or a runtime reads the package manifest and no second literal exists — SHALL be recorded as having nothing to compare, and the gate SHALL name that SDK and the reason rather than skip it silently. An SDK whose ecosystem publishes no version in its package file SHALL be recorded the same way, and the gate SHALL still assert that its constant exists, parses and is non-zero.

`test/fixtures/host_class_decl_keys.txt` SHALL list the declaration key that carries the version, so an SDK whose builder cannot spell it is a red build for that SDK, and a key the core never learned is refused by the core's own unknown-field rule.

#### Scenario: A constant left behind by a release bump is caught
- **GIVEN** an SDK whose package metadata declares a version its source constant does not
- **WHEN** the gate runs
- **THEN** it SHALL fail naming both files and both values

#### Scenario: An SDK with one source of truth is named, not skipped
- **GIVEN** an SDK whose version reaches the header from its build system with no second literal
- **WHEN** the gate runs
- **THEN** it SHALL record that SDK as having nothing to compare, naming the mechanism, and SHALL NOT report it as checked

#### Scenario: An SDK whose package file carries no version still declares one
- **GIVEN** an SDK whose ecosystem resolves a published version from a tag rather than from a file in the package
- **WHEN** the gate runs
- **THEN** it SHALL assert that SDK's constant exists, parses and is non-zero, and SHALL name it as the one SDK whose constant nothing cross-checks

#### Scenario: A builder that cannot spell the version key fails its own suite
- **WHEN** the declaration key list gains the version key and one SDK's builder does not emit it
- **THEN** that SDK's declaration-key test SHALL fail naming the key, and every other SDK's SHALL stay green

### Requirement: The Elixir SDK carries the bounded kind

The Elixir SDK SHALL accept a class declared with `kind: :bounded` and drive it to
completion, and its conformance manifest SHALL defer no capability. The Elixir SDK is
its own core and inherits nothing from `flz_core`, so it re-expresses the bounded operator
mechanism rather than binding to it; the observable contract is nonetheless identical to
every other SDK's, because the same `test/system/` scenarios prove all of them.

The manifest flip is the last act of the port: a manifest that declares a capability its
fixtures cannot drive is a declaration the gate believes and no scenario checks.

#### Scenario: A bounded class is accepted rather than refused

- **WHEN** the controller sends `AddOperator` with `kind: "bounded"` to an Elixir runner
- **THEN** the runner registers the class and drives it, where today it answers
  `{:error, "bounded kind is not supported by the Elixir stream SDK"}`

#### Scenario: The Elixir manifest defers nothing

- **WHEN** `test/sdk_conformance/gate_test.exs` compares `test/apps/elixir_runner/conformance.json`
  against the reference manifest
- **THEN** its `deferred` list is empty and its fixture ids are the reference's nineteen
  bounded ids in the reference's order, so every `capability: :bounded` scenario runs
  under `FLEZHA_E2E_RUNTIME=elixir` rather than skipping by name

#### Scenario: A fan-out reaches every consumer on the durable path

- **WHEN** an Elixir bounded class declares one producing tasklet whose output channel
  carries two consumers, and `bounded_crash_resume_e2e_test.exs:180` drives it
- **THEN** all five payloads are delivered, and removing either arm of the fan-out makes
  that test fail with three payloads where five are asserted — the negative control every
  other SDK's port ran, which is what distinguishes a fan-out that works from one that is
  merely declared

### Requirement: The reference refusals are driven through every runtime's real boot

One `test/system/` module SHALL drive every refusal row of
`test/fixtures/reference_topologies.txt` through every runtime that carries a manifest:
per runtime and row it SHALL boot the runtime's test app with `FLEZHA_REFUSAL_FIXTURE=<topology id>`,
under which the app registers only that row's topology through its own builder, and SHALL
assert the runner refuses at boot with the row's name as its error code, read from the SDK's one error type and never from its message. A row a runtime's `needs` excludes
SHALL be asserted as a skip by name. Rows SHALL be read from the fixture and runtimes from
the manifests, so a new refusal or a new SDK is covered with no test edit.

A row whose refusal a runtime raises at compile time (Rust, Java, and Elixir for a `use`
declaration) SHALL be answered by that runtime's app printing the refusal name and the
repository-relative path of the compile-fail test that pins it, then exiting non-zero; the
drill SHALL assert the name in the output and that the cited file exists and contains the
name. A row a runtime refuses at run time SHALL boot for real, whichever the language.

#### Scenario: A veneer that accepts what the reference refuses fails the drill

- **WHEN** a runtime's builder registers a topology whose fixture row names a refusal
- **THEN** the drill fails naming the runtime and the row, and the boot's exit status and
  stderr are in the failure

#### Scenario: A named skip is not a silent gap

- **WHEN** the Elixir runtime skips a `merge` row
- **THEN** the drill records the skip with its surface, and a skip whose surface the
  runtime's documented subset carries fails

#### Scenario: A compile-time refusal is cited, and the citation is checked

- **WHEN** the Rust app is booted under `FLEZHA_REFUSAL_FIXTURE=shorthand_with_topology`
- **THEN** it exits non-zero printing `topology.short_form_with_topology` and `sdk/rust/tests/ui/operator_with_hand_written_impl.rs`, the compile-fail fixture whose own text names the refusal because rustc's conflicting-impl error cannot, and the drill fails if that file is absent or does not contain the name

#### Scenario: An Elixir `use` refusal is cited the same way

- **WHEN** the Elixir app is booted under `FLEZHA_REFUSAL_FIXTURE=shorthand_with_topology`
- **THEN** it exits non-zero printing `topology.short_form_with_topology` and the path of the SDK test that compiles that declaration and asserts the `CompileError`, and the drill checks the cited file contains the name

#### Scenario: A runtime refusal boots on every runtime

- **WHEN** any runtime's app is booted under `FLEZHA_REFUSAL_FIXTURE=shorthand_duplicate_id`
- **THEN** it defines the row's unbounded and bounded under one id, dies on the second, and its output contains `topology.duplicate_factory_id`

### Requirement: Every SDK's declaration output is compared against the shared expected set in one run

The declaration goldens under `test/fixtures/declarations/` SHALL be compared for every
runtime with a manifest in one module, not for the one runtime a run selects: per golden
and per runtime the declaration SHALL be decoded at `ClassRegistry.decode_declarations/1`
and SHALL equal the golden byte-for-byte, `sdk_lang` and `sdk_version` excluded. The
derived record of every well-formed row of `reference_topologies.txt` SHALL equal the
row's lines in every SDK's conformance run.

#### Scenario: A declaration only one SDK emits is a diff on the others

- **WHEN** one SDK's builder emits a key or value for a golden fixture that the reference does not
- **THEN** the same run reports the differing key for that runtime, without waiting for a
  nightly under that runtime

#### Scenario: A derived declaration that drifts fails the run

- **WHEN** an SDK's record for a well-formed row derives to a channel order, kind or
  consumer set that differs from the row
- **THEN** that SDK's conformance run fails naming the row and the line

### Requirement: Every runtime SHALL answer a refused write by the frame's declared guarantee class

Every SDK SHALL answer a write the wire refuses by the guarantee class of the
frame, and no SDK SHALL choose that answer at the write site. The classes and
their policies SHALL be the same three in every runtime — a control frame is
refused or counted and never held; a durable record is held in a bounded ring,
drop-oldest counted, and replayed in write order on the next bound stream; a
plugin or bounded output is held already framed so its channel and identity
stamps survive the replay — with the shared core carrying them for the Rust,
Java, Node, Python, Ruby and PHP SDKs and the Elixir SDK carrying its own,
because it inherits nothing from `flz_core`.

A conformance scenario SHALL prove this on the real wire for every runtime the
manifest does not defer: the stream is severed mid-burst, the runner rebinds,
and the durable and output frames written during the outage arrive exactly
once while the control frames are refused and counted.

#### Scenario: A severed stream loses no durable frame on any runtime

- **GIVEN** a runner of the runtime under test producing records and outputs
- **WHEN** its data stream is severed mid-burst and the runner rebinds
- **THEN** every `:durable` and `:output` frame written during the outage SHALL
  arrive after the rebind, and the counters SHALL report what was held and what
  was shed

#### Scenario: A control frame is not replayed after the rebind

- **WHEN** a control frame's write is refused during the outage
- **THEN** it SHALL be counted and SHALL NOT arrive after the rebind, because
  the reconnect re-issues the request it belonged to

#### Scenario: A shed at the bound is counted, never silent

- **WHEN** more frames are held during an outage than the ring's bound allows
- **THEN** the runtime SHALL count the shed where every drop-oldest on this wire
  is counted, and the drill SHALL read that count rather than infer it

### Requirement: A departed wire SHALL be detected by one watcher per runtime, not one per host

Standard-input end-of-file and SIGTERM SHALL be detected once per runtime and
SHALL lead to one graceful exit, rather than being re-implemented by each host
on top of the core that already owns them. For every host built on `flz_core`
the detector and the signal handler SHALL be the core's, reached through the
ABI, and a host SHALL NOT install its own signal trap beside the core's unless
its runtime gives it no choice — the Java SDK's JVM shutdown hook is such a
case and is named as one. The Elixir SDK, which is its own core, SHALL keep its
own terminal-exit path.

A conformance scenario SHALL prove the exit per runtime: the runner ships its
final state, exits with the declared status, and opens no new control
connection on the way out.

#### Scenario: SIGTERM ends every runtime the same way

- **WHEN** a runner of the runtime under test is sent SIGTERM mid-activation
- **THEN** it SHALL ship `RunnerShutdownNotification` for each of its operators,
  exit with the declared status, and open no new control connection

#### Scenario: A closed standard input ends the runner

- **WHEN** the harness closes the runner's standard input
- **THEN** the runner SHALL run the same graceful shutdown SIGTERM runs, within
  the declared grace, whatever its host language

#### Scenario: A host observes the wire's departure from inside a long activation

- **GIVEN** a runner whose activation body is asleep for far longer than the
  shutdown budget
- **WHEN** the wire departs
- **THEN** the runner SHALL still exit, because the detector does not run on the
  thread the activation occupies

### Requirement: Every producer of the data heartbeat SHALL build one declared key set

The data heartbeat's metadata keys SHALL be one contract, and the static drift gate SHALL compare every producer of that contract against it on every default-suite run.

There are two producers: `flz_core`'s heartbeat builder, which serves the Rust,
Java, node, python, ruby and php runtimes through one function, and the Elixir
SDK, whose core is separate and inherits nothing from `flz_core`. The gate
SHALL read the key set from each producer's own sources and compare both with
the controller's mirror, so that a key one producer carries and another omits is
a build failure rather than a silent gap on one runtime.

A key that a producer omits SHALL NOT be substituted with a literal zero. A
reader cannot tell an absent key from a genuinely zero one, so an omission is
reported as an omission and the producer is given the counter.

The gate SHALL read the shared core's builder by a scrape that tolerates the
formatter's line wrapping. A scrape anchored to the opening parenthesis and the
string on one line reads fewer keys than the builder writes, and would pass with
the gap intact.

#### Scenario: A key one producer omits fails the gate

- **GIVEN** the key set the shared core's heartbeat builds
- **WHEN** the Elixir SDK's heartbeat builds a strictly smaller set
- **THEN** the gate SHALL fail and name the missing keys

#### Scenario: A key with no slot fails the gate

- **WHEN** a producer adds a heartbeat key for which the controller declares no liveness slot
- **THEN** the gate SHALL fail and name the key

#### Scenario: The scrape survives a wrapped tuple

- **WHEN** the shared core's builder is reformatted so a key's tuple wraps across lines
- **THEN** the gate SHALL still read that key, and a scrape that reads fewer keys than the builder writes SHALL be treated as a gate defect

### Requirement: Every test tier SHALL be one declared row, and a tier nothing runs SHALL be a gate failure

The suite SHALL declare its test tiers in one table under `test/helpers/`, one
row per tier, and every other reader SHALL derive from that table rather than
restate it. A row SHALL carry the tier's tag, where it runs (`:default`,
`:pull_request`, `:nightly`, `:sdk_workflow` or `:manual`), the workflow that
runs it, the services it needs, the transports it covers, and the anchor in
`docs/sdk/conformance.md` that documents it. `test/test_helper.exs` SHALL derive
its `exclude:` list from the table and SHALL NOT hold a hand-written list.

A gate SHALL fail when a tier tag named in `test/`, `.github/`, `scripts/` or
`docs/sdk/conformance.md` is not a declared row; when a declared row names no
workflow and is not `:manual` with a written reason; or when a row's
documentation anchor does not resolve. A tag that selects nothing anywhere SHALL
be declared as a tier or removed.

A tier's `:default` or `:pull_request` standing SHALL be derived from the
trigger of the workflow the row names, not asserted in the row, so that a tier
believed to block merge cannot silently stop doing so.

The `capability:` vocabulary read by the conformance manifests is a different
vocabulary from the tier tags and SHALL NOT be folded into it.

#### Scenario: The exclude list is derived

- **GIVEN** the tier table
- **WHEN** `mix test` starts
- **THEN** `ExUnit.start` excludes exactly the tags whose rows do not run in the
  default tier, and no tag is listed in `test_helper.exs` itself

#### Scenario: A tier no workflow runs is red

- **GIVEN** a tier row whose `workflow` field is empty and whose `runs_in` is
  not `:manual`
- **WHEN** the conformance gate runs
- **THEN** it fails naming that tier, and the message says that no job runs it

#### Scenario: An undeclared tag is red

- **GIVEN** a test module tagged with a name that is not a declared tier
- **WHEN** the conformance gate runs
- **THEN** it fails naming the tag and the file that spells it

#### Scenario: A tier that is manual says why

- **GIVEN** a tier that requires a service no CI runner provides
- **WHEN** its row declares `runs_in: :manual` with the reason
- **THEN** the gate passes and the reason appears in the generated tier table

#### Scenario: An include still beats an exclude

- **GIVEN** the interop tier, which places two SDK runtimes in one run and must
  not be pulled in by `--include system`
- **WHEN** `mix test --include system` runs
- **THEN** the interop module skips itself, as its row declares, and running it
  requires `--include interop` as well

#### Scenario: Transport coverage is reported, per tier

- **GIVEN** a tier whose modules fan out over the transports its runtime's
  manifest declares
- **WHEN** the conformance report is printed
- **THEN** the report states, per tier, how many of its modules ran over each
  declared transport

### Requirement: An SDK repository runs three tiers and no controller

An SDK developed outside this tree SHALL prove itself with three tiers, none
of which starts a controller: its unit suite, its local-mode tests, and a
smoke tier that starts the fake controller binary, launches the SDK's own test
app with the environment `test/fixtures/runner_connect_env.txt` pins, and
exits with the binary's code. All three SHALL run from the SDK's tree with only
the fixture set, the fake controller and the core source fetched from one core
tag. Every end-to-end scenario SHALL stay in this tree.

#### Scenario: The smoke tier runs with nothing of this tree checked out
- **GIVEN** an SDK tree carrying `contract/`, the fetched fake controller and its wrapper crate pinned to one core tag
- **WHEN** its CI script runs
- **THEN** unit, local mode and smoke SHALL all run and no controller SHALL be built or started

#### Scenario: A runner that cannot connect fails the smoke tier
- **WHEN** the SDK's connect path stops honouring a variable the connect fixture pins
- **THEN** the fake controller SHALL time out awaiting the connect frame and the tier SHALL fail naming it

### Requirement: The fixtures are a named set an SDK vendors

`test/fixtures/contract.txt` SHALL list every cross-language fixture and
`protobufs/runner_commands.proto` with its purpose, and the static gate SHALL
fail when the list and the directory disagree. The release SHALL publish the
set as one archive with its checksum. Each SDK's tests SHALL resolve the fixture
directory once, in this order: the directory `FLEZHA_CONTRACT_DIR` names, else
`test/fixtures` of the tree the package sits in, else `contract/` at the
package root, and SHALL fail rather than skip when none resolves.

#### Scenario: A fixture without a manifest line fails the gate
- **WHEN** a file with the fixture header exists under `test/fixtures/` and `contract.txt` does not list it
- **THEN** the gate SHALL fail naming the file

#### Scenario: The suite runs from a vendored copy
- **WHEN** an SDK suite runs from a tree that carries `contract/` and no `test/fixtures`
- **THEN** every fixture-reading test SHALL run against the vendored copy

#### Scenario: An in-tree copy does not shadow the checkout
- **WHEN** a package inside the core tree also carries a `contract/`
- **THEN** the fixture-reading tests SHALL read the enclosing tree's `test/fixtures`

#### Scenario: No fixtures is a failure
- **WHEN** none of the three locations resolves
- **THEN** the suite SHALL fail naming the three it tried

### Requirement: An extracted SDK is a pinned submodule at its original path

An SDK that has moved to its own repository SHALL be present in this tree as
a git submodule at `sdk/<lang>`, pinned to a commit, with
`test/apps/<lang>_runner` a symlink to the test app inside it. The suite, the
static gate, the build scripts and `PortPilot` SHALL run against it unchanged,
and every end-to-end tier for that runtime SHALL run here against the pinned
commit. Bumping the pin SHALL be the change that brings an SDK change into
end-to-end.

#### Scenario: The suite does not know the SDK moved
- **GIVEN** `sdk/node` is a submodule and `test/apps/node_runner` its symlink
- **WHEN** the gate, plain `mix test` and the Node smoke tier run
- **THEN** they SHALL pass with no code path that distinguishes a submodule from a directory

#### Scenario: An uninitialised submodule is a plain failure
- **WHEN** the submodule is not checked out
- **THEN** the gate SHALL fail on the missing manifest as it would for any absent test app, naming the path

### Requirement: The short form is a required capability every manifest carries and every app spells
`shorthand` SHALL be in the required set. Every manifest SHALL list it under `fixtures` with exactly the reference's ids, `sugar_do_nothing`, `sugar_store_input`, `sugar_named_input`, `sugar_bounded` and `sugar_state_only`, and those ids SHALL NOT appear under `graph_builder` or `bounded`. Every test app SHALL declare each of the five through its SDK's short form, which is **one operator declaration** at the kind-named operator entry point carrying its factory id and a tasklet body (a `sink` on all but `sugar_state_only`, and a text entry codec where the SDK's builder names codecs), and SHALL register each with one call; no app SHALL declare a tasklet beside them, and no app SHALL name a generated artefact in its registration call. `test/system/shorthand_e2e_test.exs` SHALL be tagged `capability: :shorthand` and SHALL drive all five fixtures on the selected runtime. Goldens under `test/fixtures/declarations/` SHALL pin `sugar_do_nothing`, `sugar_named_input`, `sugar_bounded` and `sugar_state_only`.

`sugar_state_only` SHALL be an unbounded short form with the entry channel `ticks`, no `sink`, and a body that records its input in declared state. It has no sink frame to observe, so its proof SHALL be the cell state read back through the live cell-state query rather than a sink assertion.

The refusal table SHALL carry one row per short-form refusal with the step `shorthand(<id>|-, <sink>|-, <input>|-)` (and the `.noout` modifier for a body declaring no output), needing the surface `shorthand`, and `shorthand_typed_output` for the row only an SDK that can read a body's output type at declaration time can write. The rows SHALL be `shorthand_with_topology` and `shorthand_sink_without_output`. The `shorthand_omittable_sink` surface SHALL be removed from the `needs` vocabulary and from every manifest, because a sink is optional in every SDK's short form and no spelling has to omit a key that rides it. Every manifest SHALL list `shorthand` under `topology_surfaces`. A row whose `needs` begins with `shorthand` SHALL be exempt from the pairing with `flz_core`'s issue variants; its name SHALL instead be one the authoring contract lists.

#### Scenario: A manifest carrying the old grouping fails the gate

- **WHEN** a manifest still lists `sugar_do_nothing` under `graph_builder`
- **THEN** the gate fails naming the manifest, the capability and the id

#### Scenario: A manifest missing the sinkless fixture fails the gate

- **WHEN** a manifest lists four `shorthand` fixtures and not `sugar_state_only`
- **THEN** the gate fails naming the manifest and the missing id

#### Scenario: The named entry channel is reached by name on every runtime

- **WHEN** the scenario injects a frame naming channel `prices` on a `sugar_named_input` operator under any runtime
- **THEN** the sink `sugar_named_input_sink` carries the input's bytes

#### Scenario: The default entry channel is reached on every runtime

- **WHEN** the scenario injects a frame on channel `input` of a `sugar_do_nothing` operator
- **THEN** `sugar_do_nothing_echo` carries the input's bytes

#### Scenario: A bounded short form completes on every runtime

- **WHEN** the scenario starts `sugar_bounded` under any runtime and sends it one message on `order`
- **THEN** `sugar_bounded_placed` carries the message's bytes and the instance completes with status `ok`

#### Scenario: A sinkless short form is proven by its state on every runtime

- **WHEN** the scenario injects a frame on channel `ticks` of a `sugar_state_only` operator under any runtime
- **THEN** the operator declares no plugin channel, and the live cell-state query answers a state carrying that input

#### Scenario: A row an untyped SDK cannot write skips by name

- **WHEN** the Ruby app is booted under `FLEZHA_REFUSAL_FIXTURE=shorthand_sink_without_output`
- **THEN** it prints that its builder carries no `shorthand_typed_output` and the drill counts the skip against the Ruby manifest, which does not list that surface

#### Scenario: A retired surface is gone from every manifest

- **WHEN** the seven manifests are read
- **THEN** none lists `shorthand_omittable_sink`, and the `needs` vocabulary does not name it

#### Scenario: A short-form row's name is checked against the contract, not the core

- **WHEN** the refusal table carries a row named `topology.short_form_with_topology` with `needs` `shorthand`
- **THEN** `topology_check_test.exs` passes without an issue variant of that name, and fails if `docs/sdk/authoring_contract.md` §5 does not list the name

### Requirement: Kind-only settings are refusal rows every builder reaches

The refusal table (`test/fixtures/reference_topologies.txt`) SHALL carry the rows `topology.effectively_once_on_bounded`, `topology.correction_window_on_bounded`, `topology.checkpoint_on_bounded` (a bounded class writing `class.checkpoint_every(500)`) and `topology.sync_durability_on_unbounded`, and every runtime SHALL drive them through its real boot as it drives every other row. The `raw_class_flags` surface SHALL leave the `needs` vocabulary and every manifest, because every builder can now write the three bounded rows, which SHALL need the `bounded` surface instead. The one remaining named skip is `topology.sync_durability_on_unbounded` on a runtime whose typed stream builder cannot spell synchronous durability (Rust, Java), recorded as a skip by name as today: the row keeps `needs: sync_durability`, which the five manifests whose unbounded class can spell the class-level setting list and the Rust and Java manifests do not. The `commit_block_chain` twin SHALL declare no tier: its steps SHALL drop `class.effectively_once`, and the `commit_block_bounded` and `saga_block_bounded` goldens under `test/fixtures/declarations/` SHALL carry no `effectively_once` key and no `retry_attempts`, `retry_backoff_ms`, `barrier_retry_attempts` or `wake_after_ms` key. `test/fixtures/host_class_decl_keys.txt` SHALL list no `bounded.retry_attempts`, `bounded.retry_backoff_ms`, `bounded.barrier_retry_attempts` or `bounded.wake_after_ms`.

#### Scenario: A builder that drops an unbounded-only setting fails the drill

- **WHEN** a runtime's builder builds the `eo_bounded` row and its declaration reaches the core without `effectively_once`
- **THEN** the drill fails naming the runtime and the row, because the boot did not refuse

#### Scenario: The checkpoint row is driven on every runtime

- **WHEN** any runtime's app is booted under `FLEZHA_REFUSAL_FIXTURE=checkpoint_bounded`
- **THEN** it exits non-zero and its output contains `topology.checkpoint_on_bounded`

#### Scenario: A retired surface is gone from every manifest

- **WHEN** the seven manifests and the `needs` vocabulary are read
- **THEN** none names `raw_class_flags`, the three bounded rows need `bounded`, and only the Rust and Java manifests lack `sync_durability`, skipping `topology.sync_durability_on_unbounded` by that name

#### Scenario: The wake key is gone from every declaration

- **WHEN** the declaration goldens and `host_class_decl_keys.txt` are read
- **THEN** none carries `wake_after_ms`, and a host runtime that puts it on the wire fails the declaration-key gate naming the key

### Requirement: A keyed bounded tasklet is a fixture every manifest carries

Every SDK test app SHALL register `keyed_block_bounded` and `keyed_saga_bounded`, and every
manifest SHALL list both under `bounded` in the reference's order. `keyed_block_bounded`
SHALL be a durable bounded operator whose entry channel takes a comma-separated key list, whose first
tasklet splits it into one value per key, whose second tasklet is reached by a `key_by` on
that value, declares no static keys, commits one idempotent change per Cell and declares a
compensation hook, and whose third tasklet blocks until `FLEZHA_BOUNDED_E2E_RELEASE` is set.
`keyed_saga_bounded` SHALL be the same chain with a third tasklet that fails. The scenarios
that drive them SHALL live in `test/system/bounded_control_flow_e2e_test.exs` and
`test/system/bounded_crash_resume_e2e_test.exs`, tagged `capability: :bounded` and pinned to
no runtime, and SHALL assert through the controller: the keyed Cells the instance persisted
(its durable Cell set, read while the pass blocks, so a blocking tasklet's runner is never
asked a live question), `Replay.resume_cell_count/2` before the kill, each keyed Cell's WAL record count
after the resume, the identities the keyed stage's `task_committed` entry names, and the
compensation-done markers.

#### Scenario: N keys run N Cells on every runtime

- **WHEN** `keyed_block_bounded` is started under any of the seven runtimes without the
  release and sent `a,b,c`, and its keyed stage has committed
- **THEN** while the third tasklet blocks, the controller holds a durable Cell of the keyed
  tasklet for each of `a`, `b` and `c` and no other, and the journal's `task_committed` entry
  for the keyed stage names exactly those three identities

#### Scenario: A crash restores the keyed Cells before the pass resumes

- **WHEN** `keyed_block_bounded` is started without the release, the runner is killed once
  `Replay.resume_cell_count/2` reports the three keyed Cells and the keyed stage is journaled,
  and a runner with the release is bound
- **THEN** the instance completes `ok`, each keyed Cell's WAL holds exactly one record, the
  keyed stage was not re-run, and the same assertions hold under every runtime

#### Scenario: A failure compensates once per key

- **WHEN** `keyed_saga_bounded` is started under any runtime and sent `a,b,c`
- **THEN** the instance completes `error`, a compensation-done marker exists for each of the
  three keyed Cells and no other Cell of the keyed tasklet, and the first tasklet, which
  declares no hook, is not compensated

### Requirement: Timers inside a bounded operator are proven on every runtime

Every manifest SHALL list two fixtures under `bounded`, registered under the same ids in every test app: `timer_loop_bounded`, a `per_input` bounded operator `in -> tick -> relay -> out` whose `tick` registers a processing-time timer of the delay its message names (`after:<ms>`), sleeps for the duration a `slow:<ms>` message names, emits `armed:<at>` from `run` and `fired:<at>` from `on_timer`, and whose `relay` prefixes what it receives with `relay:` onto the sink; and `event_timer_bounded`, a `per_input` bounded operator whose entry-fed tasklet declares an event-time clock read from the message, registers `timer_at_event_time` at the message's event time plus 100 and emits `fired:<at>` from `on_timer`, onto a sink. A `test/system/` module tagged `capability: :bounded` SHALL drive both on every runtime and assert: a firing reaches the sink through `relay` with no second message; a firing due while a `slow` pass runs reaches the sink after that pass's output; an instance parked under a short `FLEZHA_PARK_IDLE_MS` with a timer pending is reported `suspended` with the timer's timestamp as its park record's wake deadline, rehydrates at it and the firing reaches the sink; a runner killed with a timer pending and restarted fires it; an event-time timer fires ahead of the message that advanced the high-water. The interim timer-in-bounded refusal scenario and its fixture SHALL NOT exist.

#### Scenario: The processing-time fixture is driven on every runtime

- **WHEN** `timer_loop_bounded` is started under any runtime and `after:300` is sent
- **THEN** the sink SHALL receive `relay:armed:<at>` and, about 300 ms later with nothing else sent, `relay:fired:<at>` with the same `<at>`

#### Scenario: A firing waits behind a running pass on every runtime

- **WHEN** `after:500` is sent and `slow:1500` immediately after it
- **THEN** the sink SHALL receive the slow pass's output before `relay:fired:<at>`, on every runtime

#### Scenario: A sleeper wakes at its timer on every runtime

- **WHEN** `after:2000` is sent under `FLEZHA_PARK_IDLE_MS=200`
- **THEN** the instance SHALL be reported `suspended` with its park record's wake deadline equal to `<at>` before the firing, and the sink SHALL receive `relay:fired:<at>` after it, on every runtime

#### Scenario: The event-time fixture is driven on every runtime

- **WHEN** `event_timer_bounded` receives a message with event time 1000 and then one with event time 1150
- **THEN** the sink SHALL receive `fired:1100` before whatever the second message produces, on every runtime

### Requirement: A keyed per-input bounded operator is a fixture every manifest carries and proves partitioned

Every SDK test app SHALL register `keyed_loop_bounded`, and every manifest SHALL list it under
`bounded` and under `partitioned` in the reference's order. `keyed_loop_bounded` SHALL be a
durable `per_input` bounded operator with one entry channel `trades` whose entry edge declares `key_by`
over the message's symbol (the text before the first `:`), one tasklet that counts the
messages it has seen for its key in declared state and emits `<symbol>:<count>` on a sink
named `keyed_loop_bounded_out`, and no compensation hook. The refusal of a `once` bounded operator SHALL
be proven on `echo_bounded`, the `once` fixture every manifest already carries, so no fixture
exists for a refusal alone. The scenarios that drive them SHALL live in
`test/system/partitioned_operators_e2e_test.exs`, tagged `capability: :partitioned` and pinned
to no runtime, and SHALL assert through the controller: the members' rows and ranges, each
member's journal, each key's Cell under its owner, the dead-letter listing for a mis-route, and
the coverage report.

#### Scenario: Messages by key reach their member and each member commits its own passes

- **WHEN** `keyed_loop_bounded` is started with `--partitions 2` under any of the seven runtimes
  and five messages for three keys are sent, each to the member owning its key
- **THEN** each key's Cell exists under its owner and nowhere else, each member's journal holds
  one committed iteration per message it received, the sink carries `<symbol>:<count>` per
  message, and `flz operator info` reports the group `2/2` with full coverage

#### Scenario: A mis-routed key parks and opens no pass on every runtime

- **WHEN** a message whose key the receiving member does not own is sent to it
- **THEN** the member's dead-letter listing holds it with the contract error naming the key, its
  group and the owning partition, the member's journal gains no iteration, and its next in-range
  message commits as the next iteration

#### Scenario: A rescale between passes continues every key on its new owner

- **WHEN** the group is rescaled to 3 while every member is quiet and messages for the same
  keys are sent to their new owners
- **THEN** each key's count continues from its pre-rescale value on the member that now owns
  it, the survivors' journals resume at their next iteration and hold no iteration below their
  seal, `<group>#p2` commits its first pass as iteration 0, and coverage shows all 256 groups
  owned once

#### Scenario: A once bounded operator is refused partitions on every runtime

- **WHEN** `echo_bounded` is started with `--partitions 2` under any runtime
- **THEN** the first member's bind is refused with `topology.partitions_without_per_input_loop` and not with
  the keyless refusal, the group is rolled back with no member left running, and a second
  attempt is refused before any member starts

### Requirement: A bounded operator's keyed output is a fixture every manifest carries

Every SDK test app SHALL register `outbox_block_bounded` as a durable bounded operator whose first
tasklet emits once on a keyed sink channel with a key and one user metadata pair and commits an
idempotent change in the same invocation, and whose second tasklet blocks until
`FLEZHA_BOUNDED_E2E_RELEASE` is set; every manifest SHALL list it under `bounded` in the
reference's order. The scenarios that drive it SHALL live in
`test/system/bounded_crash_resume_e2e_test.exs`, tagged `capability: :bounded` and pinned to
no runtime, and SHALL assert through the controller: that the first tasklet's Cell WAL holds one
effect record carrying the change and the output; that the `task_committed` entry names that
record's seq and channel and carries no payload; that the sink received the output once, after
the commit's ack, stamped `flz.cell`, `flz.seq = <seq>.0`, `flz.epoch` `0`, `flz.key`, `channel`
and the metadata pair; and that a rebind with the Cell's flush cursor moved back re-drives the
same output with identical stamps. A second scenario SHALL raise `FLEZHA_FLUSH_LINGER_MS` on the
runner and assert, on every host runtime, that the record precedes the entry naming it.

#### Scenario: A bounded output carries its identity on every runtime

- **WHEN** `outbox_block_bounded` is started under any of the seven runtimes with the release set and sent one message
- **THEN** the sink receives exactly one frame carrying `flz.key`, the metadata pair and `channel`, its `flz.seq` equals the first tasklet's record seq with ordinal `0`, and `flz state journal` shows the stage naming that seq and channel and no `outbox` or `flushes` line

#### Scenario: A crash before the flush re-drives with identical stamps

- **WHEN** the instance is killed after the commit's ack, the Cell's `outbox_flushed_seq` is moved back below the record, and a runner with the release is bound
- **THEN** the sink receives the output a second time with stamps byte-identical to the first, and the instance completes `ok`

#### Scenario: The record precedes the marker under a long linger

- **WHEN** `outbox_block_bounded` runs on a host runtime with `FLEZHA_FLUSH_LINGER_MS=20000` and the first tasklet commits
- **THEN** the Cell WAL holds the record before the journal holds `task_committed(0)`, and the entry names that record's seq

### Requirement: Answering an outside call SHALL be a required capability every SDK proves

`call_response` SHALL join the capability vocabulary and the required set, so no manifest may defer
it. Every manifest SHALL list it under `fixtures` with exactly the reference's ids: `call_echo`, an
unbounded operator whose one tasklet answers with its input through `response()`;
`call_downstream`, an unbounded operator whose second tasklet answers; `call_split`, an unbounded
operator whose one tasklet feeds two filtered paths, each ending in `response()` through a tasklet
of its own; and `call_turn`, a durable `:per_input` bounded operator whose turn answers through
`response()`. Every test app SHALL declare each fixture through its SDK's builder, and SHALL list
the `response` step under `topology_surfaces`. `test/system/call_response_e2e_test.exs` SHALL be
tagged `capability: :call_response` and SHALL drive every fixture on the selected runtime through
`Flezha.Service.Operator.call/6`, and the reference refusal rows for `ChannelEndsTwice` SHALL be
driven through every runtime's real boot like every other reference refusal.

#### Scenario: Every runtime answers both kinds

- **WHEN** the scenario calls `call_echo` and `call_turn` on any runtime with the payload `hello`
- **THEN** both calls SHALL answer `hello`, the bounded one after its turn's commit

#### Scenario: Every runtime answers from the path the input took

- **WHEN** the scenario calls `call_split` with an input its filter sends down the refused path
- **THEN** the answer SHALL be the refused path's output, and nothing SHALL be dropped as `answered`

#### Scenario: A manifest deferring the capability fails the gate

- **WHEN** a manifest lists `call_response` under `deferred`
- **THEN** the gate SHALL fail naming the manifest and the required capability

### Requirement: Parts are a required capability every manifest carries

`parts` SHALL be in the capability vocabulary and in the required set. Every manifest SHALL
list it under `fixtures` with exactly the reference's ids, `part_echo` and
`part_echo_bounded`. `part_echo` SHALL be an unbounded operator whose one tasklet, for an
input `n`, sends the parts `1` to `n` as text and then emits `done:<n>` on its sink `answer`.
`part_echo_bounded` SHALL be a `per_input` bounded operator doing the same per message.
For the input `retry:<n>` both SHALL send the same parts and fail their first attempt
retryably, then send them again and succeed on the second attempt.
`test/system/part_stream_e2e_test.exs` SHALL be tagged `capability: :parts` and SHALL drive both fixtures on the selected runtime through
`flz operator listen` in text and JSON modes, through `Flezha.Service.Listener.listen/4`,
and with a file sink attached beside the listener.

#### Scenario: A manifest without the fixtures fails the gate

- **WHEN** a manifest lists `parts` without `part_echo_bounded`
- **THEN** the static gate SHALL fail naming the manifest, the capability and the id

#### Scenario: The same parts on every runtime

- **WHEN** `part_stream_e2e_test.exs` runs under each of the seven runtimes with input `3`
- **THEN** each run SHALL see the parts `1`, `2`, `3` with `flz.part` `0`, `1`, `2`, then
  `done:3`, and the file sink SHALL hold `done:3` alone

#### Scenario: A retried attempt's parts start again

- **WHEN** the input is `retry:2`
- **THEN** each runtime SHALL deliver the first attempt's parts with attempt `1`, the second
  attempt's from `flz.part` `0` with attempt `2`, and then the output

