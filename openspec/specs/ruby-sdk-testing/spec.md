# ruby-sdk-testing Specification

## Purpose
TBD - created by archiving change ruby-sdk. Update Purpose after archive.
## Requirements
### Requirement: A Ruby test app joins the shared conformance suite without changing the harness

The change SHALL add `test/apps/ruby_runner/` with a `conformance.json` declaring every field the manifest requirement names, and SHALL add `ruby` as a runtime option of the nightly end-to-end workflow. The suite SHALL admit the SDK with no change to `test/helpers/`, `test/system/`, `test/test_helper.exs` or the formatter, and with no new clause in the static gate: the gate's per-SDK tables — where an SDK spells its language byte, and which of its sources may name a connect variable — SHALL each gain one row, which is what admission means, and no rule SHALL be added, relaxed or special-cased for `ruby`.

#### Scenario: The app is admitted by manifest and one gate row
- **WHEN** the Ruby test app and its manifest are added
- **THEN** `mix test` SHALL pass with no edit to the harness, the system tests or the formatter, and the gate's diff SHALL be one row per per-SDK table

#### Scenario: A missing manifest field fails the gate
- **WHEN** the manifest omits a required field or names a capability outside the vocabulary
- **THEN** the static gate SHALL fail naming `ruby` and the offending field

### Requirement: The Ruby manifest matches the reference for what it carries and names what it defers

For every capability the Ruby manifest does not defer, its fixture set SHALL equal the Rust reference manifest's. Fixture ids SHALL be the reference's ids, unprefixed, and each SHALL implement the same scenario with the same sink channel names, output bytes and declared options. Every deferred capability SHALL be named in `deferred`, and the SDK's documentation SHALL name the follow-up change that owes it.

#### Scenario: Fixture parity with the reference
- **WHEN** the gate compares `test/apps/ruby_runner/conformance.json` against `test/apps/rust_runner/conformance.json`
- **THEN** every non-deferred capability's fixture set SHALL be equal, and no Ruby-prefixed fixture id SHALL exist

#### Scenario: A deferral is named, not silent
- **WHEN** the Ruby manifest defers a capability the reference carries
- **THEN** it SHALL appear in `deferred` and the follow-up that owes it SHALL be named in the SDK's documentation page

#### Scenario: A capability added later fails Ruby red until carried
- **WHEN** a later change adds a vocabulary capability or a reference fixture
- **THEN** the gate SHALL fail for `ruby` until the Ruby app carries it or defers it by name

### Requirement: The end-to-end suite runs green under the Ruby runtime

`FLEZHA_E2E_RUNTIME=ruby` SHALL run the system suite against the Ruby runner over every transport the manifest lists, and every non-deferred capability's tests SHALL pass. The runner SHALL honour the stdin-EOF teardown contract the harness uses to stop runners.

#### Scenario: The full arm is green
- **WHEN** `FLEZHA_E2E_RUNTIME=ruby mix test --include system` is run
- **THEN** every test whose capability the manifest does not defer SHALL run and pass

#### Scenario: The declaration snapshot pins the Ruby declaration keys
- **WHEN** the smoke tier runs under `ruby`
- **THEN** the decoded class declaration SHALL match the golden snapshot, and an unspelled or misspelled declaration key SHALL fail it

### Requirement: The gem carries its own unit-test workflow

The SDK SHALL have a CI workflow running the gem's unit tests on every supported Ruby version, running the concurrency requirements' scenarios (pool growth and retirement, GVL release, fork refusal, executor ordering) as real tests rather than documentation, and cross-building the platform gems.

#### Scenario: Concurrency behaviour is tested, not asserted in prose
- **WHEN** the gem's unit suite runs
- **THEN** it SHALL include tests that observe pool growth under blocked handlers, retirement afterwards, a refused post-fork core call, and identical ordering under both executors

### Requirement: The Ruby SDK SHALL declare a pinned coverage library

The Ruby SDK's `flezha.gemspec` SHALL declare a coverage library as a development dependency at an exact version, alongside the `async`, `minitest`, `rake` and `rubocop` pins it already carries, so the gem's suite can report coverage from its own manifest.

The suite SHALL load it through the `Rakefile`'s existing test task rather than through a second task added beside it, because a coverage run and a plain run SHALL exercise the same suite definition.

#### Scenario: Coverage runs from the gemspec's development dependencies

- **WHEN** the gem's development dependencies are installed into a clean checkout and the suite is run in coverage mode
- **THEN** a coverage report SHALL be produced without installing a gem the gemspec does not name

#### Scenario: One test task, not two

- **WHEN** the `Rakefile` is read
- **THEN** the coverage run SHALL use the existing test task's suite definition, and SHALL NOT duplicate it

