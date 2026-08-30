# code-quality-baseline Specification

## Purpose

The code-quality-baseline capability defines a project-wide build-hygiene contract: default Cargo, Maven, and Mix builds emit zero warnings from project-owned source, default-feature workspace tests pass, lib output filenames do not collide, comments describe present-tense behavior, and dead-code suppressions carry justification. The capability establishes a durable normative baseline; any future change that regresses it must explicitly carry the regression in its proposal. Third-party crate, Hex package, and protobuf-generated source warnings are out of scope and do not gate the build.
## Requirements
### Requirement: Default Cargo build SHALL produce zero warnings from project crates

`cargo build --workspace --all-targets` SHALL exit 0 with no warning lines emitted from project crates (`flezha_runner`, `flz_transport`, `flz_core_ffi`, `flz_nif`, `flz`, `rust_runner`). Warnings emitted by third-party crates compiled as part of the workspace are out of scope and SHALL NOT cause the build gate to fail.

#### Scenario: Clean workspace build

- **WHEN** a developer runs `cargo build --workspace --all-targets` on a freshly-cloned repository
- **THEN** the command exits 0
- **AND** the output contains zero `warning:` lines whose source path begins with any of `sdk/rust/`, `flz_transport/`, `flz_core_ffi/`, `flz_nif/`, `cli/`, or `test/apps/rust_runner/`

#### Scenario: New code introduces a warning

- **WHEN** a contributor introduces a change that produces a `dead_code`, `unused_imports`, `unused_variables`, or other default-level rustc warning
- **THEN** the CI build gate fails because `cargo build` emits the warning and the gate runs `-D warnings`

### Requirement: Default Cargo clippy SHALL produce zero warnings from project crates

`cargo clippy --workspace --all-targets` SHALL exit 0 with no `warning:` lines emitted from project crates under the default Clippy lint set. The `clippy::pedantic` group is NOT enabled and is explicitly out of scope; only the default groups (`clippy::correctness`, `clippy::suspicious`, `clippy::style`, `clippy::complexity`, `clippy::perf`) are enforced.

#### Scenario: Clean clippy run

- **WHEN** a developer runs `cargo clippy --workspace --all-targets` on a freshly-cloned repository
- **THEN** the command exits 0
- **AND** the output contains zero `warning:` lines whose source path is in a project crate

#### Scenario: A clippy warning is genuinely unfixable

- **WHEN** a project crate contains a `#[allow(clippy::<lint>)]` attribute
- **THEN** the attribute SHALL be accompanied by a one-line comment explaining why the lint cannot be fixed at that site, OR the attribute SHALL be at the crate root with the rationale recorded in the crate's top-level docstring

### Requirement: Workspace tests SHALL pass on default Cargo features

`cargo test --workspace` SHALL exit 0 on default features. Test targets that require a non-default Cargo feature (e.g. `bench-internals`) SHALL declare `required-features` in `Cargo.toml` so they are skipped under the default-feature run rather than failing to compile.

#### Scenario: Default workspace test run

- **WHEN** a developer runs `cargo test --workspace`
- **THEN** the command exits 0

#### Scenario: Bench-internals tests run with feature flag

- **WHEN** a developer runs `cargo test --workspace --features flezha_runner/bench-internals`
- **THEN** the bench-internals-gated test targets are compiled and executed
- **AND** the command exits 0

#### Scenario: A new test imports a feature-gated symbol

- **WHEN** a contributor adds a test that imports a symbol gated behind a Cargo feature
- **THEN** the test target SHALL declare `required-features = ["<feature>"]` in `Cargo.toml`

### Requirement: Cargo lib targets SHALL have unique output filenames

No two crates in the Cargo workspace SHALL produce the same shared-library output filename in the workspace target directory. Each `[lib] name = "..."` SHALL match the crate name, or differ only when there is a documented reason (e.g. an FFI binary's external load name).

#### Scenario: Workspace build does not emit collision warnings

- **WHEN** a developer runs `cargo build --workspace --all-targets`
- **THEN** the output contains zero `output filename collision` warnings

### Requirement: Elixir compile SHALL be clean under `--warnings-as-errors`

`mix compile --warnings-as-errors` SHALL exit 0 for the `flezha` application. Warnings originating from third-party Hex packages compiled under `deps/` are out of scope and SHALL NOT cause the build to fail (the warnings-as-errors gate applies only to source files under the `flezha` app).

#### Scenario: Clean Elixir compile

- **WHEN** a developer runs `mix compile --warnings-as-errors` after `mix deps.get`
- **THEN** the command exits 0
- **AND** no warning is reported against any source file under `lib/flezha/`

### Requirement: Maven Java compile SHALL produce zero warnings from `flezha-sdk-*` modules

`mvn -DskipTests compile` SHALL produce zero `[WARNING]` lines or `warning:` lines whose source path is under `sdk/java/flezha-sdk-*` modules. The `-Pstrict` Maven profile SHALL enforce this gate via `-Werror`. Warnings on protobuf-generated source under `target/generated-sources/` are suppressed at the source-set level and out of scope. Maven's `-Xlint:all` SHALL be enabled in the default compiler configuration so future warnings are visible without requiring an explicit flag.

#### Scenario: Clean Maven build

- **WHEN** a developer runs `mvn -DskipTests compile` from `sdk/java/`
- **THEN** the command exits 0
- **AND** no `[WARNING]` or `warning:` line cites a file under any `flezha-sdk-*/src/`

#### Scenario: Strict CI build

- **WHEN** CI runs `mvn -DskipTests -Pstrict compile`
- **THEN** the command exits 0
- **AND** any javac warning emitted from `flezha-sdk-*/src/` causes the build to fail

### Requirement: Source comments SHALL describe present-tense behavior

Source comments in project files SHALL describe what the code does, why it is needed, or what invariant it preserves. Comments SHALL NOT reference change history (e.g. "previously", "used to", "no longer", "removed in"), in-flight OpenSpec change names, or PR numbers. Generated source files (`sdk/rust/src/generated/`, protobuf-output Java) are excluded; their content is owned by the generator.

#### Scenario: Adding a comment that references history

- **WHEN** a contributor writes a comment containing the phrase "previously", "used to", "no longer", "removed in", or "see proposal X"
- **THEN** the comment SHALL be rewritten to describe present-tense behavior or removed entirely

#### Scenario: Workaround comments are preserved

- **WHEN** a comment names a non-obvious invariant, a hidden constraint, or a workaround for a specific bug
- **THEN** the comment SHALL be preserved (this requirement targets history comments, not load-bearing ones)

### Requirement: `#[allow(dead_code)]` annotations SHALL carry a justification

Every `#[allow(dead_code)]` attribute in project crates SHALL be accompanied by a one-line comment naming the reason the item appears unused (e.g. "JNI export", "public hook for plugin authors", "consumed only by `cargo bench`"). Bare `#[allow(dead_code)]` is forbidden.

#### Scenario: Dead-code suppression with justification

- **WHEN** a contributor adds a `#[allow(dead_code)]` attribute to a project source file
- **THEN** the attribute SHALL be either preceded by a one-line `//` comment explaining why, or use the Rust 1.81+ `#[allow(dead_code, reason = "...")]` form

### Requirement: A test SHALL restore any process-global test mode it changes

A test module SHALL return a process-global test-double mode to its default
before that module's tests finish. A module SHALL NOT rely on the library
observing its owner exit: the library begins watching an owner only once that
owner registers a test double, so a test that registers none is never watched
and the mode outlives it.

Test sources SHALL NOT call the raw mode-setting function. They SHALL use the
repository's releasing wrapper, so the release cannot be forgotten. The wrapper's
own definition is the single exemption and SHALL be allowlisted by path.

A gate SHALL fail when a test source calls the raw function, naming the file and
the wrapper to use instead.

#### Scenario: A module that stubs nothing does not leak the mode

- **GIVEN** a test module that enters global mode and registers no stub
- **WHEN** its tests finish and another module calls `stub/3` from its own process
- **THEN** that call SHALL succeed

#### Scenario: A module hands the default mode to the next one

- **GIVEN** a test module that enters global mode
- **WHEN** its tests finish, on any seed
- **THEN** the process-global mode SHALL read as the default, whatever the
  module's last-run test did

#### Scenario: The gate refuses a raw mode-setting call in a test

- **GIVEN** a test source calling the raw global-mode function directly
- **WHEN** the gate runs
- **THEN** it SHALL exit non-zero naming that file and the wrapper to use

#### Scenario: The wrapper's own definition is exempt

- **WHEN** the gate runs against a tree whose only raw call is inside the
  wrapper's own module
- **THEN** it SHALL exit zero, the exemption being declared by path

### Requirement: A failure-path test SHALL assert its diagnostic

A test that deliberately drives a path which logs an error SHALL capture that
log and assert on the part of the message that carries meaning. The diagnostic
becomes something the test checks, rather than output the run prints and the
merge gate trips over.

Capturing without asserting SHALL NOT satisfy this: it quiets the run while
removing the diagnostic from everything, which trades a visible problem for an
invisible one. A module-wide capture SHALL NOT be used either, because it also
swallows lines a later test emits by accident — including the ones that would
have revealed a defect.

#### Scenario: A failure-path test asserts what it provoked

- **GIVEN** a test that drives a path which logs an error
- **WHEN** it runs
- **THEN** it SHALL capture that log and assert on the message's meaningful part

#### Scenario: A silenced diagnostic does not count

- **GIVEN** a test that captures a log and discards it, or a module-wide capture
- **WHEN** the practice is reviewed
- **THEN** it SHALL NOT be treated as satisfying this requirement

### Requirement: A log gate SHALL be demonstrated to fail

A gate that searches output for a forbidden pattern SHALL be shown to go red on
a deliberate violation and green on its removal. A gate that has only ever
reported one verdict has been shown to be consistent, not to work — and one that
has been red since it was written reports nothing, because no one can tell a new
violation from the standing ones.

#### Scenario: The gate goes red on purpose

- **GIVEN** a deliberate uncaptured error added to the suite
- **WHEN** the gate's own command runs against the run's output
- **THEN** it SHALL exit non-zero, and SHALL exit zero once that error is removed

### Requirement: A test SHALL remove the temporary directories it creates

A test that creates a temporary directory SHALL register its removal at the
point of creation, so a run leaves the temporary-directory count no higher than
it found it. Cleanup SHALL NOT depend on a shared teardown helper that each
module must remember to call, which reproduces the same omission one level up.

#### Scenario: A run leaves no temporary directories behind

- **GIVEN** the count of temporary directories before a plain `mix test`
- **WHEN** the run completes
- **THEN** the count SHALL be no higher than before

### Requirement: A dependency SHALL resolve from a published version

Every dependency SHALL be declared as a published version requirement, so that
the manifest alone determines what a build contains. A dependency SHALL NOT be
declared against a branch: the lock pins a commit while the declaration names a
moving target, so what a checkout builds against depends on when someone last
unlocked it rather than on anything written down.

A branch dependency MAY be taken temporarily while an upstream release is
outstanding. It SHALL carry, in the declaration itself, the condition that ends
it and the requirement it reverts to — and it SHALL be recorded as an open issue
so that the condition is checked rather than forgotten.

A local-path override for developing against an unreleased checkout is a
different thing and is permitted, because it is opt-in per build and names no
version.

#### Scenario: No dependency is declared against a branch

- **WHEN** the dependency declarations are read
- **THEN** none SHALL name a git branch

#### Scenario: A temporary branch pin states its exit condition

- **GIVEN** a dependency taken from a branch while an upstream release is outstanding
- **WHEN** its declaration is read
- **THEN** it SHALL name the condition that ends it and the version requirement it reverts to, and an open issue SHALL track it

#### Scenario: A local-path override remains available

- **GIVEN** an environment variable naming a local checkout of a dependency
- **WHEN** the project is built with it set
- **THEN** the build SHALL use that checkout, and with it unset SHALL use the published version

