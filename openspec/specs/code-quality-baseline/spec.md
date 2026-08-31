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

### Requirement: An assertion about a path's absence SHALL be shown able to fail

A test asserting that a path does not exist SHALL be demonstrated to fail when
that path genuinely does exist. An assertion whose subject can never be present
reports nothing: it passes identically whether the behaviour it guards works or
has been broken since the day it was written.

A durable path SHALL be derived from the function that owns the layout, not
rebuilt from its parts at the call site. Where the owning module exposes no
suitable path, that path SHALL be added to it rather than assembled by the
caller.

#### Scenario: The assertion fails when its subject is present

- **GIVEN** a test asserting a durable path does not exist
- **WHEN** that path is genuinely created and the test runs
- **THEN** the test SHALL fail

#### Scenario: A path is derived, not rebuilt

- **WHEN** a test names a durable directory
- **THEN** it SHALL obtain it from the module that defines the layout, and SHALL NOT join a root to a name itself

#### Scenario: A test whose subject is the layout builds the path itself

- **GIVEN** a test that exists to pin the layout, or one asserting a layout the code no longer writes
- **WHEN** it names a durable directory
- **THEN** it SHALL assemble the path from its parts, because deriving it would make the assertion follow the resolution it exists to prove
- **AND** it SHALL say so where the path is built

### Requirement: A module's documented layout SHALL match what it implements

Where a module documents a durable path layout, that description SHALL match the
code in the same module. A description that disagrees with the implementation is
read and acted on: it is what produced the assertions above.

#### Scenario: The documented layout and the code agree

- **WHEN** a module's documented path layout is compared with the function that builds it
- **THEN** they SHALL describe the same path

### Requirement: A recorded defect SHALL carry a reproduction

An entry in the defect list SHALL carry a command that demonstrates the defect
and the output that command produced. An entry without one records a suspicion,
and a suspicion cannot be verified, closed, or acted on by anyone but its author.

An entry whose defect is fixed or was never true SHALL be moved out of the open
list, keeping its evidence. A closed entry retained in place makes the open list
unreadable: a reader cannot tell which items are live, and stops reading it.

An entry SHALL claim no more than has been measured. Where part of an entry is
found stale, that part SHALL be removed and what replaced it recorded, rather
than the whole entry being left to overstate.

#### Scenario: A new entry carries a reproduction

- **WHEN** a defect is recorded
- **THEN** the entry SHALL include the command that shows it and that command's output

#### Scenario: A closed entry leaves the open list

- **GIVEN** an entry whose defect is fixed or was never true
- **WHEN** the list is read
- **THEN** that entry SHALL NOT appear among open defects, and its evidence SHALL remain available

#### Scenario: A partly stale entry is narrowed

- **GIVEN** an entry of which some claims are measured to be no longer true
- **WHEN** it is revised
- **THEN** the stale claims SHALL be removed and what was measured in their place recorded

### Requirement: A plain test run SHALL emit no uncaptured error log

A default `mix test` on a clean tree SHALL produce no `[error]` or `[critical]`
line. Every failure path a test drives on purpose is captured and asserted; any
line that reaches the console is an unasserted failure, whether it comes from a
test or from the production code under it.

A line SHALL be closed at its cause. Narrowing the gate's pattern, allowlisting a
message, filtering the log, or wrapping a test that drives no failure path in a
module-wide capture SHALL NOT satisfy this: each makes the gate pass without
making the run quiet, which is the condition the gate exists to detect.

A production defect surfaced this way SHALL be fixed as a production change or
recorded with a reproduction and left — never silenced from the test side, which
would hide a live defect behind a green gate.

#### Scenario: A clean tree runs quiet

- **GIVEN** a working tree with no local modifications
- **WHEN** a default `mix test` runs and the log gate's own pattern is applied to its output
- **THEN** the pattern SHALL match nothing

#### Scenario: Silencing the symptom does not count

- **GIVEN** an uncaptured error closed by narrowing the gate, allowlisting the message, or capturing a test that drives no failure path
- **WHEN** the practice is reviewed
- **THEN** it SHALL NOT be treated as satisfying this requirement

### Requirement: A test SHALL scope its observation of a process-global signal

A test SHALL scope its observation of a process-global signal to what the test
itself caused, by a discriminator the test controls. This covers any VM-wide
facility a signal arrives through — a `:telemetry` handler, a named table, or a
shared registry — and it covers a **direct read** of such a facility as well as
a delivered signal: an assertion over a whole node-global ledger asserts about
every emitter on the node, not about the code under test.

Handlers of this kind fire for events raised in any process, so an unscoped
observation in an `async: true` module receives whatever a concurrently running
module or a background process emits. The resulting assertion is not about the
code under test, and it fails intermittently and only in a full run, which is the
most expensive shape a test failure can take.

Waiting, retrying, or settling before the assertion SHALL NOT satisfy this: it
defers the foreign signal rather than excluding it, and leaves the same
observation open for the next emitter.

A test that scopes one observation of a facility and leaves another unscoped in
the same body SHALL be treated as unscoped. The scoping SHALL be demonstrated by
a foreign entry the test itself creates, so the discriminator is proved
load-bearing rather than assumed.

#### Scenario: A concurrent emitter does not reach another test's assertion

- **GIVEN** an `async: true` test observing a process-global signal
- **WHEN** another module or a background process emits the same signal while it runs
- **THEN** the observation SHALL NOT deliver that emitter's signal to the test

#### Scenario: A settle does not count as scoping

- **GIVEN** an unscoped observation made to pass by waiting, retrying, or settling
- **WHEN** the practice is reviewed
- **THEN** it SHALL NOT be treated as satisfying this requirement

#### Scenario: A direct read of a node-global ledger is scoped like a signal

- **GIVEN** a test asserting on the contents of a node-global ledger it did not create alone
- **WHEN** another module has left an entry in that ledger
- **THEN** the assertion SHALL be scoped to the reference the test itself created, and SHALL NOT be satisfied by clearing or re-reading the ledger

### Requirement: A module SHALL be named so that its last segment survives aliasing

An Elixir module's last segment SHALL mean what the module is, without its parent
namespace, because that segment is what every call site reads once the module is
aliased. A module whose last segment names the wrong thing SHALL be renamed; one that
does not earn its own file SHALL be folded into its parent and called there.

A module SHALL NOT be folded into a parent that does not exist. Creating a namespace
module in order to have somewhere to fold into is new code, and the reuse order forbids
it where the module can simply be renamed instead.

A module SHALL NOT be folded into a parent that the fold would carry over the file-size
ceiling. For such a parent, renaming is the only available remedy and the proposal
SHALL say so per module rather than discovering it during the edit.

#### Scenario: A module's last segment names the wrong thing

- **GIVEN** a module whose last segment describes something the module is not, such
  that a neighbouring module documents the difference in prose to keep the two apart
- **WHEN** the naming rule is applied
- **THEN** the module SHALL be renamed so that the prose disambiguation is unnecessary,
  and that prose SHALL be removed in the same change

#### Scenario: A small module has no parent module to fold into

- **GIVEN** a module that does not earn its own file
- **AND** its namespace has no module of its own
- **WHEN** the naming rule is applied
- **THEN** the module SHALL be renamed rather than folded, and a namespace module
  SHALL NOT be created for it

#### Scenario: The parent has no room

- **GIVEN** a fold candidate whose parent is at or over the file-size ceiling
- **WHEN** the verdict is recorded
- **THEN** it SHALL be rename, and the proposal SHALL name the parent's line count as
  the reason

### Requirement: A persisted module name SHALL be renamed only with a migration

A module whose name is written into durable state SHALL NOT be renamed by a change that
does not carry a migration: where the name is persisted, renaming it is a migration and
not a rename. The set of such modules SHALL be enumerated from source before any rename
is proposed, and no module in that set SHALL be renamed without one.

The enumeration SHALL be derived from the write path and the read path, not from a
sample of on-disk bytes: a store that happens to hold no rows on the box being surveyed
proves nothing about what it holds in production.

A rename of a persisted module SHALL NOT be treated as low risk on the grounds that
restore does not crash. Where the restore path resolves a module and tolerates its
absence, the failure is silent — the subsystem reports itself healthy and never starts.

#### Scenario: The durable set is enumerated before a rename is proposed

- **WHEN** a change proposes renaming any module
- **THEN** it SHALL first record which modules have their names persisted, with the
  file and line of the write path for each
- **AND** it SHALL state, per proposed rename, that the module is not in that set

#### Scenario: A persisted module must move anyway

- **GIVEN** a change that must rename a module whose name is persisted
- **WHEN** the change is written
- **THEN** it SHALL carry the operator procedure that makes existing durable state
  readable again, and SHALL name it in the proposal rather than in a dev note

#### Scenario: A tolerated absence is not a safe absence

- **GIVEN** a restore path that resolves a module name and continues when the module
  cannot be loaded
- **WHEN** the risk of a rename is assessed
- **THEN** that path SHALL be treated as a silent-failure surface, and the rename
  SHALL be verified by a live restore rather than by a green compile

### Requirement: A nested module SHALL be reached by an alias, and the alias SHALL keep the name unambiguous

A call site SHALL reach a nested module through an alias rather than by its fully
qualified name. The alias SHALL bind the module's last segment when that segment is
unique across the application's own modules, and SHALL bind a qualified spelling
otherwise, so that the aliased name identifies exactly one module.

One spelling rule SHALL serve the whole tree. The same module SHALL NOT be aliased
bare in some files and under a qualified name in others: that is the same question
answered two ways, and the reader cannot tell which answer is authoritative.

An alias SHALL NOT bind a name that shadows a standard-library module the file uses,
and SHALL NOT be inserted out of alphabetical order within its group.

#### Scenario: A unique last segment is aliased bare

- **GIVEN** a nested module whose last segment names no other module in the application
- **WHEN** a call site reaches it
- **THEN** the file SHALL alias it and call it by that segment

#### Scenario: An ambiguous last segment is aliased under a qualified name

- **GIVEN** two modules in different namespaces sharing a last segment
- **WHEN** either is aliased
- **THEN** the alias SHALL bind a qualified spelling that names exactly one of them

#### Scenario: The tree does not carry two spellings for one module

- **WHEN** the alias sites for any one module are read across the tree
- **THEN** they SHALL all bind the same name

### Requirement: A contested lint check SHALL be settled by decision, and the settled count SHALL be ratcheted

A lint check the codebase does not follow SHALL be either adopted and the code fixed,
or disabled in the linter's configuration. It SHALL NOT be left enabled and failing,
because a count that has been red since it was written reports nothing: no one can
distinguish a new violation from the standing ones.

Once the contested checks are settled, the resulting warning count SHALL be recorded as
a ratcheted baseline metric. A metric recorded as absent in every area compares nothing
and is not a gate.

The recorded count SHALL come from a measured run taken after the fixes land, never
from arithmetic over the counts before them: fixing one check can raise another, and a
baseline computed by subtraction records a number the tree does not have.

A check SHALL NOT be adopted with its strictness reduced in order to lower the work.
Where the check's own defaults already exclude a class of finding, those defaults SHALL
be left as they are and SHALL be cited as the reason that class needs no code change.

#### Scenario: A contested check is disabled rather than left failing

- **GIVEN** a lint check whose findings the codebase deliberately does not follow
- **WHEN** the decision is taken
- **THEN** the check SHALL be listed as disabled in the linter configuration, and the
  reason SHALL be recorded

#### Scenario: The baseline is measured, not derived

- **GIVEN** a settled lint configuration and the code fixes for the adopted check
- **WHEN** the warning count is recorded as a baseline
- **THEN** it SHALL be the output of a run taken after those fixes, and any excess over
  the expected figure SHALL be accounted for before it is recorded

#### Scenario: Adoption does not weaken the check

- **WHEN** a check is adopted
- **THEN** its parameters SHALL NOT be relaxed to reduce the number of sites to fix

### Requirement: A rename SHALL be verified by a gate that reads more than code

A change that retires an identifier SHALL add a gate that fails on any surviving
occurrence of the retired name in any tracked human-readable file. Compilation and the
test suite verify code references only; a retired name surviving in a specification,
a document, a comment or a string compiles, tests and runs while being wrong.

The gate SHALL extend the repository's shared naming-gate walk rather than introduce a
second gate mechanism, and SHALL provide a recorded-history escape so that a note
citing a retired name as history is not an offence.

The gate SHALL be demonstrated to fail on a deliberate reintroduction and to pass on
its removal, before the rename it guards is committed.

#### Scenario: A retired name surviving in a specification is caught

- **GIVEN** a renamed module still named in a specification or document
- **WHEN** the gate runs
- **THEN** it SHALL exit non-zero naming that file

#### Scenario: The gate is proved before it is trusted

- **WHEN** the gate is added
- **THEN** it SHALL be shown to exit non-zero on a deliberate reintroduction of the
  retired name, and zero once that reintroduction is removed

#### Scenario: A historical citation is not an offence

- **GIVEN** a development note citing a retired name as a record of what changed
- **WHEN** the gate runs
- **THEN** that occurrence SHALL be exempt by an explicit recorded-history marker, and
  SHALL NOT be deleted to satisfy the gate

### Requirement: A test SHALL stop the background worker it starts

A test SHALL stop, before it ends, every long-lived process it starts under a
supervisor that outlives the test. A supervisor from the application's own
supervision tree is such a supervisor: a worker started under it is owned by the
application, not by the test, and `start_supervised/1` does not apply to it.

A worker left behind keeps running its own timers. Its next tick executes
against whatever global state the suite has reached by then — a stub owned by an
unrelated module, a table whose owner has exited — and raises somewhere no test
is watching. The run stays green and the console carries an unattributable
error, which is the most expensive shape a defect can take.

Answering the crash rather than the leak SHALL NOT satisfy this. Hardening the
one function the retry happened to reach closes one landing site out of an open
set, because the leaked worker calls into whatever is currently installed.

#### Scenario: A worker started by a test does not outlive it

- **GIVEN** a test that starts a worker under a supervisor from the application's supervision tree
- **WHEN** the test ends
- **THEN** that worker SHALL no longer be running

#### Scenario: The leak is detected at the next test, not at the next release

- **GIVEN** a test module whose tests start such workers
- **WHEN** a test in that module begins
- **THEN** any worker surviving from an earlier test SHALL fail that test and SHALL be named in the failure

#### Scenario: Hardening the landing site does not count

- **GIVEN** a leaked worker whose retry raised inside a helper or a stub, closed by making that helper answer instead of raising
- **WHEN** the practice is reviewed
- **THEN** it SHALL NOT be treated as satisfying this requirement

### Requirement: A minted test identifier SHALL fit the limit its subject enforces

A helper that mints an identifier SHALL bound what it mints to the limit its
subject enforces, so the result is accepted at any value of the varying part.
The limit SHALL be read from the production constant's own accessor, never
restated as a literal in test code.

A per-call-site truncation SHALL NOT satisfy this. It leaves the bound wherever
someone remembered to write it, cannot be applied to a computed prefix, and
budgets a fixed number of digits that a longer run exceeds silently.

A bound that reserves a digit count SHALL NOT satisfy this either: it moves the
cliff rather than removing it. The bound SHALL be computed from the value being
minted.

#### Scenario: A minted id is accepted at any counter width

- **GIVEN** an identifier minted from a prefix and a monotonically growing counter
- **WHEN** the counter reaches any width
- **THEN** the minted identifier SHALL be accepted by the validator the subject enforces

#### Scenario: An id that already fits is unchanged

- **GIVEN** a prefix and counter whose concatenation is already within the limit
- **WHEN** the identifier is minted
- **THEN** it SHALL be byte-identical to the plain `prefix <> "_" <> counter` concatenation

#### Scenario: A per-call-site truncation is not the bound

- **GIVEN** a call site that truncates its prefix by hand before minting
- **WHEN** the practice is reviewed
- **THEN** it SHALL NOT be treated as satisfying this requirement

### Requirement: A split names only modules that cannot reach it back

A refactor that splits a module SHALL leave every piece naming only modules that
cannot reach it back, and SHALL verify that with the dependency graph before the
split is committed rather than arguing it from the design. Charter §3 states the
direction between layers; this states what a split owes inside one.

Two shapes produce a cycle by construction and are named because both were met
here. A behaviour module SHALL NOT name the modules that implement it: every
implementation names the behaviour, so the behaviour naming them closes a cycle
with all of them at once. The configuration that selects an implementation is a
separate concern and belongs in a module that names the implementations and
nothing else. A piece extracted from a parent SHALL NOT name that parent: the
parent necessarily names the piece, so anything the two genuinely share — a
struct, a clock, a constant — moves to a third module that names neither.

A simulation SHALL NOT stand in for the measurement. Predicting the graph from
the call sites missed a file joining a 56-node cycle, and a set-difference of the
graph before and against the change named it in one line.

Where the count of an area's cycles or of its files inside one moves against the
work — a split putting five members where one stood, with no edge changed — the
area SHALL report the edges it cut by name, with the count beside them. A metric
that measures the wrong thing is said so and reported around, not replaced by a
further metric.

#### Scenario: A behaviour module does not name its implementations

- **GIVEN** a behaviour module and the modules that implement it
- **WHEN** the configuration that selects one is written
- **THEN** it SHALL live in a module that the behaviour does not name

#### Scenario: An extracted piece does not name its parent

- **GIVEN** a module split into a parent and one or more extracted pieces
- **WHEN** a piece needs something the parent also uses
- **THEN** that shared thing SHALL move to a module neither the parent nor the piece is named by

#### Scenario: The graph is measured, not predicted

- **GIVEN** a change that is expected to remove a dependency cycle
- **WHEN** the change is made
- **THEN** the cycle graph SHALL be re-read and compared with the graph before it

#### Scenario: A cycle metric moving against the work is reported, not optimised

- **GIVEN** a split that leaves a cycle's membership unchanged while raising the count of an area's files within it
- **WHEN** the area reports its result
- **THEN** it SHALL name the edges it cut, with the count reported beside them

