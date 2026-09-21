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

`mvn -DskipTests compile` SHALL produce zero `[WARNING]` lines or `warning:` lines whose source path is under `sdk/java/flezha-sdk-*` modules. The `-Pstrict` Maven profile SHALL enforce this gate via `-Werror`. Warnings on generated source under `target/generated-sources/` are suppressed at the source-set level and out of scope. Maven's `-Xlint:all` SHALL be enabled in the default compiler configuration so future warnings are visible without requiring an explicit flag.

#### Scenario: Clean Maven build

- **WHEN** a developer runs `mvn -DskipTests compile` from `sdk/java/`
- **THEN** the command exits 0
- **AND** no `[WARNING]` or `warning:` line cites a file under any `flezha-sdk-*/src/`

#### Scenario: Strict CI build

- **WHEN** CI runs `mvn -DskipTests -Pstrict compile`
- **THEN** the command exits 0
- **AND** any javac warning emitted from `flezha-sdk-*/src/` causes the build to fail

### Requirement: Source comments SHALL describe present-tense behavior

Source comments in project files SHALL describe what the code does, why it is needed, or what invariant it preserves. Comments SHALL NOT reference change history (e.g. "previously", "used to", "no longer", "removed in"), in-flight OpenSpec change names, or PR numbers. Generated source files (`sdk/rust/src/generated/`, Java under `target/generated-sources/`) are excluded; their content is owned by the generator.

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
- **THEN** it SHALL carry the operator tasklet that makes existing durable state
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

The recorded-history escape SHALL exempt the lines its markers delimit and no others.
A line carrying both the opening and the closing marker SHALL be a whole record: it
SHALL exempt that line alone, and SHALL leave the surrounding block state unchanged.
An opening marker with no matching closing marker before end of file SHALL fail the
gate, naming the line that opened the block, because such a block exempts every line
below it and that is the whole-file exemption this escape exists to avoid.

The shared walk's own scoping SHALL be covered by a test that runs wherever the gates
run. A gate that reports success because it stopped reading and a gate that reports
success because the tree is clean are indistinguishable from the outside, so the
scoping of the exemption is not something a passing gate can evidence about itself.

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

#### Scenario: A record written on one line exempts that line only

- **GIVEN** a document whose sentence wraps a retired name in an opening and a closing
  marker on the same line
- **WHEN** the gate runs and a later line of that file names the retired name outside
  any marker
- **THEN** the gate SHALL exempt the marked line and SHALL fail on the later one

#### Scenario: A record block left open fails the gate

- **GIVEN** a document containing an opening marker with no matching closing marker
- **WHEN** the gate runs
- **THEN** it SHALL exit non-zero and SHALL name the file and the line the block was
  opened on, rather than silently exempting the rest of that file

#### Scenario: The escape's scoping is proved rather than assumed

- **WHEN** the naming gates run in CI
- **THEN** a test of the shared walk SHALL run beside them, covering a record written
  on one line, a record spanning two markers, a block left open, and a file with no
  markers, and SHALL fail if any of those exempts more or fewer lines than it delimits

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

### Requirement: An SDK's test toolchain SHALL be declared, pinned, and installed from its own manifest

Every library an SDK's test suite loads SHALL be declared as a development
dependency in that SDK's own dependency manifest, SHALL be pinned there to an
exact version, and continuous integration SHALL install the version the manifest
names rather than a version the workflow names for itself.

The three clauses fail independently and each is a defect on its own. An
undeclared library makes a suite pass by accident of what the machine happens to
carry. An unpinned one lets the resolution drift under a green tree, across a
major release the declared range does not bound. A workflow that names its own
version makes the manifest a document nothing acts on, so the effective manifest
is a line in continuous integration where no reader looks for it.

The install command SHALL read the version out of the manifest. Where the
ecosystem's installer already resolves the manifest — a Composer `install`, a
Maven build, a `cargo` build — that is satisfied by pinning in the manifest and
nothing further is required. Where the installer takes a version on its command
line, the workflow SHALL derive that version from the manifest rather than
repeating it, so the two cannot diverge.

An extraction that derives a version from a manifest SHALL be anchored to the
exact-pin spelling, so that relaxing a pin to a range fails the install step
rather than silently resolving whatever is newest.

A lockfile SHALL be committed where the ecosystem's convention is to commit one
and where the project's own artefact shape calls for it, and the install command
SHALL be the one that honours it. An install command that MAY resolve past a
committed lockfile leaves the tree under test recorded nowhere, so the presence
of a lockfile alone does not satisfy this requirement.

Where a lockfile is committed, the installer SHALL honour it and SHALL NOT fall
back to a resolving install when the honouring one fails. A lockfile-honouring
install fails precisely when the lockfile and the manifest disagree, and that
failure is the report the lockfile exists to produce; a fallback converts it
into a silent fresh resolution whose rewritten lockfile continuous integration
then discards. This clause covers every directory that commits a lockfile,
including a test application or worked example that is not itself an SDK.

An SDK's own runtime dependencies are NOT covered by the exact-pin clause and
SHALL keep the version range they declare to consumers, because a suite must meet
the resolution a consumer gets rather than one the manifest invented for testing.

This requirement covers the libraries an SDK declares. Their transitive
dependencies are resolved by the ecosystem's resolver and are out of scope.

#### Scenario: A test suite loads a library named in no manifest

- **WHEN** an SDK's test suite calls an API from a library that SDK's dependency manifest does not declare
- **THEN** it SHALL be a defect, whether or not the suite is currently green
- **AND** the remedy SHALL be to declare the library, not to rely on it being present

#### Scenario: A declared test dependency carries a range

- **WHEN** an SDK's dependency manifest is read
- **THEN** every test-toolchain entry SHALL name an exact version
- **AND** an entry naming a lower bound with no upper bound SHALL be a defect, because the next major release satisfies it

#### Scenario: Continuous integration names its own version

- **WHEN** a workflow step installs an SDK's test toolchain
- **THEN** the version SHALL come from that SDK's manifest
- **AND** a step that spells a version, or spells none and takes the newest, SHALL be a defect even where the manifest declares the pin correctly

#### Scenario: A pin is relaxed to a range

- **GIVEN** a workflow that derives a test dependency's version from the manifest
- **WHEN** that dependency's pin is relaxed to a range
- **THEN** the install step SHALL fail
- **AND** it SHALL NOT resolve the newest available version

#### Scenario: The installer already reads the manifest

- **GIVEN** an SDK whose ecosystem installer resolves the manifest directly
- **WHEN** the test toolchain is pinned in that manifest
- **THEN** the requirement is satisfied with no change to the workflow

#### Scenario: A committed lockfile is bypassed on the test path

- **GIVEN** an SDK or example application that commits a lockfile
- **WHEN** continuous integration installs its dependencies
- **THEN** it SHALL use the install command that installs exactly what the lockfile records and fails when it cannot
- **AND** an install command free to resolve past the lockfile SHALL be a defect

#### Scenario: A lockfile-honouring install has a resolving fallback behind it

- **GIVEN** a directory that commits a lockfile
- **WHEN** its install command is read
- **THEN** it SHALL NOT fall back to a resolving install when the lockfile-honouring one fails
- **AND** a drifted lockfile SHALL fail the step, rather than being re-resolved in silence

#### Scenario: An application has no lockfile

- **GIVEN** a directory that is an application rather than a published package — a worked example, a test app
- **WHEN** its dependencies are installed
- **THEN** a lockfile SHALL be committed for it and the install SHALL honour it

#### Scenario: A runtime dependency keeps its range

- **WHEN** an SDK's own runtime dependency appears in a development dependency group
- **THEN** it SHALL keep the range the package declares to consumers
- **AND** pinning it exactly for the suite SHALL be a defect, because the suite would then test a resolution no consumer gets

#### Scenario: Every SDK's row is checkable

- **WHEN** the SDKs are audited against this requirement
- **THEN** each SDK SHALL have an answer for all three clauses — what it declares, whether it is pinned, and what continuous integration installs
- **AND** an SDK that fails any clause SHALL be recorded as failing, rather than reported as clean because its suite is green

### Requirement: A document's citation of code SHALL be verified by a gate

A citation a document makes SHALL resolve in the tree, and a gate SHALL check that it does. A citation is a pointer with a decidable answer — an Elixir module, an Elixir function, a repository path with an optional line, an environment variable, a telemetry event, a build module the reactor registers — and the gate SHALL fail a citation the tracked tree cannot answer. A prose claim about behaviour SHALL NOT be checked: a gate that reports a correct sentence gets turned off, and rewording correct prose to satisfy it is a defeat, not a fix.

The gate SHALL judge only citations whose answer the tree carries. A pointer with no directory component, a path rooted where the repository tracks nothing, and a path the repository ignores by policy SHALL be left unjudged rather than reported, and each such exclusion SHALL be a rule about the shape of the citation rather than a list of the instances that trip it.

A published specification SHALL be in the gate's scope, on the same terms as any other document. **The gate judges a citation, never the sentence around it.** Whether a pointer stands in a mandate or in a report is not decidable from the text — a requirement body places both in one sentence — so a specification that names something this tree does not carry SHALL declare that by an explicit marker rather than leave the gate to infer it from a modal verb. Working material the repository keeps out of version control SHALL stay out of scope, because a citation into it has no answer here. A changelog SHALL be out of scope for the different reason that it is a dated record, whose removal entries name what no longer exists because that is what they record.

A requirement that mandates a rename, or that records a removal, SHALL keep naming the retired name, and SHALL carry it inside a recorded-citation marker pair standing on a line of its own. The exemption covers the lines the markers delimit, so a deliberate mention SHALL NOT be wrapped around prose that carries live citations: the whole line would stop being read.

A citation this repository makes on purpose to something it does not carry — a removal quoted as a record, a coordinate in another repository — SHALL be exempted by an explicit recorded-citation marker, and SHALL NOT be deleted or reworded to satisfy the gate.

The gate SHALL report how much it read beside its verdict, so a green that means "the scope filter swallowed the tree" is distinguishable from a green that means the tree is clean. It SHALL be demonstrated to fail on a deliberately introduced dead citation of each kind it claims to check, and to pass once those are removed, before it is trusted.

#### Scenario: A path that a refactor moved is caught

- **GIVEN** a document citing a source file that a later split replaced with a directory
- **WHEN** the gate runs
- **THEN** it SHALL exit non-zero, naming the document, the line and the path

#### Scenario: A module a rename retired is caught in prose

- **GIVEN** a document naming an Elixir module no tracked source declares
- **WHEN** the gate runs
- **THEN** it SHALL exit non-zero naming that document and module, and SHALL NOT report a name that is a namespace above a module the source does declare

#### Scenario: A name the source composes at runtime is not a dead citation

- **GIVEN** a document naming a process registered as `:"#{__MODULE__}.Uds"` by the module that registers it
- **WHEN** the gate runs
- **THEN** that citation SHALL be treated as live

#### Scenario: A deliberate citation to another repository is exempt

- **GIVEN** a document naming files that belong to a sibling repository, wrapped in a recorded-citation marker pair
- **WHEN** the gate runs
- **THEN** those lines SHALL be exempt, and the prose SHALL be unchanged by the exemption

#### Scenario: A stale coordinate inside a published specification is caught

- **GIVEN** a requirement citing a source file or a `file:line` coordinate the tree no longer carries
- **WHEN** the gate runs
- **THEN** it SHALL exit non-zero naming the specification, the line and the citation, whether or not the sentence carrying it uses SHALL or MUST

#### Scenario: A requirement that names a retired name declares it

- **GIVEN** a requirement that mandates a rename, and so must keep naming the retired name, with that clause on its own line inside a recorded-citation marker pair
- **WHEN** the gate runs
- **THEN** that line SHALL be exempt, every other line of the requirement SHALL still be judged, and the requirement's wording SHALL be unchanged by the exemption

#### Scenario: A build module the reactor no longer lists is caught

- **GIVEN** a document or a specification naming a Java SDK Maven module that `sdk/java/pom.xml` does not list
- **WHEN** the gate runs
- **THEN** it SHALL exit non-zero naming the document, the line, the module and the file that holds the registry

#### Scenario: The gate says how much it read

- **WHEN** the gate passes
- **THEN** it SHALL print the number of citations judged and the number of documents read, so a fall in that count is visible without the gate failing

#### Scenario: The gate is proved able to fail before it is trusted

- **WHEN** the gate is added, or its scope or its set of citation kinds is widened
- **THEN** a dead citation of each kind it checks SHALL be introduced into more than one document, the gate SHALL be shown to exit non-zero naming every one, and it SHALL be shown to exit zero once the documents are restored

### Requirement: Tracked material SHALL identify this checkout by what it declares

A tracked file SHALL NOT depend on an absolute path that only one machine resolves. This repository is a shipped artifact, so every tracked script, document and workflow SHALL work in any checkout by any contributor, and a file that names one developer's home directory works in exactly one of them.

A script that must confirm it is operating on this project SHALL do so by reading what the checkout declares about itself — `mix.exs`'s `app:` atom — rather than by comparing its location to a fixed path. The two answer different questions: a location says where a tree sits, which a rename, a clone or a worktree changes, while the declaration says which project it is, which is what a guard against touching another project's data actually needs. Such a guard SHALL keep failing closed: a tree it cannot identify SHALL be refused, and the refusal SHALL name the resolved root and the declaration it did not find.

A document that needs the reader to choose a directory SHALL state the constraints the choice must meet and use a named variable in its commands, rather than naming one instance of a valid choice. A script that needs a path outside the repository SHALL take it from an environment variable, SHALL default to a location derived from this checkout, and SHALL refuse with the resolved path named when nothing is there.

An absolute path that an artefact this repository builds defines — a home directory a `Dockerfile` in this tree creates, and the mounts into it — is a fact about that artefact rather than an assumption about a machine, and SHALL NOT be read as a breach of this requirement. Neither SHALL an illustrative path in a scenario's GIVEN clause, which is a stand-in and not a coordinate.

#### Scenario: A guard refuses a tree that does not declare this application

- **GIVEN** a copy of a script whose guard protects this project's `app_data/`, placed in another project's checkout
- **WHEN** it is run
- **THEN** it SHALL exit non-zero without deleting anything, naming the root it resolved and that the tree's `mix.exs` does not declare this application

#### Scenario: The same guard passes in any checkout of this repository

- **GIVEN** a checkout of this repository at any path
- **WHEN** the script is run
- **THEN** it SHALL proceed, the guard having read the declaration rather than the location

#### Scenario: A near-miss application name does not pass

- **GIVEN** a sibling project whose `mix.exs` declares an application whose name begins with this one's
- **WHEN** the guard runs there
- **THEN** it SHALL refuse, the declaration being matched whole rather than as a prefix

#### Scenario: The tracked tree names no developer's home

- **WHEN** the tracked tree is searched for absolute paths under a user home directory
- **THEN** every hit SHALL be a path defined by an image this repository builds, or an illustrative path inside a scenario, and no hit SHALL be a path only one contributor's machine resolves

### Requirement: A CI job SHALL NOT report success when it could not run

A continuous-integration step SHALL NOT be marked non-blocking in a way that turns a step which can never succeed into a permanent green. A job whose inputs a fresh checkout does not carry is not a lenient gate; it is a gate that has never run, and it is indistinguishable from a passing one.

Every step in a workflow SHALL read only what the checkout carries — the tracked tree, or something the workflow itself creates. A step that reads a path outside `git ls-files` SHALL be removed from CI rather than tolerated, and the check it performed SHALL be recorded as a local instrument if that is what it is.

Where a job is removed for this reason, the workflow SHALL record why in its own text, so that a reader who knows the check exists does not re-add it. `continue-on-error` SHALL remain available for a step that genuinely can both pass and fail and whose failure is not yet blocking; it SHALL NOT be used to hold a broken step in the pipeline.

#### Scenario: A step reading an untracked baseline is removed

- **GIVEN** a workflow step running a ratchet whose baseline file `git ls-files` does not print
- **WHEN** the workflow is reviewed
- **THEN** that step SHALL be removed rather than left under `continue-on-error`, and the workflow SHALL say why the check is not run there

#### Scenario: Every surviving step is checked against a fresh checkout

- **WHEN** a workflow is changed
- **THEN** each remaining step SHALL be shown to read only tracked paths or paths the workflow creates, and the reason SHALL be stated per step rather than assumed for the file

#### Scenario: A non-blocking step that can still fail is kept

- **GIVEN** a step that passes on some trees and fails on others, whose failure is deliberately not blocking yet
- **WHEN** the workflow is reviewed
- **THEN** it SHALL be allowed to keep `continue-on-error`, because it reports a real verdict

### Requirement: A build needing a host package no manifest declares SHALL be reachable through a guarded entry point

A build that depends on a host package which no manifest of this project can declare SHALL be reachable through an entry point that inspects the box before the build starts and either supplies the equivalent from what the box does carry, or refuses naming the missing package and the remedy.

The case this exists for is the Ruby veneer. `rb-sys` runs `bindgen` over `ruby.h`, and bindgen resolves `<stdarg.h>` from clang's builtin header directory, which `libclang` alone does not provide; the raw failure is a `'stdarg.h' file not found` reported against a Ruby header path, so it reads as a broken Ruby rather than a missing clang package. Where the guard cannot be reached — `rb-sys` is a dependency, so its build script runs and fails before any build script this repository owns exists — the entry point SHALL be named in a comment at the site a reader lands on, and the residue SHALL be stated rather than left silent. The prerequisite SHALL NOT be satisfied by committing a literal include path into cargo configuration: such a value encodes one box's compiler version, target triple and distribution layout, and on a box that does carry clang's builtin headers it makes bindgen prefer another compiler's freestanding headers over clang's own.

#### Scenario: The veneer builds on a box without clang's builtin headers

- **WHEN** a cargo command enabling the `ruby` or `ruby-embed` feature of `flz_core_ffi` is run through the guarded entry point on a box that has `libclang` but no clang builtin `stdarg.h`, with no bindgen include variable set
- **THEN** the entry point SHALL derive the include path from the compiler the box does have, say which directory it chose, and the command SHALL succeed

#### Scenario: A Ruby without development headers is diagnosed as itself

- **WHEN** the same entry point runs against a Ruby on `PATH` that has no `ruby.h`
- **THEN** it SHALL refuse before cargo starts, name the missing development headers, and state that this is not the include-path problem

#### Scenario: Neither the builtin headers nor a substitute exists

- **WHEN** the entry point runs on a box with no clang builtin `stdarg.h` and no compiler include directory carrying one
- **THEN** it SHALL refuse naming the package to install, rather than letting the build fail inside a dependency

### Requirement: A toolchain image SHALL carry the coverage driver its test runner needs

A container image this repository builds for running an SDK's test suite SHALL carry the code-coverage driver that suite's runner requires, so a coverage report is producible in the image and not only in an ad-hoc environment.

A coverage diff is how this project decides whether a test may be deleted, so an image that can run a suite but not measure it leaves that SDK unable to take the decision. Where the runner accepts more than one driver, the image SHALL carry the coverage-only one rather than a debugger whose coverage mode is a side effect, because measuring is what the driver is there for and the debugger's cost is paid by every test in the image.

#### Scenario: A coverage report is produced inside the image

- **WHEN** the PHP SDK's suite is run with `--coverage-text` in either PHP toolchain image
- **THEN** the run SHALL report the driver in its runtime line and print a coverage summary, and SHALL NOT warn that no coverage driver is available

#### Scenario: Both targets of a two-target image carry it

- **WHEN** the non-thread-safe and thread-safe PHP targets are each inspected for loaded extensions
- **THEN** both SHALL list the coverage driver

### Requirement: A settled lint check SHALL be run by the gate CI actually invokes

A lint check that has been settled SHALL be added to the script CI runs, and the change
that adds it SHALL cite the workflow line that runs that script. A gate placed in a
script no workflow invokes reports nothing while appearing to be armed, which is worse
than an unsettled check because a reader stops looking.

A lint check SHALL declare how its tool reports a finding as distinct from a failure to
run, so that a linter that crashed is never reported as source that needs fixing.

A gate that both formats and checks SHALL NOT apply lint fixes in its write mode, because
a format pass must stay reproducible by re-running the formatters alone.

#### Scenario: The gate is added where the workflow reads it

- **WHEN** a settled lint check is armed
- **THEN** it SHALL be invoked by the script the format workflow runs, and the change
  SHALL name the workflow line that runs it

#### Scenario: A crashed linter is not reported as unformatted source

- **WHEN** the linter cannot run — its binary is absent, or it exits with its own error
  status
- **THEN** the gate SHALL report it under the tool-could-not-run heading and SHALL NOT
  report the tree as having findings

#### Scenario: The write mode does not rewrite source with lint fixes

- **WHEN** the format script runs in its write mode
- **THEN** the lint check SHALL report and SHALL NOT modify any file

### Requirement: A gated linter's enabled rule set SHALL be fixed by its version pin

A linter SHALL run at a version pinned in the repository, and where the enabled rule set
comes from that version's own default rather than from repository configuration, the
change that arms the gate SHALL record the size of that default and the fact that the pin
is what fixes it.

Raising the pin SHALL be treated as a rule-set change: every family the new version
reports for the first time SHALL be settled — adopted, allowed at a site with a reason,
or refused in configuration with a reason — in the change that raises the pin, never left
to be discovered by an unrelated run.

A refusal SHALL be argued from what the code means. A rule SHALL NOT be disabled because
the number of sites it names is large.

#### Scenario: The default rule set is recorded, not assumed

- **WHEN** a linter is gated without explicit rule selection
- **THEN** the change SHALL record the enabled rule count measured from the pinned
  version, and SHALL name the pin that fixes it

#### Scenario: A version bump settles what it newly reports

- **WHEN** the pinned linter version is raised and the new version reports a family the
  old one did not
- **THEN** that family SHALL be settled in the same change, and the gate SHALL NOT be
  left failing

#### Scenario: A refusal names a meaning, not a count

- **WHEN** a rule is disabled in the linter's configuration
- **THEN** the recorded reason SHALL describe what the code means, and SHALL NOT be the
  number of sites the rule reports

### Requirement: A metric SHALL NOT report a file it silently skipped as measured

Every duplication or quality scan SHALL either measure a file or report that it did
not. A file skipped for its size, its permissions or a tool's own limit SHALL be
named in the run's output, so a skipped file can never be read by the ratchet as an
improvement.

#### Scenario: A tracked file crosses a scanner's size limit

- **WHEN** a scan skips a file because it exceeds the tool's maximum size
- **THEN** the run SHALL report the skip, and the recorded metric SHALL NOT present
  the remainder as a complete measurement

#### Scenario: The skip is visible before it matters

- **WHEN** no tracked file is near the limit
- **THEN** the reporting SHALL still be in place, so the first file to cross it is
  seen rather than silently dropped

### Requirement: An SDK SHALL be able to measure its own test coverage from its declared toolchain

Every SDK whose test runner needs a library to report coverage SHALL declare and pin that library in its own dependency manifest, so a coverage report is producible from a clean checkout of that SDK with no tool the manifest does not name.

A coverage diff is how this project decides whether a test may be deleted. An SDK that can run its suite but not measure it cannot take that decision, and the gap is invisible from the outside: the suite is green, the manifest reads complete, and nothing reports that the measurement is impossible until someone tries to take it.

Where the runner reports coverage without a library — a built-in flag on the language's own test runner — the requirement is satisfied with no manifest entry, and the SDK SHALL record which flag that is so a reader does not read the absence as this defect. Where the driver is a native extension no dependency manifest can supply, it is the toolchain image's obligation and not the manifest's.

Declaring the driver SHALL NOT be read as setting a coverage floor. A threshold is a separate decision, taken on measured evidence, and an SDK satisfies this requirement by being measurable.

The report SHALL be a figure for the package's own sources. A file the suite writes outside the package — a temporary fixture module — SHALL NOT enter the table, because a total over a set that changes between runs is not a measurement anyone can quote.

#### Scenario: A coverage report is producible from the manifest alone

- **WHEN** an SDK's declared development dependencies are installed into a clean checkout and its suite is run in coverage mode
- **THEN** a coverage report SHALL be produced, and the run SHALL NOT report that no coverage tool is available

#### Scenario: A runner with built-in coverage declares nothing

- **GIVEN** an SDK whose test runner reports coverage through a flag of its own
- **WHEN** that SDK's manifest is read
- **THEN** the absence of a coverage dependency SHALL be satisfied by the recorded flag, and SHALL NOT be a defect

#### Scenario: The census names every SDK

- **WHEN** the SDK manifests are audited for a coverage tool
- **THEN** each SDK SHALL either declare one, name the built-in flag it uses, or name the image that carries its driver

#### Scenario: The table lists only the package

- **WHEN** an SDK's suite writes temporary modules outside the package and is run in coverage mode
- **THEN** the report SHALL list the package's own files only, and its total SHALL be over those files

### Requirement: A manifest pin SHALL be read by one reader

Every place continuous integration derives a version from an SDK manifest SHALL
call one shared reader rather than spell its own extraction, so that the anchored
exact-pin pattern the toolchain requirement demands exists once and a pin cannot
resolve to two versions in two jobs. The reader SHALL refuse a pin that is not an
exact version.

#### Scenario: Two jobs read the same pin

- **WHEN** two workflow jobs derive the version of one tool from one manifest
- **THEN** both SHALL obtain the same string, through the same reader

#### Scenario: A relaxed pin is refused

- **WHEN** a manifest entry the reader is asked for is a range rather than an exact version
- **THEN** the reader SHALL exit non-zero and the install step SHALL fail

### Requirement: A durable-state reset in the test harness SHALL be complete when it returns

A test helper that removes durable state SHALL return only when nothing it
terminated can still write to that state and nothing can restore from what it
removed, so that a test which calls it starts from the state it declares
whatever the previous test left running. A reset that returns while a writer is
still flushing, or before a teardown it depends on has finished, produces a
failure that lands on a different test on every run, and a module that passes
alone and fails in the suite cannot be verified by any run of the module.

Such a helper SHALL wait on the exit of what it terminated, never on time.

#### Scenario: An operative stopped by the previous test cannot leak into the next

- **WHEN** a test stops an operative in its teardown, the next test resets durable
  state and starts an operative under the same id
- **THEN** the new operative's Cell SHALL hold no input the previous test sent,
  on every run and at every seed

#### Scenario: The full suite is as green as the module

- **WHEN** a module whose setup resets durable state is run inside the full suite
- **THEN** it SHALL pass at every seed it passes alone, and a failure that appears
  only in the full run SHALL be treated as this defect rather than as load

### Requirement: A module's file SHALL live in the directory that owns its feature

Every module under `lib/flezha/` SHALL sit in the directory that owns the feature it
implements, and a file SHALL remain at `lib/flezha/` only when it is the public facade
of a directory of the same name or the OTP application callback named in `mix.exs`.
One directory SHALL own one feature: where two jobs with different reasons to change
share a directory, they SHALL be separated, or the layer table SHALL name their
modules individually and the change SHALL record why the directory stays whole.

#### Scenario: A new module has one place to go

- **WHEN** a module implementing part of an existing feature is added
- **THEN** it SHALL be written into that feature's directory, without a registration
  step and without a second candidate location

#### Scenario: A file at the top level is a facade

- **WHEN** a file sits directly at `lib/flezha/`
- **THEN** it SHALL be either the OTP application callback or the facade of a
  directory of the same name, and any other top-level file SHALL be moved into the
  directory that owns it

### Requirement: Moving a module's file SHALL NOT change its name

A change that relocates a module SHALL leave its `defmodule` line byte-identical,
because an Elixir file path does not determine a module name and a rename is a
separate decision with a separate cost. A relocation SHALL therefore produce a diff
of renames with no content hunks, and a change that both moves and renames SHALL be
refused and split.

A module name reached over the wire, persisted in durable state, or expanded from
`__MODULE__` into a registry key or a table name SHALL be treated as unchanged by a
move, and the change SHALL prove it by the suite rather than by assertion.

#### Scenario: A move produces a rename-only diff

- **WHEN** a change relocates modules
- **THEN** `git diff -M` SHALL show renames and no content hunk for the moved files,
  and no `defmodule` line SHALL appear in the diff

#### Scenario: A path and a module name may diverge

- **GIVEN** a module whose name records the directory it used to live in
- **WHEN** its file moves to the directory that owns its feature
- **THEN** the divergence SHALL be accepted and recorded, and the rename SHALL be
  left to a change that carries the migration its persistence requires

#### Scenario: A move and a rename are not combined

- **WHEN** a change proposes to relocate a module and rename it at once
- **THEN** the change SHALL be split, so that the move can be proved by a rename-only
  diff and the rename judged on its own migration cost

### Requirement: A test fixture SHALL be declared, not rebuilt in a test's own setup

A test module SHALL obtain every shared fixture — a temporary directory, an
auth store, an environment override, a state tree, a supervised collaborator —
by declaring it against one fixture table, and SHALL NOT build one in its own
`setup` or `setup_all`. The table SHALL live in `test/helpers/`, SHALL hold one
row per fixture naming its setup and its cleanup, and SHALL run every row's
cleanup through a single `on_exit` so that a fixture cannot be set up without
being torn down. A module that must build its own fixture SHALL be named on an
allow-list with a written reason.

A fixture kind that does something beyond creating and removing a resource —
restoring an environment key, detaching a telemetry handler, closing a writer,
uninstalling a stub — SHALL be a row in the table rather than an exception to
it. A caller running on a second BEAM, where `on_exit` raises, SHALL stay
guarded outside the table.

#### Scenario: A test declares the fixtures it needs

- **GIVEN** a test module that needs a temporary `app_data` root and an
  environment override
- **WHEN** it declares `use FlezhaTest.Case, fixtures: [:tmp_app_data, :env]`
- **THEN** both fixtures are set up before the first test and both are removed
  after the last one, with no `on_exit` written in the module

#### Scenario: A hand-built fixture fails the gate

- **GIVEN** a test module whose own `setup` calls `System.tmp_dir!/0` and is not
  on the allow-list
- **WHEN** the gate runs
- **THEN** it reports that module by path and exits with the finding code

#### Scenario: A cleanup that does more than remove a path is a row

- **GIVEN** a fixture that must restore an application environment key it
  changed
- **WHEN** it is added to the table as a row whose cleanup restores the key
- **THEN** every test declaring it gets the restore, and no test file writes its
  own restoring `on_exit`

### Requirement: Every gate SHALL be a row in one runner with one exit contract

A repository gate SHALL be declared to one runner, and the runner SHALL own
running it, walking the tree for it and reporting its result. The exit contract
SHALL be one vocabulary for every gate, and the runner SHALL be where it is
written down: `0` clean,
`1` a finding, `2` a tool the gate needs is absent so its check did not run,
`3` a tool ran and did not complete so its check has no answer. A gate that
cannot reach a verdict for any other reason — a missing input file, an
unreadable baseline — SHALL report `3` as well, since the caller's question is
the same: this check has no answer. CI SHALL fail on `1`, `2` and `3`; a
developer's box MAY skip on `2`. A gate SHALL NOT spell its own exit code and
SHALL NOT carry its own tree walk.

A gate that must read the working tree from disk rather than from the index —
because the sources it judges are not tracked in the same tree, or because an
untracked file must still be judged — SHALL declare that as an option on the
runner rather than implement a second walk.

A gate whose tool is absent SHALL be recorded as not run and SHALL NOT stop the
gates behind it: the runner SHALL run every other requested gate and SHALL list
the absent tools together, each with the command that installs it. The runner
SHALL NOT test every tool before running any gate — that preflight is the
defect removed by `format-gate-tells-the-truth.md`, where one absent formatter
ended the run and said nothing about the six languages behind it.

#### Scenario: CI runs the gates through one call

- **GIVEN** the naming and formatting workflows
- **WHEN** they run the repository's gates
- **THEN** each calls the runner once and no workflow lists gates step by step

#### Scenario: A broken gate is not a clean tree

- **GIVEN** a gate whose input file is missing
- **WHEN** the runner runs it
- **THEN** it exits `3` with a message naming the gate and the missing input,
  and the run is red — never `0`, and never the `2` that means a tool is absent

#### Scenario: An absent local tool is distinguishable from a finding

- **GIVEN** a developer's box without one of the language formatters installed
- **WHEN** the runner runs the formatting gates
- **THEN** every other formatter still runs, it exits `2` naming the absent tool
  and the command that installs it, and the same run in CI is red

#### Scenario: A tool that ran without reaching a verdict is neither

- **GIVEN** a gate whose tool starts, crashes and returns a status its finding
  rule does not claim
- **WHEN** the runner runs it
- **THEN** it exits `3` reporting that check as unanswered, apart from both the
  findings and the absent tools

#### Scenario: A gate that reads disk declares it

- **GIVEN** a gate that judges test sources that are not tracked in this tree
- **WHEN** it is declared with the disk-reading option
- **THEN** the runner walks disk for that gate and the tracked-file walk for
  every other, with one walk implementation

#### Scenario: A gate cannot bypass the contract

- **GIVEN** a gate added to the runner that returns a bare non-zero code with no
  headline
- **WHEN** the runner's self-test runs
- **THEN** the self-test fails naming that gate

### Requirement: A decision that publication makes irreversible SHALL be recorded before the first public release

The repository SHALL carry a publication-readiness record naming every decision that the
first public release makes irreversible, and each entry SHALL be classified `blocking` or
`owed` by one test.

An entry is **blocking** when settling it differently after the first public release would
need at least one of: an action outside the project's control, such as a registry
operator's or another person's consent; an act that cannot be undone, such as recalling a
clone or un-disclosing a vulnerability; or a compatibility break for someone who already
depends on the current answer. Every other entry is **owed**: it should be done, and a
later fix costs no more than an early one. An entry that is merely a good idea is not an
entry at all, and belongs wherever ordinary work is tracked.

Each entry SHALL state its door — which of those three costs it incurs, and in one
sentence why — and SHALL state a check whose answer is yes or no without a debate. A check
is a command, or a named observation someone can make once. An entry whose check needs a
judgement is not an entry: it is an opinion, and a record of opinions is not gated.

An entry that is a decision rather than a task SHALL name who decides and what each option
costs, because the reader of a decision entry is choosing, not being told what to do.

The record is published with the repository, so an entry SHALL be a rule with a check and
SHALL NOT be a list of the instances that trip it. An entry that enumerates its own
violations publishes them, ages the moment one is fixed, and cites paths the tree may not
carry. For the same reason the record SHALL cite only what the published tree carries, and
SHALL NOT cite the defect ledger or any other working document kept out of the repository.

An entry whose decision is settled SHALL name where it was settled — a commit, a
requirement, or a file in the tree. A record whose entries say `settled` and point nowhere
cannot be checked by anyone but its author, which is the failure the defect record already
learned.

#### Scenario: An entry with no door is refused

- **GIVEN** a proposed entry that names no cost incurred by settling it after publication
- **WHEN** the record is reviewed
- **THEN** the entry SHALL be reclassified as ordinary work and SHALL NOT be carried in the record

#### Scenario: A blocking entry's check has one answer

- **WHEN** a blocking entry is read
- **THEN** it SHALL name a command, or a named observation, whose answer is yes or no, and SHALL NOT ask the reader to weigh anything

#### Scenario: A decision entry names its owner and the cost of each option

- **GIVEN** an entry that records a choice rather than a task
- **WHEN** it is read
- **THEN** it SHALL name who decides and what each option costs

#### Scenario: The record states rules, not the instances that trip them

- **WHEN** the record is read
- **THEN** no entry SHALL enumerate the files, paths or commits that currently violate it, and no entry SHALL cite a document the published tree does not carry

#### Scenario: A settled entry names its evidence

- **GIVEN** an entry marked settled
- **WHEN** it is read
- **THEN** it SHALL name the commit, requirement or file that settles it

### Requirement: The publication-readiness record SHALL be gated on its shape now and on its blocking entries at release

A gate SHALL check the record's shape on every pull request, and SHALL refuse a release
tag while any blocking entry is unsettled.

The split is the point. Every blocking entry is open on the day the record lands, so a
gate that judged content on a pull request would be red from its first commit — and a gate
that lands red stops being read, which this project has already paid for once. The
pull-request arm therefore judges shape alone and is green from the first commit: every
entry parses, carries a class, a door, a check and an owner, ids are unique, and a settled
entry names its evidence.

The release arm judges content and has nothing to say until a version tag exists. It SHALL
run before any release artefact is produced, on the job the tag already triggers, so an
unsettled blocking entry stops the release rather than being noticed after the artefact is
published. An owed entry SHALL NOT stop a release: that is the whole difference between
the two classes, and a gate that ignored it would push every entry into `owed` to get a
release out.

The gate SHALL report how many entries it read beside its verdict, so a green that means
"the parser matched nothing" is distinguishable from a green that means the record is
sound. It SHALL be demonstrated to fail — on a malformed entry, on a settled entry with no
evidence, and on a release tag with an open blocking entry — and to pass once each is
repaired, before it is trusted.

#### Scenario: The shape check passes while every blocking entry is open

- **GIVEN** a record in which no entry is yet settled
- **WHEN** the pull-request arm runs
- **THEN** it SHALL exit zero

#### Scenario: An entry missing a required field fails the shape check

- **GIVEN** an entry with no class, no door, no check or no owner
- **WHEN** the pull-request arm runs
- **THEN** it SHALL exit non-zero naming the entry and the missing field

#### Scenario: A settled entry that names no evidence fails

- **GIVEN** an entry marked settled whose evidence column is empty
- **WHEN** the pull-request arm runs
- **THEN** it SHALL exit non-zero naming that entry

#### Scenario: A release tag with an open blocking entry produces no artefact

- **GIVEN** a version tag pushed while one blocking entry is unsettled
- **WHEN** the release job runs
- **THEN** the gate step SHALL exit non-zero naming that entry, and no release artefact SHALL be produced

#### Scenario: An owed entry does not stop a release

- **GIVEN** a version tag pushed while every blocking entry is settled and some owed entries are open
- **WHEN** the release job runs
- **THEN** the gate step SHALL exit zero

#### Scenario: The gate says how much it read

- **WHEN** the gate passes
- **THEN** it SHALL print the number of entries it read and how many are blocking, so a fall in that count is visible without the gate failing

#### Scenario: The gate is proved able to fail before it is trusted

- **WHEN** the gate is added
- **THEN** a malformed entry, a settled entry with no evidence, and an open blocking entry under a release tag SHALL each be introduced, the gate SHALL be shown to exit non-zero naming each, and it SHALL be shown to exit zero once they are repaired

