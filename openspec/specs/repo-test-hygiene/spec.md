# repo-test-hygiene Specification

## Purpose

How the repository's tests behave. A test restores any process-global mode it changes, scopes its
observation of a process-global signal, stops the workers it starts and removes the temporary
directories it creates. A failure-path test asserts its diagnostic, and an assertion of absence or a
log gate is shown able to fail. A plain test run emits no uncaptured error log, a minted identifier
fits its subject's limit, a durable-state reset is complete when it returns, fixtures are declared
rather than rebuilt in a test's setup, and a test that uses a Linux-only interface is compiled for
Linux alone.

## Requirements

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

### Requirement: A durable-state reset in the test harness SHALL be complete when it returns

A test helper that removes durable state SHALL return only when nothing it
terminated can still write to that state and nothing can restore from what it
removed, so that a test which calls it starts from the state it declares
whatever the previous test left running. A reset that returns while a writer is
still flushing, or before a teardown it depends on has finished, produces a
failure that lands on a different test on every run, and a module that passes
alone and fails in the suite cannot be verified by any run of the module.

Such a helper SHALL wait on the exit of what it terminated, never on time.

#### Scenario: A relay stopped by the previous test cannot leak into the next

- **WHEN** a test stops a relay in its teardown, the next test resets durable
  state and starts a relay under the same id
- **THEN** the new relay's Cell SHALL hold no input the previous test sent,
  on every run and at every seed

#### Scenario: The full suite is as green as the module

- **WHEN** a module whose setup resets durable state is run inside the full suite
- **THEN** it SHALL pass at every seed it passes alone, and a failure that appears
  only in the full run SHALL be treated as this defect rather than as load

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

- **GIVEN** a test module that needs a temporary data root and an
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

### Requirement: A test SHALL depend on a Linux-only interface only when it is compiled for Linux alone

A Rust test SHALL be compiled only for `target_os = "linux"`, together with any helper only such
tests use, when it reads `/proc` or relies on behaviour only Linux has (`RLIMIT_NPROC` limiting
threads, CPU affinity). A test that only needs a fixture with a property SHALL build
that fixture portably instead of borrowing a Linux path for it:
- a directory this process cannot enter: a temporary directory with no permissions, restored
  before it is removed
- a directory candidate that cannot be created: a path under a regular file
- a process that is alive: `kill -0`

#### Scenario: A denied socket directory needs no /proc
- **WHEN** the socket probe, `flz runner check` or `flz reset --include-sockets` is tested against a directory this process cannot enter, run as a non-root user
- **THEN** the fixture is a temporary directory with mode `000` created by the test, the probe reports access denied with the `FLEZHA_SOCKET_DIR_GROUP` fix, and the directory is removed after the test

#### Scenario: A Linux mechanism stays tested on Linux
- **WHEN** the workspace's tests are compiled for macOS
- **THEN** the tests that count threads through `/proc/self/task` or stop thread creation through `RLIMIT_NPROC` are not compiled, and on Linux they run as before
