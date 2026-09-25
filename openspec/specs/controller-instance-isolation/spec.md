# controller-instance-isolation Specification

## Purpose
Defines what one controller instance owns exclusively on a machine, so that several may share
one host on purpose and none may share a resource by accident. Covers the per-instance variable
set (one directory tree and one node name per controller), the two guards that turn a missed
variable into a refusal rather than damage — a socket directory a live peer answers in, and an
exclusive lock on the state root — and the normative statement that a peer controller is
capacity and isolation, never a substitute for the remote durability tier.
## Requirements
### Requirement: A machine may host several controllers, each owning its own resources

Several controller instances MAY run on one machine. Each SHALL own an exclusive set of
resources, selected by environment: socket directory, state root, hub registry root, DLQ
directory, snapshot directory, node name, healthz port, and — where enabled — QUIC listen
address, authz directory, schedule directory, audit directory and plugin root. The unit of
separation SHALL be the directory rather than a filename within a shared directory, so that the
socket directory remains the UDS access boundary for exactly one instance.

The documentation SHALL state the complete variable set in one place, expressed as one directory
tree and one node name per controller.

#### Scenario: Two controllers coexist on one machine
- **WHEN** two controllers are started on one host, each with its own socket directory, state root, hub registry root, node name and healthz port
- **THEN** both reach ready, each serves the runners attached to its own socket directory, and neither observes the other's sockets or state

#### Scenario: Each instance's socket directory is its own access boundary
- **WHEN** two co-located controllers run under different user accounts
- **THEN** each socket directory permits access according to its own owner and mode, and neither user can traverse the other's

### Requirement: A controller refuses to start over a live peer's socket directory

Before preparing its socket directory a controller SHALL probe any control socket present. When a
live peer accepts the probe the controller SHALL refuse to start, naming the resolved absolute
path, the peer it found and the variable that separates instances, and SHALL NOT remove any file.
When no process is listening the socket is stale and the directory SHALL be prepared as normal, so
a controller that died hard needs no manual cleanup before restarting.

#### Scenario: A shared socket directory is refused, not clobbered
- **WHEN** a controller is started with the socket directory of a controller that is already running and serving runners
- **THEN** it does not boot, its refusal names the path and the variable, and the running controller's control socket and its runners' stream sockets are untouched

#### Scenario: A stale socket directory is still cleared
- **WHEN** a controller is started with a socket directory holding sockets left by a controller that was killed without cleanup
- **THEN** the probe finds nothing listening, the directory is prepared, and the controller reaches ready

#### Scenario: Runners survive the refusal
- **WHEN** the refusal above occurs while runners are attached to the running controller
- **THEN** no attached runner loses its control channel or its data streams

### Requirement: A controller holds an exclusive lock on its state root

A controller SHALL resolve its state root to an absolute path once, at the point the
root is read, so that the advisory lock, the refusal it raises, `held/0` and every
durable path built from the root name one directory rather than one per working
directory. It SHALL acquire an exclusive advisory lock on that resolved root at boot
and hold it for the process lifetime, so that no two controllers write one cell WAL
tree. The lock SHALL be released by the operating system when the holding process
exits, requiring no stale-lock recovery. A controller that cannot acquire the lock
SHALL refuse to start, naming the resolved state root and the holder.

#### Scenario: A shared state root is refused
- **WHEN** a second controller is started with the state root of a running controller
- **THEN** it does not boot and the refusal names the state root and the holding process

#### Scenario: The lock does not survive its holder
- **WHEN** a controller holding the lock is killed without cleanup and another is started with the same state root
- **THEN** the second controller acquires the lock and reaches ready

#### Scenario: A single controller is unaffected
- **WHEN** one controller boots, runs and restarts normally
- **THEN** the lock is acquired on each boot with no user action

#### Scenario: The lock and the data name one directory
- **WHEN** a controller boots with a relative state root configured
- **THEN** the path it locks and the path its WAL, snapshot and outbox files are written to are the same absolute directory

### Requirement: A peer controller is capacity, not durability

The documentation SHALL state that co-locating or adding controllers provides capacity and
isolation and SHALL NOT be relied on to survive the loss of a controller with its disk: the hub's
redundancy strategy runs each operator on exactly one node with no replicas, and cross-controller
state transfer requires the source controller to be alive to serve it. It SHALL name the remote
durability tier as the mechanism that does survive that loss.

This statement SHALL appear both where co-location is documented and on the remote durability
tier's page, so a user reaching the question from either direction meets it.

#### Scenario: The co-location documentation names the limit
- **WHEN** the documentation describing several controllers on one machine is read
- **THEN** it states that a peer controller is not a failover target, gives both reasons, and names the remote durability tier

#### Scenario: The durability page carries the same statement
- **WHEN** the remote durability tier's page is read
- **THEN** it states that adding controllers does not substitute for the tier

### Requirement: Every contested resource a test run holds SHALL derive from the run

A resource that only one process on a machine can hold SHALL be selected from the run's own environment rather than written as a constant in the harness, so that two suites can execute on one checkout at the same time.

The controller's state root, socket directory and QUIC listen address already derive this way. A network port a test daemon binds is the same kind of resource and SHALL be selected the same way: a helper that spawns such a daemon SHALL pass the selected value into the daemon's environment, and every test module that dials the daemon SHALL read the same value rather than repeat the literal.

A daemon whose port is a constant makes a second concurrent run bind-fail. Where the failing tests are excluded by default the failure is not red — it is a stack trace in the output of every concurrent run, and a run that wants those tests cannot have them at all.

A distribution node name is the same kind of resource: one VM on a host holds it, and a second run that asks for it gets no distribution at all, so every test that needs a peer fails with a message that never names the cause. The name SHALL derive from the run, and the harness and every helper that repeats it SHALL read one source.

#### Scenario: Two suites run at once without a port collision

- **WHEN** two suites run on one checkout at the same time, each with its own isolation environment
- **THEN** neither test daemon SHALL fail to bind, and neither run's output SHALL carry an address-in-use trace

#### Scenario: The daemon receives the port the run selected

- **WHEN** the harness spawns the test daemon
- **THEN** the selected port SHALL reach the daemon's environment, and SHALL NOT be left to the daemon's own default

#### Scenario: A dialling test reads the selected port

- **WHEN** a test module connects to the test daemon
- **THEN** it SHALL take the port from the same source the harness spawned it with, and SHALL NOT hardcode the literal

#### Scenario: Two suites run at once and both are distributed

- **WHEN** two suites run on one host at the same time
- **THEN** each SHALL start distribution under its own node name, and neither run's output SHALL carry a `:nodistribution` report

### Requirement: A controller names its state root at boot

A controller SHALL report the absolute path of its resolved state root on every boot,
and SHALL state explicitly when that directory holds no durable state, so that a cold
start is never silent. The report SHALL name the environment variable that overrides
the root. The same resolved path SHALL be readable from `flz controller status` while the
controller runs, so that a controller SHALL NOT depend on the user having observed a
boot to know where its durable state lives.

#### Scenario: The state root is named on every boot

- **WHEN** a controller boots
- **THEN** its output names the absolute state root in use and the environment
  variable that sets it, whether the root was configured or defaulted

#### Scenario: A cold start says so

- **WHEN** a controller boots against a state root holding no durable state
- **THEN** its output says the directory is empty and that the controller is
  starting cold

#### Scenario: The state root is readable without watching a boot

- **WHEN** a user runs `flz controller status` against a running controller
- **THEN** the output names the absolute state root in use, alongside the durability
  posture it already reports

#### Scenario: A relocated working directory is diagnosable

- **WHEN** a controller using the default relative state root is restarted from a
  different working directory
- **THEN** the boot names an absolute state root differing from the previous boot's,
  and reports it as empty, so the user can see which directory the durable state
  is in

