# controller-configuration Specification

## Purpose
TBD - created by archiving change read-every-knob-once. Update Purpose after archive.
## Requirements
### Requirement: Every configuration knob SHALL be one row of one table, read through one function

Every knob the controller reads SHALL be declared exactly once as a row of
`Flezha.Config.Keys` — an environment variable, an application-env key, or both —
carrying its name, application-env key, environment variable, type, default, the
runtimes that read it, the operator surface that shows it and its documentation line.
Every controller-side read SHALL go through `Flezha.Config.get/1` by row name;
`config/runtime.exs` SHALL derive its `:flezha` configuration from the table rather than
translate variables by hand; a keyword-list knob SHALL be a row per child key, never a
raw read followed by a lookup. A read of a `FLEZHA_*` variable or a `:flezha` key outside
the table's own files SHALL fail the seam test naming the file, the line and the row it
should read. A variable whose NAME is itself an operator-supplied setting value (a secret
indirection) is not a knob and SHALL be exempt by shape.

#### Scenario: A knob is added by writing one row

- **WHEN** a developer adds a row with an environment variable, a type and a default
- **THEN** `config/runtime.exs` parses that variable at boot, `Flezha.Config.get/1` answers
  it by name, the documentation gate requires its line, and the end-to-end drill observes
  it on its declared surface, with no other file edited

#### Scenario: A read beside the seam is refused

- **WHEN** a module under `lib/` reads `Application.get_env(:flezha, :healthz_bind, "127.0.0.1")`
  or `System.get_env("FLEZHA_HEALTHZ_BIND")` directly
- **THEN** the seam test fails naming the file, the line and the row `:healthz_bind`

#### Scenario: A malformed variable is refused at boot, naming itself

- **WHEN** the controller boots with `FLEZHA_TRANSPORT=tcp`
- **THEN** `config/runtime.exs` raises a message naming `FLEZHA_TRANSPORT`, the value and
  the accepted members, and the node does not start

### Requirement: A key SHALL have one default, and every document SHALL render that default

A row's default SHALL be the only default its key has: no read site SHALL carry a
literal fallback, no environment SHALL skip the row so that a reader's own literal wins,
and a per-environment value SHALL be declared on the row rather than in a per-environment
config file. Where a knob is read by two roles with different needs, the row SHALL declare
one default per role and the documentation SHALL state both. Every table in
`docs/configuration.md`, `docs/operations/*.md` and `docs/internals/*.md` that restates a
default SHALL render the row's default, and the seam test SHALL fail on a cell that does
not.

#### Scenario: The health listener binds the documented address in every environment

- **WHEN** the controller boots under `mix test` with `FLEZHA_HEALTHZ_BIND` unset and the
  `/healthz` listener enabled
- **THEN** the listener binds `0.0.0.0`, the documented default, not a literal of its own

#### Scenario: A documented default that drifts from the row fails the gate

- **WHEN** `docs/configuration.md` states `FLEZHA_HEALTHZ_PORT` defaults to `9570` while the
  row's default is `9569`
- **THEN** the seam test fails naming the document line and the row

#### Scenario: A late default is evaluated when read, not when configured

- **WHEN** `FLEZHA_DURABILITY_WRITER_ID` is unset
- **THEN** `Flezha.Config.get(:durability_writer_id)` answers the node's name at the time of
  the read, after distribution has started

### Requirement: A per-run resource SHALL derive from one run token through one entry, and an explicit variable SHALL win in every environment

Each per-run resource SHALL be one derivation entry over the one run token
`config/runtime.exs` mints — the state root, the socket directory, the QUIC listen
address, the testnet port, the node base name and the ProcessHub registry root — applied
only when the resource's variable is unset, in `:test` as in every other environment. A
peer BEAM a test starts SHALL take its resources from the same entry keyed by its derived
name, never from a hand-spelled constant, and no test configuration hook SHALL overwrite
a resource the table derived.

#### Scenario: The socket directory variable is honoured under `mix test`

- **WHEN** a suite runs with `FLEZHA_SOCKET_FILES_PATH=/some/dir` set
- **THEN** `Flezha.Socket.Support.socket_dir/0` answers `/some/dir` and the controller's
  control socket is created there

#### Scenario: A peer's six resources come from the same derivation

- **WHEN** a test starts a peer BEAM through `NodeTestHelper.start_peer_node/2`
- **THEN** the peer's state root, socket directory, QUIC address, testnet port, node name
  and registry root are the table's derivation for that peer name, each distinct from the
  primary's, and no two peers of one run share any of the six

#### Scenario: The registry root is per run and needs no wipe

- **WHEN** two suites run at once on one checkout
- **THEN** each writes its ProcessHub registry under its own state root, and no boot removes
  another run's registry

### Requirement: A name read in more than one runtime SHALL be declared once, with its readers and one parse rule per type

A `FLEZHA_*` name SHALL be a row whose `runtimes` field names every reader — the
controller, the Elixir SDK, a Rust crate, the Java SDK, a veneer SDK, a shell wrapper,
the CLI, a bench task or the test harness. Each runtime SHALL read the operating-system
environment with its own reader, and SHALL parse by the one rule the row's type states —
a positive integer that is unparsable or below one reads as the default in every runtime —
pinned by one shared case fixture each runtime's suite feeds to its reader. A default
literal a runtime keeps beside its read SHALL equal the row's default. A name a runtime's
source reads that no row declares for that runtime SHALL fail the seam test.

#### Scenario: A runtime reads a name the table does not declare for it

- **WHEN** `sdk/ruby` gains a read of `FLEZHA_NEW_KNOB` and no row lists `:ruby` for it
- **THEN** the seam test fails naming the file and the name

#### Scenario: Every runtime answers "0" the same way

- **WHEN** `FLEZHA_WORKER_POOL_SIZE=0` is set for a Rust, Java, Python, Ruby, Node, PHP or
  Elixir runner
- **THEN** each reads its documented default, and the shared fixture's row for `0` passes in
  every runtime's suite

#### Scenario: A runner and the controller default the log level per role

- **WHEN** `FLEZHA_LOG_LEVEL` is unset
- **THEN** the controller's file log is at `info`, every SDK's runner console logger is at
  `warn`, and the documentation row states both

### Requirement: Every documented variable SHALL be observed on the surface its row declares

For every row with an environment variable, setting a non-default value SHALL be
observable: through `config/runtime.exs` evaluated the way a release evaluates it, at the
row's key; and, for a row that declares an operator surface, on that surface of a booted
controller — `flz controller status`, `/healthz`, `flz observe` or the directory on disk.
A row that no reader consults SHALL NOT exist: a variable set by `runtime.exs` and read by
nothing is deleted with its documentation line rather than kept inert.

#### Scenario: The drill drives every row through the real boot entry

- **WHEN** the end-to-end drill sets every row's variable to a non-default value and
  evaluates `config/runtime.exs` for `:prod` and for `:test`
- **THEN** every row's key holds the parsed value in both, and no row is skipped by an
  environment guard

#### Scenario: A surfaced row is read back where the operator would read it

- **WHEN** the drill boots a controller with `FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS=120000` and
  `FLEZHA_HEALTHZ_BIND=127.0.0.2`
- **THEN** `flz controller status` reports `graceful_shutdown_timeout_ms: 120000` and
  `/healthz` answers on `127.0.0.2`

#### Scenario: An inert variable is removed rather than documented as inert

- **WHEN** a row's key is read by nothing under `lib/`
- **THEN** the row, its `runtime.exs` line and its documentation row are deleted in the same
  change, and the documentation gate would fail on the name thereafter

### Requirement: Every Erlang VM Argument the Release Sets SHALL Be Declared in One Place and Rendered by the Configuration Reference

The Erlang VM arguments the release sets SHALL be declared in `rel/vm.args.eex` and `rel/remote.vm.args.eex` and nowhere else, and every flag either file sets SHALL appear in the configuration reference with its value and its reason.

The VM is configured before the application starts, so no flag here can be a row
of `lib/flezha/config/keys.ex` and none of the four configuration layers already
covers it. That is precisely why it rots quietly: no compile step and no suite
reads an args file, and the reference has never mentioned one. A flag that ships
undocumented is a behaviour change nobody can find, and a flag the reference
claims the release sets when it does not is worse, because an operator will
spend the afternoon overriding a default that was never there.

#### Scenario: A flag is added to the release and not documented

- **WHEN** a flag is set in either args template and does not appear in the configuration reference's VM-arguments table
- **THEN** the VM-arguments gate SHALL fail, naming the flag and the template that sets it

#### Scenario: The reference claims a shipped flag the release does not set

- **WHEN** the configuration reference's VM-arguments table presents a flag as shipped and neither args template sets it
- **THEN** the VM-arguments gate SHALL fail, naming the flag

#### Scenario: A flag the reference discusses but does not present as shipped

- **GIVEN** a flag the reference names only as an option an operator may set
- **WHEN** the VM-arguments gate runs
- **THEN** the gate SHALL pass, because only flags presented as shipped are held against the templates

### Requirement: An Operator SHALL Be Able to Override Any Shipped VM Argument Without Rebuilding, Through One Documented Route

An operator SHALL be able to change any VM argument the release ships by setting `ERL_FLAGS` in the process environment, and the configuration reference SHALL state the precedence rule that makes this work and SHALL state that `ELIXIR_ERL_OPTIONS` does not override a flag an args file sets.

Three spellings exist and only two of them win, which is not guessable and was
not written down. The rule is one sentence once measured: flags are concatenated
and the last occurrence wins, `ELIXIR_ERL_OPTIONS` is placed before the args
file, and `ERL_FLAGS` after it.

#### Scenario: An operator overrides a shipped default

- **GIVEN** an args template that sets a scheduler count
- **WHEN** the node is started with `ERL_FLAGS` setting a different scheduler count
- **THEN** the node SHALL run with the operator's value

#### Scenario: The route that does not work is documented as not working

- **GIVEN** an args template that sets a scheduler count
- **WHEN** the node is started with `ELIXIR_ERL_OPTIONS` setting a different scheduler count and no other override
- **THEN** the node SHALL run with the template's value, and the configuration reference SHALL say so

#### Scenario: The whole file is replaced

- **WHEN** `RELEASE_VM_ARGS` names a readable args file
- **THEN** the node SHALL start with that file's flags in place of the release's own, and the reference SHALL name this as the route for a deployment whose root filesystem is read only

