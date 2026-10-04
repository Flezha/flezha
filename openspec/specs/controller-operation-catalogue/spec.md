# controller-operation-catalogue Specification

## Purpose

Every published controller operation is described once, where its function binds its
authorization row: its arguments, its one-line description, whether it streams and whether
it may be reached over the network. The catalogue built from those declarations is what the
door resolves a name through, what `flz help --json` prints, and what an MCP tool list or a
JSON API reads, so no surface keeps a list of its own.

## Requirements
### Requirement: Every published operation SHALL be described once, on its binding

Every published function SHALL declare, where it binds its row, the positional arguments it
takes before the caller context (each with its type, whether it is required, its default and
a one-line description, in the vocabulary plugin options use) and whether it answers a
subscription. Its description SHALL be the first sentence of its `@doc`. The catalogue SHALL
be built from these declarations and from the rows, and SHALL NOT be written anywhere else by
hand.

Each catalogue entry SHALL carry the operation's name in the door's wire form
(`"<Module>.<function>"`), its arguments, its description, its action, whether it streams,
and whether it may be reached over the network.

#### Scenario: A binding whose arguments do not match its arity fails the suite

- **WHEN** a published function takes three arguments before its context and its binding
  declares two
- **THEN** the declaration test SHALL fail naming the module, the function and both counts

#### Scenario: A published function without a description fails the suite

- **WHEN** a published function has no `@doc`
- **THEN** the declaration test SHALL fail naming it

#### Scenario: The catalogue answers at runtime in a release

- **WHEN** `Flezha.Service.catalogue/0` is called on a controller started from a release
- **THEN** it SHALL answer every published operation with its description, although the
  release carries no documentation chunks

### Requirement: A row SHALL say whether its verb may be reached over the network

Every row of the verb declaration SHALL carry `remote`, true unless declared otherwise. The
rows of stopping the controller, issuing and revoking tokens, binding and unbinding roles,
deleting a namespace and claiming the first admin SHALL declare `remote: false`. The flag
SHALL live in the row, so that no list of forbidden verbs is kept beside the declaration.

#### Scenario: A non-remote verb is marked in the catalogue

- **WHEN** the catalogue is read
- **THEN** the entry of `Controller.stop` SHALL carry `remote: false`, and the entry of
  `Dlq.list` SHALL carry `remote: true`

### Requirement: The CLI SHALL ship a generated copy of the catalogue

`mix flezha.catalogue` SHALL write the catalogue to `cli/catalogue.json`, and the suite SHALL
fail when the committed file differs from a fresh generation. `flz` SHALL embed the file, and
`flz help --json` SHALL print it without reaching a controller.

#### Scenario: A stale catalogue file fails the suite

- **WHEN** a published function gains an argument and `cli/catalogue.json` is not
  regenerated
- **THEN** the catalogue test SHALL fail naming the entry that differs

#### Scenario: Help as data works offline

- **WHEN** `flz help --json` is run on a machine where no controller runs
- **THEN** it SHALL print the catalogue as JSON and exit 0

### Requirement: The CLI SHALL reach the controller only through the door

The controller side of `flz` SHALL call nothing but the door, the catalogue, its own
renderers and the standard library. A test SHALL read the compiled imports of every CLI
module and fail on a call outside that set. Every door name `flz` sends SHALL be a catalogue
entry, and a Rust test SHALL fail on a verb row that sends a name, or a positional count, the
catalogue does not declare.

#### Scenario: A CLI module that reads a store fails the suite

- **WHEN** a module under `Flezha.Service.Cli` calls `Flezha.Dlq.Store.list/1`
- **THEN** the import test SHALL fail naming the module and the call

#### Scenario: A Rust verb that sends an unknown name fails the build's tests

- **WHEN** a verb row in `cli/src` sends `"Dlq.lst"`
- **THEN** the Rust catalogue test SHALL fail naming the verb and the name

