# runner-send Specification

## Purpose
How application code on a runner sends to an operator: through the controller with an optional
message id and partition key, or locally to an operator bound on that runner, what each
guarantees, and the runner's list of the operators bound on it.
## Requirements
### Requirement: The runner sends to an operator through the controller

The runner object of every SDK SHALL send a message to an operator in its namespace, naming
the operator, the message and the entry channel, through the controller, under the same
role check the control surface's calls take. It SHALL answer the controller's verdict:
`delivered`, `absorbed` or `suspended`, with the operator that took it. It SHALL take an
optional message id, stamped on the input as `flz.msg_id`, so an operator with an input log
that still holds an input of that id absorbs the repeat. It SHALL take an optional
partition key that selects the member of a partition group whose range owns it.
`flz operator send` SHALL take `--msg-id <id>` with the same meaning.

#### Scenario: A message reaches an operator

- **WHEN** application code on a runner bound to role `app` sends `hello` to `my_echo` on
  channel `in`
- **THEN** the call SHALL answer `delivered` naming `my_echo`, and the operator SHALL
  handle the message

#### Scenario: A repeated message id is absorbed

- **WHEN** application code sends a message with id `orders/0/41` to an operator with an
  input log, and sends it again with the same id while the log still holds the first
- **THEN** the first call SHALL answer `delivered`, the second SHALL answer `absorbed`,
  and the operator SHALL handle the message once

#### Scenario: A key reaches the owning member

- **WHEN** application code sends a message with key `EURUSD` to a partition group
- **THEN** the member whose range owns `EURUSD` SHALL take it, and the answer SHALL name
  that member

#### Scenario: A group without a key is refused

- **WHEN** application code sends to a partition group without a key
- **THEN** the call SHALL be refused naming the group's members, as the CLI's is

#### Scenario: The CLI passes a message id

- **WHEN** `flz operator send orders --text x --msg-id a1` runs twice against an operator
  with an input log
- **THEN** the second run SHALL report the input as absorbed

### Requirement: A runner lists the operators bound on it

The runner object of every SDK SHALL answer the ids of the operator instances bound on that
runner at the moment of the call, a partition member by its member id, without contacting
the controller. It SHALL answer an empty list when none is bound.

#### Scenario: Two bound operators are listed

- **WHEN** a runner has `orders` and `prices-1` bound and application code asks for its
  operators
- **THEN** the answer SHALL be exactly `orders` and `prices-1`

#### Scenario: An operator that moved away is no longer listed

- **WHEN** an operator is unbound from the runner and application code asks again
- **THEN** that operator SHALL not be in the answer

### Requirement: A local send skips the controller

The send SHALL take a `local` option. A local send SHALL deliver the message to an operator
bound on that runner in-process, without a frame to the controller, entering where a frame
off the wire enters, so the channel's decoder, transforms and keying run as they do for any
input. It SHALL apply the backpressure that runner's own entry path applies: where a frame
off the wire waits while the consumer's mailbox is full, a local send SHALL wait too. It
SHALL return once the message is routed. State changes and outputs it causes SHALL travel to the controller as
any others do.

#### Scenario: A local message is handled

- **WHEN** application code sends `hello` locally to the entry channel `in` of a bound echo
  operator
- **THEN** the operator SHALL handle it and its output SHALL reach its sink as an output
  from controller-delivered input does

#### Scenario: State changes are kept as usual

- **WHEN** a local message changes a cell's state on a durable operator and the runner
  then stops gracefully
- **THEN** the change SHALL be in the controller's cell log, as any state change is

### Requirement: A local send is refused only for what this runner cannot take

A local send SHALL be refused with `runtime.operator_not_local`, naming the operator, when
the operator is not bound on this runner or names a partition group. A channel the operator
does not declare SHALL be refused with `runtime.channel_undeclared`, and a channel that is
not an entry channel with `runtime.channel_wrong_kind`. A local send given a message id or
a key SHALL be refused before anything is sent. A refused local send SHALL not be forwarded
to the controller.

#### Scenario: An operator on another runner

- **WHEN** application code sends locally to an operator bound on a different runner
- **THEN** the call SHALL be refused with `runtime.operator_not_local` and nothing SHALL be
  sent

#### Scenario: An undeclared channel

- **WHEN** application code sends locally to a channel the operator does not declare
- **THEN** the call SHALL be refused with `runtime.channel_undeclared` listing the declared
  entry channels

#### Scenario: Local with an id

- **WHEN** application code sends locally with a message id
- **THEN** the call SHALL be refused and nothing SHALL be sent

### Requirement: A local input is not durable and is handled at most once

A local input SHALL not be written to the controller's disk and SHALL take no input-log
offset. It SHALL be allowed to every operator, including one with an input log
(`exactly: effectively_once`, a durable steps operator). A crash before it is handled SHALL
lose it, and a replay SHALL never repeat it. On an operator with an input log, the
operator's guarantee SHALL hold for input that came through the controller and SHALL hold
for no local input. Every SDK's reference for the `local` option SHALL say this in one
line.

#### Scenario: A crash loses an unhandled local input

- **WHEN** application code sends locally to an at-least-once flow operator and the runner
  is killed before the input is handled
- **THEN** the operator SHALL rebind and carry on, and the input SHALL not be handled

#### Scenario: An effectively-once operator takes a local input

- **WHEN** application code sends locally to an effectively-once operator, the input is
  handled, and the runner is then killed and the operator replays its input log
- **THEN** the operator SHALL rebind and carry on, the replay SHALL not handle the local
  input again, and controller-delivered input SHALL keep its effectively-once guarantee

#### Scenario: A durable steps operator takes a local input

- **WHEN** application code sends locally to a durable steps operator waiting for input,
  and the runner is killed after the step it started commits
- **THEN** the operator SHALL resume after that committed step

### Requirement: The docs show the new names and warn about the local send

A docs page SHALL show runner code sending to an operator: through the controller with a
message id and a key first, the local send second, preceded by a warning box that says its
input is not on the controller's disk, is lost on a crash, and is not covered by the
guarantee of an operator with an input log. No docs page, SDK example or SDK reference
comment SHALL show `admin()` or a send on the control surface.

#### Scenario: The warning is on the page

- **WHEN** the docs are built
- **THEN** the page SHALL carry a `{: .warning}` box above the local send's first example

#### Scenario: No old name is left

- **WHEN** `docs/`, `README.md`, `CHANGELOG.md`'s new entry, `sdk/` and `examples/` are
  searched for the SDK accessor `admin`
- **THEN** no hit SHALL remain outside the authorization roles and earlier changelog entries

