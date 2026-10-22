# runner-send Specification

## Purpose
How application code on a runner sends to a relay: through the controller with an optional
message id and partition key, or locally to a relay bound on that runner, what each
guarantees, and the runner's list of the relays bound on it.
## Requirements
### Requirement: The runner sends to a relay through the controller

The runner object of every SDK SHALL send a message to a relay in its namespace, naming
the relay, the message and the entry channel, through the controller, under the same
role check the control surface's calls take. It SHALL answer the controller's verdict:
`delivered`, `absorbed` or `suspended`, with the relay that took it. It SHALL take an
optional message id, stamped on the input as `flz.msg_id`, so a relay with an input log
that still holds an input of that id absorbs the repeat. It SHALL take an optional
partition key that selects the member of a partition group whose range owns it.
`flz relay send` SHALL take `--msg-id <id>` with the same meaning.

#### Scenario: A message reaches a relay

- **WHEN** application code on a runner bound to role `app` sends `hello` to `my_echo` on
  channel `in`
- **THEN** the call SHALL answer `delivered` naming `my_echo`, and the relay SHALL
  handle the message

#### Scenario: A repeated message id is absorbed

- **WHEN** application code sends a message with id `orders/0/41` to a relay with an
  input log, and sends it again with the same id while the log still holds the first
- **THEN** the first call SHALL answer `delivered`, the second SHALL answer `absorbed`,
  and the relay SHALL handle the message once

#### Scenario: A key reaches the owning member

- **WHEN** application code sends a message with key `EURUSD` to a partition group
- **THEN** the member whose range owns `EURUSD` SHALL take it, and the answer SHALL name
  that member

#### Scenario: A group without a key is refused

- **WHEN** application code sends to a partition group without a key
- **THEN** the call SHALL be refused naming the group's members, as the CLI's is

#### Scenario: The CLI passes a message id

- **WHEN** `flz relay send orders --text x --msg-id a1` runs twice against a relay
  with an input log
- **THEN** the second run SHALL report the input as absorbed

### Requirement: A runner lists the relays bound on it

The runner object of every SDK SHALL answer the ids of the relay instances bound on that
runner at the moment of the call, a partition member by its member id, without contacting
the controller. It SHALL answer an empty list when none is bound.

#### Scenario: Two bound relays are listed

- **WHEN** a runner has `orders` and `prices-1` bound and application code asks for its
  relays
- **THEN** the answer SHALL be exactly `orders` and `prices-1`

#### Scenario: A relay that moved away is no longer listed

- **WHEN** a relay is unbound from the runner and application code asks again
- **THEN** that relay SHALL not be in the answer

### Requirement: A local send skips the controller

The send SHALL take a `local` option. A local send SHALL deliver the message to a relay
bound on that runner in-process, without a frame to the controller, entering where a frame
off the wire enters, so the channel's decoder, transforms and keying run as they do for any
input. It SHALL apply the backpressure that runner's own entry path applies: where a frame
off the wire waits while the consumer's mailbox is full, a local send SHALL wait too. It
SHALL return once the message is routed. State changes and outputs it causes SHALL travel to the controller as
any others do.

#### Scenario: A local message is handled

- **WHEN** application code sends `hello` locally to the entry channel `in` of a bound echo
  relay
- **THEN** the relay SHALL handle it and its output SHALL reach its output channel as an output
  from controller-delivered input does

#### Scenario: State changes are kept as usual

- **WHEN** a local message changes a cell's state on a durable relay and the runner
  then stops gracefully
- **THEN** the change SHALL be in the controller's cell log, as any state change is

### Requirement: A local send is refused only for what this runner cannot take

A local send SHALL be refused with `runtime.relay_not_local`, naming the relay, when
the relay is not bound on this runner or names a partition group. A channel the relay
does not declare SHALL be refused with `runtime.channel_undeclared`, and a channel that is
not an entry channel with `runtime.channel_wrong_kind`. A local send given a message id or
a key SHALL be refused before anything is sent. A refused local send SHALL not be forwarded
to the controller.

#### Scenario: A relay on another runner

- **WHEN** application code sends locally to a relay bound on a different runner
- **THEN** the call SHALL be refused with `runtime.relay_not_local` and nothing SHALL be
  sent

#### Scenario: An undeclared channel

- **WHEN** application code sends locally to a channel the relay does not declare
- **THEN** the call SHALL be refused with `runtime.channel_undeclared` listing the declared
  entry channels

#### Scenario: Local with an id

- **WHEN** application code sends locally with a message id
- **THEN** the call SHALL be refused and nothing SHALL be sent

### Requirement: A local input is not durable and is handled at most once

A local input SHALL not be written to the controller's disk and SHALL take no input-log
offset. It SHALL be allowed to every relay, including one with an input log
(`exactly: effectively_once`, a durable steps relay). A crash before it is handled SHALL
lose it, and a replay SHALL never repeat it. On a relay with an input log, the
relay's guarantee SHALL hold for input that came through the controller and SHALL hold
for no local input. Every SDK's reference for the `local` option SHALL say this in one
line.

#### Scenario: A crash loses an unhandled local input

- **WHEN** application code sends locally to an at-least-once flow relay and the runner
  is killed before the input is handled
- **THEN** the relay SHALL rebind and carry on, and the input SHALL not be handled

#### Scenario: An effectively-once relay takes a local input

- **WHEN** application code sends locally to an effectively-once relay, the input is
  handled, and the runner is then killed and the relay replays its input log
- **THEN** the relay SHALL rebind and carry on, the replay SHALL not handle the local
  input again, and controller-delivered input SHALL keep its effectively-once guarantee

#### Scenario: A durable steps relay takes a local input

- **WHEN** application code sends locally to a durable steps relay waiting for input,
  and the runner is killed after the step it started commits
- **THEN** the relay SHALL resume after that committed step

### Requirement: The docs show the new names and warn about the local send

A docs page SHALL show runner code sending to a relay: through the controller with a
message id and a key first, the local send second, preceded by a warning box that says its
input is not on the controller's disk, is lost on a crash, and is not covered by the
guarantee of a relay with an input log. No docs page, SDK example or SDK reference
comment SHALL show `admin()` or a send on the control surface.

#### Scenario: The warning is on the page

- **WHEN** the docs are built
- **THEN** the page SHALL carry a `{: .warning}` box above the local send's first example

#### Scenario: No old name is left

- **WHEN** `docs/`, `README.md`, `CHANGELOG.md`'s new entry and `sdk/` are
  searched for the SDK accessor `admin`
- **THEN** no hit SHALL remain outside the authorization roles and earlier changelog entries

### Requirement: Every SDK SHALL call a relay's channel by value

Every SDK's runner object SHALL offer, beside its send, a call of a relay's channel with a
value (`runner.call(id, channel, value, answerType)` in Java, the language's idiom elsewhere),
carried over the SDK's shared entry as `Relay.call_channel` under the role check its send
takes. The value SHALL be encoded as an output with no codec encodes it, and the answer decoded by
its format: JSON into the requested type or the language's JSON value, text as a string, bytes
as bytes, and `{"accepted": true}` for a send. A refusal SHALL raise the SDK's error type with
the controller's code. The control surface SHALL not offer this call.

#### Scenario: Java calls with a record

- **WHEN** a Java runner calls `runner.call("orders-eu", "place", new PlaceOrder("EUR/USD", 1000), Answer.class)`
- **THEN** it receives the answer as an `Answer` record

#### Scenario: Same in every SDK

- **WHEN** a test in each SDK calls a JSON channel of a test relay through its
  runner object
- **THEN** each receives the same decoded answer

### Requirement: A runner's send SHALL reach the relay byte for byte

The core SHALL carry the payload of every SDK's runner `send` to `Relay.send` as base64 with the
`payload: "base64"` option `Relay.call_channel` already takes, so a send and a call encode a payload
one way and whatever an SDK's `send` accepts reaches the relay byte for byte: a text message as its
UTF-8 bytes, and a byte string from an SDK whose `send` takes one (Ruby, PHP, Elixir) unchanged even
when it is not UTF-8. No SDK's `send` signature changes. `Relay.send`
SHALL read `payload: "text"` (the default, as `flz relay send --text` sends) and `"base64"`, and
refuse any other value with `usage.unknown_word`.

#### Scenario: A binary message reaches the relay

- **WHEN** Elixir application code sends the binary `<<0xFF, 0x00, 0x7F>>` to `my_echo` on channel `in`
- **THEN** the call SHALL answer `delivered` and the relay SHALL receive exactly those three bytes

