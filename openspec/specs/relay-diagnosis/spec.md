# relay-diagnosis Specification

## Purpose
`Relay.diagnose`: one relay's verdict and findings, read from facts the controller already
keeps, each finding a catalogued `diagnose.*` code with its hint, for a person and for an AI
assistant alike.
## Requirements
### Requirement: `Relay.diagnose` SHALL answer a verdict and findings from existing facts

`Relay.diagnose(namespace, relay_id, caller)` SHALL be a published, remote, non-streaming
catalogue operation judged as `relay.list`, writing no audit record. It SHALL read only the
convergence loop's published verdicts for the relay, its runner and its bound plugins, the
runners announcing the relay's factory, the relay's dead letters, its input log's waiting
inputs and, for a finished relay, its completion record, and SHALL answer
`{relay, verdict, findings}` with `verdict` one of `ok`, `warning`, `problem` (the highest finding level, `ok` when only notes) and each finding
`{level, code, message, details, hint}` with `message` and `hint` from the error catalogue. A
relay neither live nor retained SHALL answer `relay.not_found`. The dead letters' error texts
SHALL be given only to a caller also allowed `dlq.list` in the namespace, and the input log and
dead letters SHALL be read only when the relay runs on the controller that answers.

#### Scenario: A healthy relay

- **WHEN** a running relay with a connected runner, no dead letters and healthy plugins is
  diagnosed
- **THEN** the verdict is `ok` and the only finding is `diagnose.healthy` naming its runner

#### Scenario: No runner for the factory

- **WHEN** the only runner announcing factory `orders` is gone and `orders-eu` is diagnosed
  after the convergence loop judged it
- **THEN** the verdict is `problem` with `diagnose.no_runner`, whose hint says to start a runner
  that defines `orders`

#### Scenario: Dead letters are summarised

- **WHEN** a caller allowed `dlq.list` diagnoses a relay with three dead letters, all with the
  error `call.timeout`
- **THEN** a `diagnose.dead_letters` warning carries `count: 3`, `errors: ["call.timeout"]` and
  the newest one's age, and its hint names `flz dlq list`

#### Scenario: A viewer sees the count only

- **WHEN** a `viewer` diagnoses a relay with dead letters
- **THEN** the `diagnose.dead_letters` warning carries their count and no error texts, and no
  `:authorization_denied` record is written for `dlq.list`, which the viewer never asked for

#### Scenario: An unhealthy plugin

- **WHEN** a Kafka consumer bound to the relay cannot reach its broker
- **THEN** a `diagnose.plugin_unhealthy` problem names the plugin, its provider and the reason

#### Scenario: A disabled plugin is a note

- **WHEN** a relay bound to a plugin its operator disabled, and otherwise healthy, is diagnosed
- **THEN** the verdict is `ok`, with `diagnose.healthy` and a `diagnose.plugin_disabled` note
  saying the plugin is disabled and its guaranteed outputs are held

#### Scenario: Asleep is not a problem

- **WHEN** a session that sleeps is diagnosed
- **THEN** the verdict is `ok` with a `diagnose.asleep` note saying the next message wakes it

#### Scenario: Unknown relay

- **WHEN** an id that never ran is diagnosed
- **THEN** the call answers `relay.not_found`

### Requirement: Every verdict reason and state SHALL map to one finding

Each convergence verdict state and reason SHALL map to exactly one `diagnose.*` finding code, and
the suite SHALL fail when a state or reason has none. Every `diagnose.*` code SHALL be in the
error catalogue with a message, and every hint that names a `flz` command SHALL name a verb
the `flz` command table has.

#### Scenario: A new reason without a finding fails the suite

- **WHEN** a verdict reason is added to the judge's relay rules and no finding maps it
- **THEN** the mapping test fails naming the reason

### Requirement: Waiting and stuck inputs SHALL be told apart

When the relay's input log holds inputs above its floor, diagnose SHALL give a
`diagnose.inputs_waiting` note with their count, and SHALL give a `diagnose.inputs_stuck` warning
instead when the watermark has not moved under the same condition the input log's sweep uses to
emit its frozen-watermark event.

#### Scenario: A stuck relay

- **WHEN** a relay's input log holds waiting inputs, its watermark stands where the last sweep
  left it and the log is past the size threshold
- **THEN** diagnose gives `diagnose.inputs_stuck` with the waiting count and the log's size

### Requirement: `flz relay diagnose` SHALL render the findings

`flz relay diagnose <id>` SHALL print one block per finding (level, code, message, then its
hint indented), or one line for `ok`, and with `--json` the answer as data. It SHALL exit 0 when
the diagnosis succeeded, whatever its verdict, and by the error's class when it did not.

#### Scenario: Problem output

- **WHEN** `flz relay diagnose orders-eu` is run while no runner announces `orders`
- **THEN** it prints a `PROBLEM diagnose.no_runner` block with its hint and exits 0

### Requirement: Assistants SHALL reach diagnose as a tool and be told to use it

The operation SHALL be served as the MCP tool `relay_diagnose` on `flz mcp` and `/mcp` and as
`POST /v1/call/Relay.diagnose`, and the knowledge rules SHALL tell an assistant to call it
first when a relay misbehaves.

#### Scenario: An assistant diagnoses

- **WHEN** an MCP client with an `ops` token lists tools
- **THEN** `relay_diagnose` is listed as read-only with the required arguments `namespace` and
  `relay_id`, and calling it returns the findings as structured content

