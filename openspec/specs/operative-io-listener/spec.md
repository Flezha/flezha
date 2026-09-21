# operative-io-listener Specification

## Purpose

The operative-io-listener capability defines what `flz operative listen` and
`flz operative send --await` print, how long they run, what ends them, what they
leave behind on the controller, and how a partition group is listened to. It is the
check a reader deploying a runner can make with nothing but a controller and that
runner: no plugin infrastructure, no local mode.

## Requirements
### Requirement: `flz operative listen` SHALL print every frame with the metadata it carries

`flz operative listen <operative_id>` SHALL attach a transient forwarding plugin to
the named operative's output chain and print each forwarded frame as it arrives, and
with `--input` it SHALL attach to the input chain instead. In text mode each frame
SHALL be one line: the channel name, a tab, the payload bytes, and, when the frame
carries `flz.cell`, `flz.seq` or `flz.key`, a further tab and those three as
`cell=… seq=… key=…`; a frame with no channel name SHALL print `-` in its place. In
JSON mode (`--json` or `--output json`) each frame SHALL be one JSON object on its own
line carrying `channel`, `payload` when the bytes are valid UTF-8 or `payload_base64`
otherwise, and `metadata` with the frame's whole metadata map. No metadata key SHALL be
dropped in either mode.

#### Scenario: A named sink is readable

- **WHEN** a listener is attached to an operative whose tasklet emits on a sink
  named `echo`, and the operative is sent `hello`
- **THEN** the listener SHALL print a line whose first field is `echo` and whose second
  is `hello`

#### Scenario: A keyed sink's stamps are printed

- **WHEN** a listener is attached to an operative whose keyed sink emits `k1=v1` for
  input `k1:v1`
- **THEN** the printed line SHALL carry the channel, the payload and a third field
  naming the frame's `flz.cell`, `flz.seq` and `flz.key` values

#### Scenario: JSON mode carries the map

- **WHEN** the same frame is listened to with `--json`
- **THEN** the line SHALL decode as an object whose `channel` and `payload` match the
  frame and whose `metadata` holds every key the frame carried

#### Scenario: Input mode prints what the operative ingests

- **WHEN** a listener is attached with `--input` and the operative is sent `again` on
  no named channel
- **THEN** the listener SHALL print a line whose first field is `-` and whose second is
  `again`, before the operative's own output could have been produced

### Requirement: A listener SHALL be bounded, and SHALL leave nothing behind

A listener SHALL run for `--duration-ms` milliseconds, sharing the option, its default
window and its refusal of a non-numeric value with `flz observe tail`, and SHALL
return exit 0 when the window closes. The plugin it attached SHALL be disabled when
the window closes, when the calling `flz` process or its node goes away, and when the
loop's own process exits, on every member instance a partition group fanned it out to.
Ctrl-C on `flz operative listen` SHALL end the verb: the node a `flz` verb starts
SHALL terminate on SIGINT rather than answer it with the Erlang break menu.

#### Scenario: The window closes

- **WHEN** `flz operative listen op --duration-ms 500` runs against a quiet operative
- **THEN** it SHALL exit 0 after about half a second, and `flz operative info` SHALL
  list no listener plugin on `op`

#### Scenario: The caller dies

- **WHEN** the `flz` process of a running listener is killed
- **THEN** within the provider's heartbeat interval the listener's plugin SHALL be
  disabled on the controller and no process SHALL remain in its loop

#### Scenario: A group listener releases every member

- **WHEN** a listener on a two-member partition group ends by any of the three routes
- **THEN** both member instances of its plugin SHALL be disabled and the group's
  members SHALL report no listener plugin

#### Scenario: Ctrl-C ends the verb

- **WHEN** SIGINT reaches a running `flz operative listen` started through the
  release's `flzvm`
- **THEN** the verb SHALL exit without printing the `BREAK:` menu, and the controller
  SHALL disable its plugin

#### Scenario: An undeclared option is refused

- **WHEN** `flz operative listen op --duration 5` is run
- **THEN** the verb SHALL refuse the unknown spelling rather than run without it

### Requirement: A listener on a partition group SHALL attach to every member

`flz operative listen <group_id>` against a partition group SHALL attach one plugin
instance per member, each carrying the member's `#pK` suffix, and SHALL print the
frames of every member on the one stream, each with its own metadata.

#### Scenario: Two members, one stream

- **WHEN** a listener is attached to a two-member group and two inputs with keys owned
  by different members are sent, one to each member
- **THEN** both frames SHALL be printed by the one listener, and `flz operative info`
  SHALL show the plugin on both members while it runs

### Requirement: `flz operative send --await` SHALL attach the listener before it sends

`flz operative send <operative_id> --text <payload> --await <ms>` SHALL attach the
output listener to the operative, then deliver the payload, print the delivery
verdict, and print every output frame that arrives within `<ms>` milliseconds in the
text format `flz operative listen` uses. The listener SHALL be
attached before the payload is delivered. Without `--await` the verb SHALL behave as it
does today and SHALL carry no `await_ms` in its request.

#### Scenario: One command checks a deployment

- **WHEN** `flz operative send my_echo --text hello --await 2000` runs against a
  running echo operative
- **THEN** the output SHALL carry the delivery verdict followed by a frame line whose
  channel is the echo sink's and whose payload is `hello`, and the verb SHALL exit 0

#### Scenario: Attaching after the send misses the frame

- **WHEN** the same steps are run with the listener attached after the delivery
  answers
- **THEN** no frame SHALL be printed within the window, which is why the order is
  pinned

#### Scenario: Nothing came back

- **WHEN** `--await 500` is given and the operative emits nothing for the input
- **THEN** the verb SHALL print the delivery verdict, print no frame, and exit 0 after
  the window

### Requirement: The listener SHALL be documented where a reader deploying a runner looks

The `flz operative` help screen SHALL list `listen` and show one example of it and one
of `send --await`; `flz operative listen --help` SHALL be its own screen, pinned by a
golden capture, printing exactly the options its row declares. The runner deployment
guide's "Start it, check it" SHALL teach `flz operative start`, `flz operative send
--await` and `flz operative listen` as the check that needs no plugin infrastructure.

#### Scenario: The screen and the row agree

- **WHEN** the CLI's help-screen tests run
- **THEN** `operative-listen` SHALL have a golden capture, and every option the screen
  prints SHALL be one the `listen` row declares

#### Scenario: The guide's check runs as written

- **WHEN** the commands the guide's "Check it" section prints are run in order against
  a controller and one runner
- **THEN** each SHALL exit 0 and the frame the guide says appears SHALL appear

