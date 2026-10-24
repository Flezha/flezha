# cli-controller-socket Specification

## Purpose
How `flz` reaches a running controller: each verb is one request over the controller's UDS
control socket, served by the code every verb runs through, its output and exit code sent back
as frames. The capability owns the request and answer frames, the protocol number, the rule
that a CLI request is accepted only over UDS, the listener every controller opens, ending a
call when its caller goes away, and how an unreachable controller is reported.
## Requirements
### Requirement: A controller verb SHALL be one request over the UDS control socket

Every `flz` verb that reaches a running controller SHALL connect to the controller's UDS control
socket, at the path `socket_dir::control_socket_path()` resolves, and SHALL write one frame of
payload id `cli_request` in the runner transport's framing. Its body SHALL be one JSON object:
`protocol` (the number 1), `entry` (`cli` or `mcp`), for `cli` the operation's door `name` and
its `args` array, for `mcp` the protocol `line`, and `opts`, an object of the options the verb
sends, its output format and the caller's token when it has one. `flz` SHALL start no `flzvm`
for such a verb, and the token SHALL appear nowhere but in that frame.

#### Scenario: No VM is started for a controller verb

- **WHEN** `flz dlq list --token <t>` runs against a stand-in socket server, with a `flzvm` beside
  `flz` that records every start
- **THEN** the server SHALL receive one `cli_request` whose body names `Dlq.list` and carries the
  token in `opts`, and the recorder SHALL show no start

#### Scenario: Every verb sends its door name and arguments

- **WHEN** each verb the catalogue test enumerates runs against the stand-in server
- **THEN** each SHALL send the operation its verb row names, with no more arguments than the
  operation declares and no fewer than it requires

### Requirement: The controller SHALL serve a CLI request with the code that serves it today

On a connection whose first frame is `cli_request`, the controller SHALL answer `cli_accepted`
with its protocol number, then run the request with `Flezha.Service.Cli.call/3` for entry `cli`
or `Flezha.Mcp.Stdio.answer/2` for entry `mcp`. Everything the call writes to its group leader
SHALL be sent as `cli_stdout` frames and everything it writes to the caller's stderr as
`cli_stderr` frames, in order, a write larger than the frame limit split across frames. It
SHALL then send `cli_exit` carrying the exit code: 0 when the call returns, `n` when it exits
`{:shutdown, n}`, and the internal class's code after an internal error rendered on stderr for
any other exit, and SHALL close the connection. `flz` SHALL write the stdout frames to its
stdout and the stderr frames to its stderr as they arrive, and SHALL exit with the code
`cli_exit` carries. A CLI request SHALL NOT start a runner session: it SHALL NOT be admitted,
appear in the fleet, or be announced.

#### Scenario: Text and JSON answers are unchanged

- **WHEN** `flz namespace list` and `flz namespace list --json` run against a live controller
- **THEN** stdout SHALL equal what the same verbs printed before this change, and both SHALL
  exit 0

#### Scenario: A refusal keeps its stream and its code

- **WHEN** `flz namespace delete ghost` runs and `ghost` does not exist
- **THEN** stderr SHALL carry the rendering ending `(namespace.not_found)` and its hint, stdout
  SHALL be empty, and `flz` SHALL exit 5

#### Scenario: A streaming verb prints as it goes

- **WHEN** `flz observe tail --duration-ms 1000` runs while events are emitted
- **THEN** each event SHALL reach stdout before the window closes, and `flz` SHALL exit 0 when it
  does

#### Scenario: A CLI request is not a runner

- **WHEN** a CLI request is served
- **THEN** `flz runner list` SHALL not list it, and no runner connect SHALL be counted for it

### Requirement: A CLI request SHALL be accepted only over UDS

A `cli_request` arriving on a QUIC connection SHALL run nothing. The controller SHALL answer it
with `cli_stderr` carrying a `denied` error, `cli_exit` with that class's code, and close the
connection.

#### Scenario: A CLI request over QUIC is refused

- **WHEN** a `cli_request` naming `Namespace.list` reaches the controller's QUIC server as a
  connection's first frame
- **THEN** it SHALL receive a `denied` error and exit code 4, and no operation SHALL run

### Requirement: Every controller SHALL listen on its UDS control socket

A controller SHALL start its UDS listener whatever `FLEZHA_TRANSPORT` names, in addition to any
other transport it is configured for, so `flz` reaches a controller whose runners use QUIC.

#### Scenario: A QUIC controller answers flz

- **WHEN** a controller boots with `FLEZHA_TRANSPORT=quic`
- **THEN** its control socket SHALL exist, and `flz controller status` SHALL answer from it

### Requirement: A CLI request SHALL carry a protocol number the controller checks

A controller SHALL refuse a `cli_request` whose `protocol` it does not speak with
`cli.protocol_unsupported` (class `invalid`), whose details name the number received and the
number it speaks, and SHALL run nothing. `flz` SHALL wait at most five seconds after
connecting for `cli_accepted`; when none arrives it SHALL end with `cli.protocol_unsupported`,
naming the number it sent and `none` as the number spoken.

#### Scenario: An unknown protocol is refused

- **WHEN** a client sends a `cli_request` with `protocol: 2`
- **THEN** the controller SHALL answer `cli.protocol_unsupported` naming 2 and 1, and exit code 2

#### Scenario: A controller that never accepts

- **WHEN** `flz namespace list` runs against a stand-in server that reads the request and
  answers nothing
- **THEN** `flz` SHALL exit 2 with `cli.protocol_unsupported` after about five seconds

### Requirement: Options SHALL become keywords without creating atoms

Each key of a request's `opts` SHALL become a keyword only when it names an existing atom; any
other key SHALL refuse the request with `control.unknown_parameter`, naming the key, before the
call runs.

#### Scenario: An unknown option key is refused

- **WHEN** a client sends a `cli_request` whose `opts` holds the key `zz_not_an_atom_9f2`
- **THEN** the answer SHALL be `control.unknown_parameter` naming it, and the node SHALL hold no
  atom of that name afterwards

### Requirement: A closed connection SHALL end the call

When the connection of a CLI request closes before the call ends, the controller SHALL end
the call, so nothing it was driving keeps running for a caller that is gone.

#### Scenario: Killing flz ends a listener

- **WHEN** the `flz` process of a running `flz relay listen op` is killed
- **THEN** within the listener's heartbeat interval its plugin SHALL be disabled on the
  controller and no process SHALL remain in its loop

#### Scenario: Ctrl-C ends a tail

- **WHEN** SIGINT reaches a running `flz observe tail`
- **THEN** `flz` SHALL exit, and the controller SHALL hold no process serving that request

### Requirement: An unreachable controller SHALL be reported as before

When the control socket is absent, refuses the connection or cannot be opened, `flz` SHALL
end with `controller.unreachable` (exit 3) naming the socket, and `flz boot await` SHALL keep
retrying until the connection opens or its window closes. When the connection closes without
`cli_exit`, the verb SHALL end with `controller.unreachable`, except `flz controller stop`, which
decides by the outcome line it reads.

#### Scenario: No controller runs

- **WHEN** `flz namespace list` runs and no controller has created the socket
- **THEN** `flz` SHALL exit 3 with `controller.unreachable` naming the socket path

#### Scenario: A stop reports its outcome although the node is gone

- **WHEN** `flz controller stop` runs against a live controller
- **THEN** it SHALL exit 0 after the node is gone, as it does today

### Requirement: `flz mcp` SHALL answer over the socket while a controller runs

`flz mcp` SHALL answer each protocol line with a `cli_request` of entry `mcp` while the socket
can be reached, and SHALL fall back to `flzvm eval` for the knowledge tools only when it cannot,
as it does today. A cancelled request SHALL close its connection.

#### Scenario: A tool call with a running controller starts no VM

- **WHEN** `flz mcp` answers a `tools/call` for `relay_list` with a controller running
- **THEN** the answer SHALL come from the controller and no `flzvm` SHALL be started

#### Scenario: Knowledge tools still answer with no controller

- **WHEN** `flz mcp` answers a `tools/call` for `flezha_new_project` with no controller running
- **THEN** the answer SHALL hold the starter's files, as it does today

