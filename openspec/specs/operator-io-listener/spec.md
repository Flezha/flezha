# operator-io-listener Specification

## Purpose

The operator-io-listener capability defines what `flz operator listen` and
`flz operator send --await` print, how long they run, what ends them, what they
leave behind on the controller, and how a partition group is listened to. It is the
check a reader deploying a runner can make with nothing but a controller and that
runner: no plugin infrastructure, no local mode.
## Requirements
### Requirement: `flz operator listen` SHALL print every frame with the metadata it carries

`flz operator listen <operator_id>` SHALL attach a transient forwarding plugin to
the named operator's output chain and print each forwarded frame as it arrives, and
with `--input` it SHALL attach to the input chain instead. In text mode each frame
SHALL be one line: the channel name, a tab, the payload bytes, and, when the frame
carries `flz.cell`, `flz.seq` or `flz.key`, a further tab and those three as
`cell=… seq=… key=…`; a frame with no channel name SHALL print `-` in its place. In
JSON mode (`--json` or `--output json`) each frame SHALL be one JSON object on its own
line carrying `channel`, `payload` when the bytes are valid UTF-8 or `payload_base64`
otherwise, and `metadata` with the frame's whole metadata map. No metadata key SHALL be
dropped in either mode.

An output listener SHALL also print the operator's parts and its dead-letter notices, so
that each can be told from an output. In text mode a part's line SHALL be laid out as an
output's, with a third field that begins `part=<flz.part> attempt=<flz.part_attempt>`
followed by the `cell=` and `key=` stamps it carries; a notice's line SHALL be the channel,
a tab, the error's message, a tab, and `error=<code> entry=<dlq entry>`. In JSON mode every
line SHALL carry `kind`, one of `output`, `input`, `part` and `dead_lettered`; a
`dead_lettered` line SHALL carry `error`, the error model's JSON object, in place of the
payload. An input listener (`--input`) SHALL print neither parts nor notices.

#### Scenario: A named sink is readable

- **WHEN** a listener is attached to an operator whose tasklet emits on a sink
  named `echo`, and the operator is sent `hello`
- **THEN** the listener SHALL print a line whose first field is `echo` and whose second
  is `hello`

#### Scenario: A keyed sink's stamps are printed

- **WHEN** a listener is attached to an operator whose keyed sink emits `k1=v1` for
  input `k1:v1`
- **THEN** the printed line SHALL carry the channel, the payload and a third field
  naming the frame's `flz.cell`, `flz.seq` and `flz.key` values

#### Scenario: JSON mode carries the map

- **WHEN** the same frame is listened to with `--json`
- **THEN** the line SHALL decode as an object whose `channel` and `payload` match the
  frame and whose `metadata` holds every key the frame carried

#### Scenario: Input mode prints what the operator ingests

- **WHEN** a listener is attached with `--input` and the operator is sent `again` on
  no named channel
- **THEN** the listener SHALL print a line whose first field is `-` and whose second is
  `again`, before the operator's own output could have been produced

#### Scenario: A part is told from an output

- **WHEN** a listener is attached to an operator whose tasklet sends the part `hel` and
  then emits `hello` on its sink `answer`
- **THEN** the listener SHALL print `answer`, `hel` and a third field beginning
  `part=0 attempt=1`, and then `answer` and `hello` with no `part=` field

#### Scenario: JSON names the kind

- **WHEN** the same run is listened to with `--json`
- **THEN** the first line SHALL carry `"kind": "part"` and the second `"kind": "output"`

#### Scenario: A dead-lettered input is printed as a notice

- **WHEN** an operator a listener is attached to dead-letters an input on channel `in`
- **THEN** the listener SHALL print a line whose first field is `in` and whose third field
  is `error=input.dead_lettered entry=<the entry>`, and in JSON a line with
  `"kind": "dead_lettered"` and an `error` object carrying that code

### Requirement: A listener SHALL be bounded, and SHALL leave nothing behind

A listener SHALL run for `--duration-ms` milliseconds, sharing the option, its default
window and its refusal of a non-numeric value with `flz observe tail`, and SHALL
return exit 0 when the window closes. The plugin it attached SHALL be disabled when
the window closes, when the calling `flz` process or its node goes away, and when the
loop's own process exits, on every member instance a partition group fanned it out to.
Ctrl-C on `flz operator listen` SHALL end the verb: the node a `flz` verb starts
SHALL terminate on SIGINT rather than answer it with the Erlang break menu.

A listener SHALL also end, with exit 0, when the operator it listens to ends. It SHALL end
with the error `listener.overrun` when its backlog overflows (see "A listener's backlog
SHALL be bounded") and with `listener.detached` when its plugin is disabled by anyone but
itself, exiting with the class's code. However it ends, the disable SHALL be addressed to
the operator (or every member of the group) by namespace and id, and SHALL NOT depend on
which controller node recorded the enable: a listener whose operator has moved to another
controller, or whose group gained members by a rescale, SHALL leave no plugin row on any
member, and the heartbeat's own disable SHALL be addressed the same way.

#### Scenario: The window closes

- **WHEN** `flz operator listen op --duration-ms 500` runs against a quiet operator
- **THEN** it SHALL exit 0 after about half a second, and `flz operator info` SHALL
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

- **WHEN** SIGINT reaches a running `flz operator listen` started through the
  release's `flzvm`
- **THEN** the verb SHALL exit without printing the `BREAK:` menu, and the controller
  SHALL disable its plugin

#### Scenario: An undeclared option is refused

- **WHEN** `flz operator listen op --duration 5` is run
- **THEN** the verb SHALL refuse the unknown spelling rather than run without it

#### Scenario: A listener on a moved operator leaves nothing behind

- **GIVEN** a listener attached through controller node A to an operator that A hosted
- **WHEN** the operator migrates to node B and the listener's window then closes
- **THEN** the operator's declared plugin set and B's extension table SHALL hold no
  listener plugin, and the convergence loop SHALL NOT start one again

#### Scenario: A killed listener on a moved operator is cleaned by its heartbeat

- **GIVEN** the same move
- **WHEN** the listening process is killed instead
- **THEN** within the heartbeat interval the worker on B SHALL disable the plugin by the
  operator's namespace and id, and no row SHALL remain on any node

#### Scenario: A listener ends with its operator

- **WHEN** a `once` bounded operator a listener is attached to completes
- **THEN** `flz operator listen` SHALL exit 0 after printing the operator's outputs,
  before its window closes

### Requirement: A listener on a partition group SHALL attach to every member

`flz operator listen <group_id>` against a partition group SHALL attach one plugin
instance per member, each carrying the member's `#pK` suffix, and SHALL print the
frames of every member on the one stream, each with its own metadata.

#### Scenario: Two members, one stream

- **WHEN** a listener is attached to a two-member group and two inputs with keys owned
  by different members are sent, one to each member
- **THEN** both frames SHALL be printed by the one listener, and `flz operator info`
  SHALL show the plugin on both members while it runs

### Requirement: `flz operator send --await` SHALL attach the listener before it sends

`flz operator send <operator_id> --text <payload> --await <ms>` SHALL attach the
output listener to the operator, then deliver the payload, print the delivery
verdict, and print every output frame that arrives within `<ms>` milliseconds in the
text format `flz operator listen` uses. The listener SHALL be
attached before the payload is delivered. Without `--await` the verb SHALL behave as it
does today and SHALL carry no `await_ms` in its request.

#### Scenario: One command checks a deployment

- **WHEN** `flz operator send my_echo --text hello --await 2000` runs against a
  running echo operator
- **THEN** the output SHALL carry the delivery verdict followed by a frame line whose
  channel is the echo sink's and whose payload is `hello`, and the verb SHALL exit 0

#### Scenario: Attaching after the send misses the frame

- **WHEN** the same steps are run with the listener attached after the delivery
  answers
- **THEN** no frame SHALL be printed within the window, which is why the order is
  pinned

#### Scenario: Nothing came back

- **WHEN** `--await 500` is given and the operator emits nothing for the input
- **THEN** the verb SHALL print the delivery verdict, print no frame, and exit 0 after
  the window

### Requirement: The listener SHALL be documented where a reader deploying a runner looks

The `flz operator` help screen SHALL list `listen` and show one example of it and one
of `send --await`; `flz operator listen --help` SHALL be its own screen, pinned by a
golden capture, printing exactly the options its row declares. The runner deployment
guide's "Start it, check it" SHALL teach `flz operator start`, `flz operator send
--await` and `flz operator listen` as the check that needs no plugin infrastructure.

#### Scenario: The screen and the row agree

- **WHEN** the CLI's help-screen tests run
- **THEN** `operator-listen` SHALL have a golden capture, and every option the screen
  prints SHALL be one the `listen` row declares

#### Scenario: The guide's check runs as written

- **WHEN** the commands the guide's "Check it" section prints are run in order against
  a controller and one runner
- **THEN** each SHALL exit 0 and the frame the guide says appears SHALL appear

### Requirement: One listener function SHALL serve the CLI and every plugin

`Flezha.Service.Listener.listen(namespace, operator_id, opts, handler)` SHALL be the one
listener in the controller. It SHALL attach the listener plugin to the operator, or to
every member when `operator_id` names a partition group, run in the caller's own process,
hand each event to `handler`, and detach on every way out, including a raise in `handler`.
Its events SHALL be `{:output, payload, metadata}`, `{:part, payload, metadata}`,
`{:dead_lettered, error, metadata}` with `error` a `%Flezha.Error{}`, and, for
`direction: :input`, `{:input, payload, metadata}`. `handler` SHALL answer `:continue` or
`:done`. Its options SHALL be `direction` (`:output` by default, or `:input`), `window_ms`
(a positive integer, or `:infinity` by default), `before` (a function run once after the
attach and before the first event, which `send --await` uses to deliver its input) and
`watch` (a process whose exit ends the listener, which the CLI sets to its caller's group
leader). It SHALL answer `{:ok, :window_closed | :done | :caller_down | :operator_ended}`,
or `{:error, %Flezha.Error{}}` for a refused attach (the enable's own code, such as
`operator.not_found`), for `listener.overrun` and for `listener.detached`.

The function SHALL NOT authorize: `flz operator listen` and `flz operator send --await`
authorize through their verb rows before they call it, and a plugin that calls it is
trusted code whose own logic decides who may read. The CLI SHALL keep only the rendering of
events and outcomes, and SHALL hold no attach, detach, heartbeat or receive loop of its own.

#### Scenario: A plugin listens from its own process

- **WHEN** a test process standing in for a plugin's request process calls `listen/4` on
  an operator by namespace and id with `window_ms: :infinity` and a handler that collects
  events, and the operator is sent two inputs
- **THEN** the handler SHALL receive both outputs as `{:output, _, _}` events in the caller
  process, and after the handler answers `:done` the operator SHALL carry no listener plugin

#### Scenario: A handler that raises detaches

- **WHEN** the handler raises on its first event
- **THEN** the raise SHALL propagate to the caller and the operator SHALL carry no listener
  plugin

#### Scenario: The CLI and a plugin share the path

- **WHEN** the source of `lib/flezha/cli/` is searched for a plugin enable, a plugin
  disable or a heartbeat reply
- **THEN** none SHALL be found outside the call to `Flezha.Service.Listener.listen/4`

### Requirement: A listener SHALL survive its operator sleeping, waking, rebinding and moving

A listener SHALL keep receiving its operator's frames, of either kind of operator, across:
the operator sleeping when idle and waking on input or its timer; the operator's runner
exiting and the operator being rebound to the same or another runner; the operator's
Overwatch process crashing and being restarted from the declared set; the operator moving
to another controller node by a handover or a hash-ring rebalance; and a partition group
gaining members by a rescale, whose new members SHALL be listened to as well. It SHALL
survive the controller node that hosted the operator dying, while its own node lives, once
the operator is restarted elsewhere. An operator produces no frame while no runner holds
it, so nothing is missed in the gap; a turn that an interruption re-runs SHALL show its
parts again from `0`.

A listener attached to an instance found asleep at a controller boot SHALL NOT keep that
instance's declared plugins down when it wakes (see `operator-extensions`, "A bind SHALL
bring up every declared plugin its table does not hold").

#### Scenario: A listener across a sleep and a wake

- **GIVEN** a listener on an unbounded operator that sleeps when idle, and one on a
  `per_input` bounded session
- **WHEN** each falls asleep, and is then woken by a new input
- **THEN** each listener SHALL receive the output the woken operator produces, without
  having been re-attached

#### Scenario: A listener across a runner's death

- **WHEN** the runner holding a listened-to operator is killed and the operator is rebound
  to another runner
- **THEN** the listener SHALL receive the outputs of the inputs sent after the rebind

#### Scenario: A listener across an Overwatch restart

- **WHEN** the listened-to operator's Overwatch process is killed and restarted
- **THEN** the listener SHALL receive the outputs of the inputs sent after the restart

#### Scenario: A listener across a migration

- **GIVEN** a listener whose process runs on controller node A, on an operator hosted by
  node B
- **WHEN** the operator migrates to node C
- **THEN** the listener SHALL receive the outputs of the inputs sent after the move

#### Scenario: A listener across a rescale

- **WHEN** a two-member group a listener is attached to is rescaled to three members
- **THEN** the listener SHALL receive the frames of all three members, and when it ends
  all three SHALL carry no listener plugin

#### Scenario: A wake after a boot brings up the declared plugins

- **GIVEN** an operator with a file sink plugin that was asleep when the controller
  restarted, and a listener attached to it before it wakes
- **WHEN** an input wakes it
- **THEN** the file sink SHALL receive the operator's output, as the listener does

### Requirement: A listener's backlog SHALL be bounded

A listener SHALL hold the frames its handler has not yet taken in a relay process on the
listener's own node, which also answers the plugin's heartbeat, so a slow handler neither
grows the operator's memory nor stops the heartbeat. The relay SHALL hold at most
`listener_pending_bytes` of payload (4 MiB by default, `FLEZHA_LISTENER_PENDING_BYTES`)
and `listener_pending_frames` frames (4 096 by default, `FLEZHA_LISTENER_PENDING_FRAMES`).
When a new frame would pass either bound, the relay SHALL drop held parts, oldest first,
until it fits; a new part that still does not fit SHALL be dropped; an output or a notice
that still does not fit SHALL end the listener with the error `listener.overrun`, of class
`unavailable`, naming the operator and both bounds. An output SHALL never be dropped while
the listener goes on. The Overwatch SHALL send a part to the relay without suspending on a
busy distribution link, dropping the part instead, and SHALL send an output as it does
today.

#### Scenario: A slow reader loses parts, not outputs

- **GIVEN** a listener whose handler sleeps 200 ms per event, with
  `listener_pending_frames` set to 8
- **WHEN** a tasklet sends 100 parts and then emits one output
- **THEN** the handler SHALL receive fewer than 100 parts, in increasing `flz.part` order,
  and then the output, and the listener SHALL go on

#### Scenario: A reader that falls behind on outputs is ended

- **GIVEN** the same handler and bound
- **WHEN** the operator emits 100 outputs
- **THEN** the listener SHALL end with `listener.overrun`, detach, and leave no plugin on
  the operator, and the relay SHALL at no point have held more than 8 frames

### Requirement: A listener SHALL see an input its operator dead-lettered

The controller SHALL hand an operator's listeners a notice through the `on_preview` hook
once an input the operator dead-lettered is durable in the DLQ, for either kind: a
retry-exhausted or non-retryable input of an unbounded operator, on either durability tier;
an unreadable message of a bounded operator; any input a bounded loop dead-letters. For a
failed turn a durable `per_input` `keep_progress` loop parks through its iteration commit
(`keep-a-session-after-a-failed-turn`), the release point SHALL be that turn's durable
`dead_lettered` record, not the DLQ append, because a turn whose record was never written
runs again and may succeed.
The notice SHALL carry the error `input.dead_lettered`, of class `unavailable`, whose
details name the operator, the channel, the DLQ entry, the tasklet, the attempts and the
author's error text, and whose hint is `flz dlq show <entry>`, and SHALL carry the metadata
`channel` and, when known, the input's `flz.offset`. The notice SHALL be as durable as a
part is (not at all) and SHALL be kept like an output by a listener's backlog. A payload
parked by a sink or a forward, which is an output that could not be delivered, SHALL NOT
raise a notice.

#### Scenario: A poison input of a stream is announced

- **GIVEN** an unbounded operator whose tasklet fails every attempt on the input `boom`
- **WHEN** a listener is attached and `boom` is sent
- **THEN** once the entry is parked the listener SHALL receive a `{:dead_lettered, error, _}`
  event with code `input.dead_lettered` naming that entry

#### Scenario: A session's failed turn is announced once its record is durable

- **GIVEN** a durable `per_input` `keep_progress` session whose turn fails on `boom`, as
  `keep-a-session-after-a-failed-turn` defines, and a listener attached to it
- **WHEN** `boom` is sent
- **THEN** the listener SHALL receive the `input.dead_lettered` notice only after the turn's
  `dead_lettered` record is durable

#### Scenario: A sink's park raises no notice

- **WHEN** an output of a listened-to operator is parked by its sink plugin
- **THEN** the listener SHALL receive the output and no notice

### Requirement: A listener SHALL hand its handler an idle event when asked

`Flezha.Service.Listener.listen/4` SHALL accept, beside the options `direction`,
`window_ms`, `before` and `watch`, the option `idle_ms`, a positive integer or `:infinity`,
`:infinity` by default. When it is an integer and no event has been handed to
the handler for `idle_ms` milliseconds, `listen/4` SHALL call the handler with `:idle`, in
the caller's own process, and SHALL honour its answer as it honours an event's: `:continue`
goes on and starts a new idle interval, `:done` ends the listener with `{:ok, :done}` and
detaches it. An idle call SHALL NOT drop, reorder or delay an event the relay holds, and
SHALL NOT change the relay's bounds. With `idle_ms` at `:infinity` the handler SHALL never
be called with `:idle`, so `flz operator listen` and `flz operator send --await` behave as
before. This SHALL hold for a listener on an operator of either kind.

#### Scenario: A quiet listener is told it is idle

- **WHEN** a test process calls `listen/4` with `idle_ms: 200` on an operator that produces nothing for one second, with a handler that counts `:idle` calls and answers `:continue`
- **THEN** the handler SHALL be called with `:idle` about five times, and the operator SHALL still carry the listener plugin

#### Scenario: An idle answer of done detaches

- **WHEN** the handler answers `:done` to its first `:idle` call
- **THEN** `listen/4` SHALL return `{:ok, :done}` and the operator SHALL carry no listener plugin

#### Scenario: Events reset the idle interval

- **GIVEN** `idle_ms: 500` on a bounded `per_input` session
- **WHEN** the session's turn sends a part every 100 ms for two seconds
- **THEN** the handler SHALL receive every part the relay kept, in order, and SHALL NOT be called with `:idle` during those two seconds

#### Scenario: The CLI is unchanged

- **WHEN** `flz operator listen op --duration-ms 1000` runs against a quiet operator
- **THEN** it SHALL print nothing and exit 0, as before this option existed

