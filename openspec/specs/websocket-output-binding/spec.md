# websocket-output-binding Specification

## Purpose
TBD - created by archiving change bind-websocket-both-ways. Update Purpose after archive.
## Requirements
### Requirement: A websocket plugin SHALL send relay outputs on the connection it holds

`Plugin.Provider.Websocket` SHALL implement `on_input/2` and `on_output/2` and SHALL
send the dispatched payload upstream as a frame on the socket it already holds, without
opening a second connection. A plugin with a hook in neither direction SHALL behave
exactly as it does today and SHALL start no queue, no timer and no additional process.

The provider SHALL accept `sink_frame_type` (`"text"` or `"binary"`, default `"text"`),
an optional `sink_template` whose `{{payload}}` placeholder is replaced by the dispatched
payload and which SHALL be refused at enable time without that placeholder, and an optional
`channel_out` restricting the plugin to outputs whose metadata names that channel, with the
same meaning the Kafka producer's own restriction has. With no `sink_template` the payload
SHALL be sent verbatim as the frame body.

Because the websocket provider also feeds the relay, `channel` on it names both
directions by the rule in `relay-extensions`: the outputs it sends and the entry channel
its inbound frames are injected on. A plugin whose two names differ SHALL write `channel_out`
for the restriction and `channel_in` for the entry channel, and `channel_out` SHALL be
accepted as the explicit spelling of the restriction in every case.

Outbound frames SHALL be handed to the connection only while it is established. The
provider SHALL NOT use the connection library's public send function, whose disconnected
path discards a frame without an error.

#### Scenario: An output is sent on the established socket
- **WHEN** a relay with a websocket plugin hooked on `on_output` produces an output
- **THEN** the payload SHALL be sent as one frame of the configured type on the connection the plugin already holds
- **AND** no second connection SHALL be opened

#### Scenario: Both directions share one connection
- **WHEN** a plugin has an `on_output` hook and the upstream sends an inbound frame
- **THEN** the inbound frame SHALL be injected into the relay as it is for a receive-only plugin
- **AND** both directions SHALL use the same socket

#### Scenario: A channel-restricted output plugin ignores other channels
- **GIVEN** a plugin configured with `channel: "prices"`
- **WHEN** an output whose metadata names a different channel is dispatched
- **THEN** no frame SHALL be sent, and the output SHALL pass through the chain unchanged

#### Scenario: The explicit spelling restricts the same way
- **GIVEN** a plugin configured with `channel_out: "prices"`
- **WHEN** an output whose metadata names a different channel is dispatched
- **THEN** no frame SHALL be sent, and the output SHALL pass through the chain unchanged

#### Scenario: One `channel` names both directions
- **GIVEN** a plugin with an `on_output` hook and configured with `channel: "prices"`, on a relay declaring an entry channel `prices`
- **WHEN** an inbound frame arrives and an output naming `alerts` is dispatched
- **THEN** the frame SHALL be injected on `prices`, and no frame SHALL be sent for the `alerts` output

#### Scenario: A plugin with no hook costs nothing
- **WHEN** a plugin is enabled with no `on_input` or `on_output` hook
- **THEN** no outbound queue SHALL be allocated and no outbound timer SHALL be armed

### Requirement: An outbound frame SHALL be queued while the socket is down, never dropped

The provider SHALL hold outbound frames in a bounded queue whenever the connection is not
established, and SHALL flush that queue on connect. No outbound frame SHALL be discarded
without either being sent, being parked in the dead-letter store, or refusing the caller.

The queue SHALL be bounded by `sink_queue_max` (default `10000`). When the queue is at its
bound the enqueueing caller SHALL be parked until the queue drains, applying back-pressure
rather than dropping, which is the policy the other bounded output plugins share. A frame whose
retry budget is permanently exhausted SHALL be parked through the dead-letter store,
attributed to the output's own cell and channel, and SHALL NOT be silently lost.

On connect, subscription frames SHALL be sent before queued outbound frames, so a server
that requires a subscription handshake receives it first.

#### Scenario: Frames sent while disconnected are held and flushed
- **GIVEN** a plugin whose socket has dropped and is reconnecting
- **WHEN** outputs are dispatched to it
- **THEN** each frame SHALL be held in the queue rather than handed to the connection
- **AND** on reconnect they SHALL be sent in dispatch order

#### Scenario: Subscription frames precede queued output on connect
- **GIVEN** a plugin with a subscription source and queued outbound frames
- **WHEN** the socket connects
- **THEN** the subscription frames SHALL be sent before any queued outbound frame

#### Scenario: A full queue parks the caller
- **WHEN** the outbound queue reaches `sink_queue_max`
- **THEN** the enqueueing caller SHALL be parked until the queue drains below the bound
- **AND** no frame SHALL be discarded to make room

#### Scenario: An exhausted frame is parked, not dropped
- **WHEN** a frame's retry budget is permanently exhausted
- **THEN** it SHALL be written to the dead-letter store carrying its cell and channel
- **AND** the outbox hold SHALL be answered so the cursor advances past exactly the parked range

### Requirement: Delivery SHALL be confirmed by a declared acknowledgement mode

The provider SHALL accept `sink_ack_mode` with the values `"none"` (default) and
`"required"`, and SHALL answer the outbox hold for an output according to it. The two modes
SHALL carry different, separately stated guarantees, and neither SHALL be described as
exactly-once.

Under `"none"` the hold SHALL be answered when the frame is handed to the established
connection. The guarantee SHALL be stated as at-most-once beyond that point: a frame
accepted by the local socket and lost to a reconnect is not recoverable and is not
retried.

Under `"required"` the hold SHALL be answered only when an inbound frame carries the
correlation id the outbound frame was sent with, and an unanswered frame SHALL be re-sent
on the next cycle rather than assumed delivered. The guarantee SHALL be stated as
at-least-once. The correlation rule SHALL be the one the subscription delta commit already
uses, and SHALL NOT be a second mechanism.

`"required"` SHALL be refused at enable time without a `sink_template` carrying the
correlation placeholder. A websocket frame has no header, so a verbatim payload has nowhere
to put an id the upstream can echo, and an acknowledgement a frame cannot ask for is not a
guarantee.

A frame SHALL be able to carry the output's durable identity stamps, through the same
template: `{{cell}}` and `{{seq}}` SHALL be substituted with the output's `flz.cell` and
`flz.seq`. Because a repeat is a normal event under `"required"` and impossible under
`"none"`, the mode that can repeat is exactly the mode a template is mandatory for, so a
repeated frame always carries the identity a consumer needs. The specification SHALL state
that deduplication is the consumer's, because a websocket exposes no transaction to enlist
and no key to overwrite.

#### Scenario: Confirm on handoff under the default mode
- **GIVEN** a plugin with `sink_ack_mode: "none"`
- **WHEN** an output with a delivery-guaranteed hold is sent on an established socket
- **THEN** the hold SHALL be answered at handoff
- **AND** no acknowledgement SHALL be awaited

#### Scenario: Confirm on acknowledgement under the required mode
- **GIVEN** a plugin with `sink_ack_mode: "required"`
- **WHEN** an output is sent and the upstream echoes its correlation id
- **THEN** the hold SHALL be answered on that inbound frame and not before

#### Scenario: An unacknowledged frame is re-sent, not assumed
- **GIVEN** a plugin with `sink_ack_mode: "required"`
- **WHEN** a sent frame receives no acknowledgement within its window
- **THEN** the frame SHALL be re-sent
- **AND** the hold SHALL remain unanswered until an acknowledgement arrives or the retry budget is exhausted

#### Scenario: A mode that can repeat carries the identity a consumer dedups on
- **GIVEN** a plugin with `sink_ack_mode: "required"`
- **WHEN** the enable supplies no `sink_template`, or one without the correlation placeholder
- **THEN** the enable SHALL be refused, naming the placeholder

#### Scenario: Outputs carry their identity stamps
- **WHEN** an output is sent through a template carrying the cell and sequence placeholders
- **THEN** the frame SHALL carry the output's `flz.cell` and `flz.seq` stamps
- **AND** the published documentation SHALL state that the consumer deduplicates on them

### Requirement: A websocket output plugin on an effectively-once relay SHALL warn at enable time

A websocket output binding enabled on an effectively-once relay SHALL warn and SHALL
NOT be refused. The warning SHALL name the plugin and the acknowledgement mode, and SHALL
state that the relay's internal guarantee is unchanged while its websocket output hop
is not effectively-once.

The provider SHALL read the tier from the host-injected `:effectively_once` opt rather than
from any state of its own.

#### Scenario: Enabling on a tier relay warns and proceeds
- **GIVEN** a relay running with effectively-once delivery
- **WHEN** a websocket plugin with an `on_output` hook is enabled on it
- **THEN** a warning naming the plugin and the acknowledgement mode SHALL be emitted
- **AND** the plugin SHALL be enabled

#### Scenario: Enabling on a plain relay is silent
- **GIVEN** a relay not running with effectively-once delivery
- **WHEN** the same plugin is enabled
- **THEN** no such warning SHALL be emitted

### Requirement: The outbound handoff SHALL NOT block the socket process on the relay

The handoff from the output dispatch chain to the socket process SHALL be a correlated
asynchronous exchange, and SHALL NOT be a synchronous call in either direction while the
opposite direction can also block. The socket process SHALL NOT wait on the relay's
Overwatch while the Overwatch is waiting on it.

This is a deadlock refusal, not a performance preference: on the effectively-once tier the
socket process already calls the Overwatch without a deadline to confirm an injected input,
and the Overwatch is the process that runs the output chain. A synchronous outbound send
closes that cycle and hangs the relay permanently.

#### Scenario: Simultaneous ingest and output do not deadlock
- **GIVEN** a relay running with effectively-once delivery and a websocket plugin bound in both directions
- **WHEN** an inbound frame is being confirmed while an output is dispatched to the same plugin
- **THEN** neither process SHALL block on the other
- **AND** both the injection and the outbound frame SHALL complete

#### Scenario: The caller learns the outcome without holding the socket
- **WHEN** an output is handed to the socket process
- **THEN** the caller SHALL receive a correlated result within a bounded budget
- **AND** the socket process SHALL remain able to read inbound frames throughout

