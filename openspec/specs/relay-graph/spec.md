# relay-graph Specification

## Purpose
How the controller keeps the graph a factory's runners announce, and how a caller reads one relay's graph.
## Requirements
### Requirement: The controller SHALL keep each factory's announced graph

The controller SHALL keep the graph of each runner's latest announcement of a factory beside that
announcement's API, replaced by every full-set announcement of the runner. When the runners of one
factory announce different graphs, the read SHALL mark the factory as disagreeing and serve the latest
announcement. A factory whose runners all disconnect SHALL lose its graph with the rest of its
announcement. The graph SHALL NOT be part of the factory rows `Relay.api` and `Relay.call_channel`
read.

#### Scenario: A reconnecting runner replaces the graph

- **WHEN** a runner reconnects announcing factory `market` with one more tasklet than before
- **THEN** the graph read for a `market` relay lists the new tasklet

#### Scenario: Two runners disagree

- **WHEN** two connected runners announce factory `market` with different graphs
- **THEN** the graph read for a `market` relay is marked as disagreeing and is the later announcement

#### Scenario: A call does not carry the graph

- **WHEN** a caller calls an entry channel of a `market` relay through `Relay.call_channel`
- **THEN** the factory row the call reads holds no graph

### Requirement: A relay's graph SHALL be readable through the published surface

The controller SHALL expose `Flezha.Service.Relay.graph/3`, taking a namespace, a relay id and a
caller context, bound to the `relay_info` verb. It SHALL answer the relay id, its factory, the
factory's graph and whether the factory's runners disagree, resolving the relay as `Relay.api/3`
does: a partition member (`<group>#p<n>`) and a group id both resolve to the group's factory. A factory
with no announced graph SHALL be answered with no graph and the reason `not_announced`, not an
error. An unknown relay SHALL be refused as `Relay.api/3` refuses it. The CLI SHALL expose the function
as `flz relay graph <relay_id>`, printing each tasklet with the channels it consumes and produces and
each channel's plugin end.

#### Scenario: Reading a member's graph

- **WHEN** a caller holding `relay_info` reads the graph of relay `demo#p0`, a member of group `demo` of factory `market`
- **THEN** the answer is factory `market`'s graph with relay id `demo#p0`

#### Scenario: A factory from an old runner

- **WHEN** the only runner of factory `legacy` announced no graph
- **THEN** the graph read for a `legacy` relay answers no graph with the reason `not_announced`

#### Scenario: A caller without the verb is refused

- **WHEN** a caller whose role does not grant `relay_info` reads a graph
- **THEN** the call is refused by the authorization seam and no graph is returned

#### Scenario: The terminal prints the graph

- **WHEN** an operator runs `flz relay graph demo#p0`
- **THEN** the CLI prints each tasklet with its input and output channels, and marks the channels that end at a plugin output or a call

### Requirement: The reference test app SHALL announce a graph with every shape the graph describes

The Rust reference test app SHALL register a `showcase` factory whose announced graph holds:
- two entry channels whose tasklets merge into one (a fan-in);
- one channel read by four tasklets (a fan-out);
- a straight run of four single-input, single-output tasklets;
- a channel both read by a tasklet and bound to plugins (a hybrid);
- a call end with its answer entry channel;
- tasklets in single, static and dynamic Cell modes;
- five output channels.

It SHALL run on the testnet generator's trade and quote messages without stalling on a malformed one.

#### Scenario: The showcase graph is announced

- **WHEN** the dev runner connects and `Relay.graph/3` is read for a relay bound to `showcase`
- **THEN** the graph holds the entry channels `trades`, `quotes` and the call's answer channel, the call end `quote`, and the output channels `prices`, `volumes`, `alerts`, `orders` and `audit`

#### Scenario: Records reach every output

- **WHEN** a `showcase` relay receives trade and quote messages for a minute
- **THEN** each of its five output channels has emitted at least one frame

