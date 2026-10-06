# relay-api Specification

## Purpose
Every relay has a public API: its factory's entry channels as its runner announces them, each
with its format, whether it answers, and its description and JSON Schema when declared. The
controller keeps it per namespace and factory, `Relay.api` lists it, `Relay.call_channel` calls a
channel by value, and the CLI, the HTTP route, every SDK's `runner.call` and the MCP relay tools
serve it.
## Requirements
### Requirement: Every entry channel SHALL be part of its relay's API

A relay's API SHALL be its factory's entry channels, each with its name, its format (`json`,
`text`, `bytes`, `msgpack` or `custom`, from the codec the channel declares), whether an
output that answers is reachable from it and that answer's format, an optional description, and, for
a `json` channel, an optional JSON Schema of its input and, where the SDK knows it, of its answer.
No step beyond declaring the channels SHALL be needed for a channel to be in the API.

#### Scenario: A plain relay has an API

- **WHEN** a Java factory `orders` declares `c.input(PlaceOrder.class, "place").into(Place.class).output("placed")`
  and `c.input(String.class, "audit").into(Audit.class)`
- **THEN** its API lists `place` as `json` answering `json`, and `audit` as `text` with no answer

### Requirement: Every SDK SHALL declare descriptions and schemas on entry channels

Every SDK SHALL accept a description on an entry channel and an explicit JSON Schema on a `json`
entry channel. The Java SDK SHALL derive a schema from a record input type, the Python SDK from a
`TypedDict`, a dataclass or a type with `model_json_schema()`, the PHP SDK from a class with typed
constructor-promoted properties, and the Rust SDK from a type implementing `schemars::JsonSchema`
when its `schemars` feature is on. An explicit schema SHALL override a derived one. An explicit
schema that is not a JSON object with a `type` SHALL refuse the class at registration with
`topology.schema_invalid`. A type that cannot be read SHALL give no schema and SHALL NOT refuse
the class.

#### Scenario: A Java record becomes a schema

- **WHEN** a Java entry channel's type is `record PlaceOrder(String pair, double amount, Optional<String> note)`
- **THEN** its schema is an object with required `pair` (string) and `amount` (number) and an
  optional `note` (string)

#### Scenario: Node takes an explicit schema

- **WHEN** a Node entry channel is declared with `{ codec: "json", schema: { type: "object", properties: { pair: { type: "string" } } }, description: "Places an order" }`
- **THEN** the API entry carries that schema and description

#### Scenario: A broken schema is refused at registration

- **WHEN** an Elixir entry channel declares `schema: "object"`
- **THEN** the runner refuses the class with `topology.schema_invalid` and announces nothing for it

### Requirement: The controller SHALL keep the latest API per namespace and factory

The controller SHALL store the API entries a runner announces for a factory, keyed by namespace
and factory, replacing them with each later announcement, and SHALL record when two connected
runners announce different APIs for one factory. The API SHALL never decide placement.

#### Scenario: The API is known before any instance runs

- **WHEN** a runner with factory `support` connects and no `support` instance has started
- **THEN** `Relay.api` for the namespace lists `support`'s channels

#### Scenario: A rolling deploy shows the newest and says so

- **WHEN** two connected runners announce different APIs for `orders`
- **THEN** `Relay.api` answers the later one and marks that the runners disagree, and
  relays of `orders` are still placed on both

### Requirement: `Relay.api` SHALL list a namespace's APIs

`Relay.api(namespace, caller)` SHALL answer, for each factory announced in the namespace, its
id, description and API entries, judged as `flz relay info` is (`relay.list`), and SHALL be a remote
catalogue entry.

#### Scenario: Read over the door

- **WHEN** a namespace `viewer` token calls `Relay.api`
- **THEN** it receives the namespace's factories and their entries

### Requirement: `Relay.call_channel` SHALL call a channel with a value

`Relay.call_channel(namespace, relay_id, channel, value, opts, caller)` SHALL be judged as
`relay.send`, resolve the relay's factory and its API entry for `channel`, encode `value` by
the entry's format (any JSON value for `json`, a string for `text`, a base64 string for `bytes`,
`msgpack` and `custom`), and then call the relay with the deadline in `opts` (default 30000 ms)
when the entry answers, or send to it otherwise. It SHALL decode the answer by the answer's format
and answer `{answer, answer_codec}`, or `{"accepted": true}` for a send. It SHALL refuse a channel
the API lacks with `api.channel_unknown` naming the channels it has, a value that does not fit the
format, or an object lacking a field the schema's `required` lists, or carrying one its
`additionalProperties: false` forbids, with `api.input_invalid` naming the expected form, and a
factory with no announced API with `api.not_advertised`, except that a `text` payload (`opts`
`payload: "text"`, what `flz relay call --text` sends) to such a factory SHALL call the relay as
`Relay.call` does. A `base64` payload SHALL take every check a value takes, and a channel that
does not answer SHALL never be called. Every other failure SHALL keep the code `Relay.call` or
`Relay.send` answers.

#### Scenario: No JSON inside a string

- **WHEN** `Relay.call_channel` is called with channel `place` and the value `{"pair":"EUR/USD","amount":1000}`
- **THEN** the relay receives that JSON object as its input and the call answers the decoded
  JSON answer

#### Scenario: A channel without an answer is a send

- **WHEN** `Relay.call_channel` is called on `audit` with `"checked"`
- **THEN** the input is delivered and the call answers `{"accepted": true}` without waiting for an
  output

#### Scenario: An unknown channel is named

- **WHEN** `Relay.call_channel` names channel `plce` on `orders-eu`
- **THEN** it is refused with `api.channel_unknown`, whose details list `place` and `audit`

#### Scenario: A required field is held at the call

- **WHEN** `Relay.call_channel` is called on `place`, whose schema requires `pair` and `amount`,
  with `{"pair":"EUR/USD"}`
- **THEN** it is refused with `api.input_invalid` naming `amount`, and nothing is delivered

#### Scenario: A base64 payload to an unannounced factory is refused

- **WHEN** no connected runner announces `orders` and `Relay.call_channel` is called on `audit`
  with `payload: "base64"`
- **THEN** it is refused with `api.not_advertised`, and nothing is sent or called

#### Scenario: Bytes travel as base64

- **WHEN** a `bytes` channel is called with the base64 of three bytes
- **THEN** the relay receives those three bytes

### Requirement: Described entry channels SHALL be MCP tools

The MCP endpoint and `flz mcp` SHALL list, for a token bound to a namespace whose role grants
`relay.send` there, one tool per announced entry channel that has a description, named
`<factory>_<channel>` with each part in snake case and every character outside
`[a-zA-Z0-9_-]` made `_`, at most 64 bytes (a longer name cut and ended with a short hash of the
whole), described by the factory's and the
channel's descriptions, with an input schema of `relay` (required) merged into an object
schema's properties, or `relay` with `input`, `text` or `base64` otherwise, and an output schema
when the answer's schema is known. A tool name equal to a catalogue or knowledge tool, or to an
earlier factory's channel's, SHALL NOT be listed. A tool call SHALL be `Relay.call_channel`, a JSON answer returned as structured content,
text as text, bytes as an embedded resource, a send as `accepted`, and a failure as a tool error
carrying the error object. There SHALL be one tool per factory and channel, whatever the number of
instances.

#### Scenario: An agent sees and calls a tool

- **WHEN** factory `orders` describes `place` and an MCP client of the namespace lists tools
- **THEN** `orders_place` is listed with `relay`, `pair` and `amount` arguments, and calling it
  with `{"relay":"orders-eu","pair":"EUR/USD","amount":1000}` returns the answer as structured
  content

#### Scenario: Undescribed channels are not tools

- **WHEN** `audit` has no description
- **THEN** no `orders_audit` tool is listed, and `Relay.api` still lists `audit`

#### Scenario: A thousand sessions are one tool

- **WHEN** a thousand instances of `support` run
- **THEN** `tools/list` lists `support_ask` once

#### Scenario: A viewer sees no relay tools

- **WHEN** a namespace `viewer` token lists tools
- **THEN** no relay tool is listed

