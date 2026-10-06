# A relay's API

Every relay has a public API: the entry channels of its factory, each with the format it
reads, whether it answers and in which format, and, when you give them, a description and a
JSON Schema of its input. Declaring the channels is all it takes. The runner announces the API
when it connects, so it is known before any instance runs, and one API serves every way of
calling a relay: `flz`, HTTP, an SDK and an AI agent's tools.

**Contents**

1. [What a relay's API is](#what-a-relay-s-api-is)
2. [Describe a channel](#describe-a-channel)
3. [Read it](#read-it)
4. [Call it](#call-it)
5. [Formats](#formats)
6. [AI agents and tool calling](#ai-agents-and-tool-calling)
7. [Limits](#limits)
8. [See also](#see-also)

## What a relay's API is

The API belongs to the factory, because every instance runs the same code; a call names a
relay id. `orders-eu` and `orders-us` are two addresses with one API. For each entry channel
it holds:

| Fact | Where it comes from |
| --- | --- |
| the channel name | the `input` declaration |
| its format: `json`, `text`, `bytes`, `msgpack` or `custom` | the codec the channel declares; a parser of your own is `custom` |
| whether it answers, and in which format | an output reachable from the channel; outputs that disagree answer `bytes` |
| a description | `describe` or the `description` option; optional |
| the input's JSON Schema, for a `json` channel | derived from the input type where the SDK can read it, or given |
| the answer's JSON Schema | derived from the output type in Java, Python, PHP and Rust |

A channel that reaches no output does not answer: a call to it is a send, answered `accepted`.

## Describe a channel

A description says what the channel is for. A schema says what it takes. Both are optional,
and only a `json` channel carries a schema. A schema you give overrides a derived one, and must
be a JSON object with a `type`: anything else refuses the class at registration with
`topology.schema_invalid`, so a broken schema never reaches the controller.

<!-- tabs-open -->

### Java

```java
// OrdersRelay.java: a record is read into its schema
public record Order(String pair, BigDecimal amount) {}

c.input(Order.class, "place").describe("Places an order").into(Place.class).output("placed");
c.input(String.class, "audit").into(Audit.class);
```

### Rust

```rust
// orders.rs: the schema given; `derive_schema()` reads it from a `JsonSchema` type
// with the `schemars` feature
c.input::<PlaceOrder>("place")
    .describe("Places an order")
    .schema(serde_json::json!({
        "type": "object",
        "properties": {"pair": {"type": "string"}, "amount": {"type": "number"}},
        "required": ["pair", "amount"]
    }))
    .into::<PlaceTasklet>()
    .output_named("placed");
c.input::<String>("audit").into::<AuditTasklet>();
```

### Node

```js
// orders.mjs: no types at run time, so the schema is given (zod: z.toJSONSchema(Order))
c.input('place', {
  description: 'Places an order',
  schema: {
    type: 'object',
    properties: { pair: { type: 'string' }, amount: { type: 'number' } },
    required: ['pair', 'amount'],
  },
})
  .into(step('place', ({ pair, amount }, ctx) => ctx.emit({ pair, amount, status: 'placed' })))
  .output('placed');
c.input('audit', { codec: 'text' }).into(step('audit', () => {}));
```

### Python

```python
# orders.py: the TypedDict the tasklet takes is read into the schema
class Order(TypedDict):
    pair: str
    amount: float

c.input("place", description="Places an order").into(place).output("placed")
c.input("audit", codec="text").into(audit)
```

### Ruby

```ruby
# orders.rb: the schema is given
ORDER = { type: "object", properties: { pair: { type: "string" }, amount: { type: "number" } },
          required: %w[pair amount] }.freeze

c.input("place", description: "Places an order", schema: ORDER).into(PlaceOrder).output("placed")
c.input("audit", codec: :text).into(AuditOrder)
```

### PHP

```php
// Orders.php: the typed constructor of the class #[Task] names is read into the schema
final class OrderToPlace
{
    public function __construct(public string $pair, public float $amount)
    {
    }
}

#[Task(name: 'orders_place', inputType: OrderToPlace::class, outputType: OrderPlaced::class)]
final class OrdersPlace extends Tasklet
{
    public function run(mixed $input, Ctx $ctx): void
    {
        $ctx->emit(['pair' => $input['pair'], 'amount' => $input['amount'], 'status' => 'placed']);
    }
}

$c->input('place', description: 'Places an order')->into(OrdersPlace::class)->output('placed');
$c->input('audit', codec: ChannelCodec::Text)->into(OrdersAudit::class);
```

### Elixir

```elixir
# orders.ex: the schema is given
c
|> input("place", description: "Places an order", schema: @order)
|> into(PlaceOrder)
|> output("placed")

c |> input("audit", codec: :text) |> into(AuditOrder)
```

<!-- tabs-close -->

A type the SDK cannot read gives no schema and never refuses the class. Python reads a
`TypedDict`, a dataclass and any type with `model_json_schema()` (pydantic, without depending
on it); Java reads records with strings, numbers, booleans, enums, lists, sets, arrays,
`Map<String, V>` and nested records, and an `Optional` or `@Nullable` field is not required.

## Read it

`flz relay info` with a relay id shows that relay's API, with one example call per channel:

```sh
$ flz relay info orders-eu
```

```output
API of relay 'orders-eu' (factory 'orders'):
  place  json → answer json
    Places an order
    schema: {"properties":{"amount":{"type":"number"},"pair":{"type":"string"}},"required":["pair","amount"],"type":"object"}
    MCP tool: orders_place
    flz relay call orders-eu --channel place --arg pair=... --arg amount=...
  audit  text, no answer
    flz relay call orders-eu --channel audit --text <payload>
```

With `--json` the section is the entries as data. `Relay.api` is the same read as a published
operation: the factories of a namespace with their entries, judged as `flz relay info` is.
When two connected runners announce different APIs for one factory, as in a rolling deploy,
the latest one is shown and a line says the runners disagree. The API describes and never
decides where a relay runs.

## Call it

Every way of calling goes through one operation, `Relay.call_channel`: it encodes the value by
the channel's format, calls the relay when the channel answers (sends to it otherwise), and
decodes the answer by the answer's format.

**From the command line.** `--arg` gives one field each, typed by the channel's schema
(`string`, `number`, `integer`, `boolean`); `--text` sends the payload as written:

```sh
$ flz relay call orders-eu --channel place --arg pair=EUR/USD --arg amount=1000
```

```output
{"amount":1000,"pair":"EUR/USD","status":"placed"}
```

```sh
$ flz relay call orders-eu --channel audit --text checked
```

```output
accepted
```

**Over HTTP**, with the [HTTP API plugin](../../plugins/http_api.md): the body is the value as the
channel's format reads it, and the answer comes back in its own content type, or `202` for a
channel that does not answer:

```sh
$ curl -X POST http://127.0.0.1:8765/v1/relays/orders-eu/place \
    -H "Authorization: Bearer $FLEZHA_TOKEN" -d '{"pair":"EUR/USD","amount":1000}'
```

```output
HTTP/1.1 200 OK
content-type: application/json
{"amount":1000,"pair":"EUR/USD","status":"placed"}
```

**From runner code**, beside `send` (see
[`sending_from_runner_code.md`](../advanced/sending_from_runner_code.md)):

<!-- tabs-open -->

### Java

```java
Placed placed = runner.call("orders-eu", "place", new Order("EUR/USD", new BigDecimal("1000")), Placed.class);
```

### Rust

```rust
let placed: serde_json::Value =
    runner.call("orders-eu", "place", &serde_json::json!({"pair": "EUR/USD", "amount": 1000}))?;
```

### Node

```js
const placed = await runner.call('orders-eu', 'place', { pair: 'EUR/USD', amount: 1000 });
```

### Python

```python
placed = runner.call("orders-eu", "place", {"pair": "EUR/USD", "amount": 1000})
```

### Ruby

```ruby
placed = runner.call("orders-eu", "place", { pair: "EUR/USD", amount: 1000 })
```

### PHP

```php
$placed = $runner->call('orders-eu', 'place', ['pair' => 'EUR/USD', 'amount' => 1000]);
```

### Elixir

```elixir
{:ok, placed} = Flezha.SDK.call(runner, "orders-eu", "place", %{"pair" => "EUR/USD", "amount" => 1000})
```

<!-- tabs-close -->

The value is encoded as an output with no codec encodes it: bytes as they are, a string as
UTF-8, anything else as JSON. A channel that does not answer answers `{"accepted": true}`, and a
refusal raises the SDK's error with the controller's code.

**As an AI agent's tool**, below.

## Formats

| Format | Value to call with | Answer |
| --- | --- | --- |
| `json` | any JSON value | the JSON value; the text, when the bytes are not JSON |
| `text` | a string | a string |
| `bytes`, `msgpack`, `custom` | base64 of the bytes (raw bytes over HTTP) | base64 (raw bytes over HTTP, an embedded blob over MCP) |

A value that does not fit the format is refused with `api.input_invalid`, naming the form it
expected, and so is an object that lacks a field its schema's `required` lists, or carries one
the schema forbids with `"additionalProperties": false`. A channel the API lacks is
`api.channel_unknown`, naming the channels it has, and a factory no connected runner has
announced is `api.not_advertised`.

## AI agents and tool calling

Any relay can be a tool an AI agent calls. Every entry channel that has a description is a tool
on the controller's MCP endpoint (`flz mcp`, and `POST /mcp` of the HTTP API plugin), named
`<factory>_<channel>`, each part in snake case (`placeOrder` is `place_order`), every other
character a tool name cannot hold made `_`, and a name past 64 bytes cut and ended with a short
hash of the whole:

- its arguments are `relay` and the fields of the channel's schema (or `input`, `text` or
  `base64` for a channel without an object schema)
- its output schema is the answer's schema, when the SDK knows it
- a call is `Relay.call_channel`: a JSON answer comes back as structured content, text as text,
  bytes as a blob, a send as `accepted`

It is one tool per factory and channel, however many relays run it: a thousand sessions of one
factory are one tool. Relay tools are listed for a token whose role may call relays in its
namespace (`relay.send`), so a viewer sees none. Describe the channels you want an agent to use;
undescribed ones stay callable through `relay_call_channel` but are not listed as tools.

```sh
$ echo '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"orders_place","arguments":{"relay":"orders-eu","pair":"EUR/USD","amount":1000}}}' | flz mcp
```

```output
{"id":1,"result":{"content":[{"type":"text","text":"{\"amount\":1000,\"pair\":\"EUR/USD\",\"status\":\"placed\"}"}],"isError":false,"structuredContent":{"amount":1000,"pair":"EUR/USD","status":"placed"},"_meta":{"io.modelcontextprotocol/serverInfo":{"name":"flezha","version":"0.1.0"}},"resultType":"complete"},"jsonrpc":"2.0"}
```

## Limits

- **The call holds only part of the schema.** It checks an object's `required` fields and
  `"additionalProperties": false`; types and nested shapes are the relay's to read, and MCP
  clients check their arguments against the tool's schema. A payload that does not decode fails
  as any input does.
- **The API is learned from the runners.** A factory whose runners all stopped has no API until
  one connects again, and a call answers `api.not_advertised`. After a controller restart the
  relays themselves are not known until a runner reconnects, so a call answers
  `relay.not_found` until then. Only `flz relay call --text` still calls a relay of a runner
  older than the API, as before.
- **A tool name is given once.** A name a catalogue or knowledge tool already holds, or an
  earlier factory's channel (`a_b`/`c` against `a`/`b_c`), is not listed, and `flz relay info`
  says `tool name taken` for a name a catalogue or knowledge tool holds. Avoid channel names that
  end in another factory's prefix.
- **Only described channels become tools,** so a long tool list is the author's choice.

## See also

- [`running_relays.md`](running_relays.md): starting relays and `flz relay info`
- [`../advanced/relay_calls.md`](../advanced/relay_calls.md): how a call finds its answer
- [`../cli/relay_commands.md`](../cli/relay_commands.md): `flz relay call` and its options
- [`../../plugins/http_api.md`](../../plugins/http_api.md): the HTTP route and the MCP endpoint
- [`../working_with_ai_assistants.md`](../working_with_ai_assistants.md): connecting an assistant
