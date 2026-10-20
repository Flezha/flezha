# Sending and calling

There are three ways to hand a relay an input. A **send** delivers it and is
answered with the delivery verdict. A **call** delivers it and waits for the
one answer the input causes. A **relay call** is one relay calling another
from its graph, with the answer coming back later as an input. Each goes to an
entry channel, and the entry channels with their formats are the relay's
**API**, which every way of sending and calling reads.

**Contents**

1. [A relay's API](#a-relay-s-api)
   1. [What a relay's API is](#what-a-relay-s-api-is)
   2. [Describe a channel](#describe-a-channel)
   3. [Read it](#read-it)
   4. [Formats](#formats)
2. [Sending from runner code](#sending-from-runner-code)
   1. [Send through the controller](#send-through-the-controller)
   2. [From a Kafka consumer](#from-a-kafka-consumer)
   3. [The relays bound on this runner](#the-relays-bound-on-this-runner)
   4. [Send locally](#send-locally)
   5. [Refusals of a send](#refusals-of-a-send)
3. [Calls](#calls)
   1. [How a call finds its answer](#how-a-call-finds-its-answer)
   2. [Answering: the relay's outputs](#answering-the-relay-s-outputs)
4. [Outside calls](#outside-calls)
   1. [`flz relay call`](#flz-relay-call)
   2. [From runner code](#from-runner-code)
   3. [Over HTTP and MCP: `Relay.call_channel`](#over-http-and-mcp-relay-call_channel)
   4. [`Flezha.Service.Relay.call/6`](#flezha-service-relay-call-6)
   5. [A feeding plugin](#a-feeding-plugin)
   6. [A partition group](#a-partition-group)
   7. [Authorization of a call](#authorization-of-a-call)
5. [AI agents and tool calling](#ai-agents-and-tool-calling)
6. [Relay calls: `call(...)`](#relay-calls-call)
   1. [Where a call goes](#where-a-call-goes)
   2. [The `Response`](#the-response)
   3. [A steps relay as the caller](#a-steps-relay-as-the-caller)
   4. [Authorization and quotas](#authorization-and-quotas)
7. [Example: a gateway that calls a session](#example-a-gateway-that-calls-a-session)
8. [When a call ends early](#when-a-call-ends-early)
   1. [Error codes](#error-codes)
9. [Guarantees](#guarantees)
10. [Order and bounds](#order-and-bounds)
11. [Observing calls](#observing-calls)
12. [Limits](#limits)
13. [See also](#see-also)

## A relay's API

Every relay has a public API: the entry channels of its factory, each with the format it
reads, whether it answers and in which format, and, when you give them, a description and a
JSON Schema of its input. Declaring the channels is all it takes. The runner announces the API
when it connects, so it is known before any instance runs, and one API serves every way of
calling a relay: `flz`, HTTP, an SDK and an AI agent's tools.

### What a relay's API is

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

### Describe a channel

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

### Read it

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
`Relay.api` shows the latest one and a line says the runners disagree, while a relay's own API,
in `flz relay info <relay_id>` and at a call, is the one its runner announced. The API describes
and never decides where a relay runs.

### Formats

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

## Sending from runner code

Application code in a runner process often holds a source of its own: a Kafka
consumer, a queue client, a poll loop. `runner.send` hands each message it reads
to a relay in the runner's namespace. It goes through the controller by
default, under the same role check as the [`control()`](namespaces.md#administering-from-application-code)
handle, and answers the controller's verdict. Two options make a source that reads
a record again safe to repeat: a message id and a partition key. A third, `local`,
skips the controller for input you can afford to lose.

### Send through the controller

A send names the relay, the message and the entry channel, and blocks for the
answer: the relay that took the message and its verdict.

| Verdict | Means |
| --- | --- |
| `delivered` | the relay took the message |
| `absorbed` | a repeat of a message id the relay's input log still holds; the message is not handled again |
| `suspended` | the relay is suspended or moving, or keeps an input log and has no runner bound yet; the log holds the message until it wakes or a runner binds |

Two options ride along:

- **A message id**, stamped on the input as `flz.msg_id`. A relay with an input
  log (the [effectively-once tier](effectively_once.md), a durable steps relay, a
  class that sleeps) absorbs a repeat of an id it still holds. A relay without
  one handles every send. [`durability.md`](durability.md#delivery) says
  which relays keep one and for how long.
- **A partition key**: the member of a [partition group](partitioning.md) whose range
  owns the key takes the message, and the answer names that member. A send to a
  group without a key is refused naming its members.

<!-- tabs-open -->

### Java

```java
// Send.java: one order to the orders group, by id and pair
JsonNode answer = runner.send(
        "orders", "{\"pair\":\"EURUSD\",\"qty\":5}", "in", SendOpts.of().msgId("orders/0/41").key("EURUSD"));
```

### Rust

```rust
// send.rs: one order to the orders group, by id and pair
let opts = flezha_runner::SendOpts::new().msg_id("orders/0/41").key("EURUSD");
let answer = runner.send("orders", r#"{"pair":"EURUSD","qty":5}"#, "in", &opts)?;
```

### Node

```javascript
// send.mjs: one order to the orders group, by id and pair
const answer = await runner.send('orders', '{"pair":"EURUSD","qty":5}', 'in', {
  msgId: 'orders/0/41',
  key: 'EURUSD',
});
```

### Python

```python
# send.py: one order to the orders group, by id and pair
answer = runner.send("orders", '{"pair":"EURUSD","qty":5}', "in", msg_id="orders/0/41", key="EURUSD")
```

### Ruby

```ruby
# send.rb: one order to the orders group, by id and pair
answer = runner.send_to("orders", '{"pair":"EURUSD","qty":5}', "in", msg_id: "orders/0/41", key: "EURUSD")
```

### PHP

```php
// send.php: one order to the orders group, by id and pair
$answer = $runner->send('orders', '{"pair":"EURUSD","qty":5}', 'in', msgId: 'orders/0/41', key: 'EURUSD');
```

### Elixir

```elixir
# send.ex: one order to the orders group, by id and pair
{:ok, %{"verdict" => verdict}} =
  Flezha.SDK.send(runner, "orders", ~s({"pair":"EURUSD","qty":5}), "in",
    msg_id: "orders/0/41",
    key: "EURUSD"
  )
```

<!-- tabs-close -->

Ruby spells it `send_to`, because every Ruby object already has a `send`. The same
options are on the command line: `flz relay send orders --text x --key EURUSD
--msg-id orders/0/41`.

### From a Kafka consumer

A consumer that commits its offset only after the controller answered never loses
a record: a crash between the two reads the record again, and the message id, built
from the record's position, makes the repeat `absorbed` on a relay with an input
log. Here a Java runner reads the `orders` topic and keys each order by its currency
pair:

```java
// OrderFeed.java: the runner's own Kafka consumer feeding the orders group
Properties props = new Properties();
props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
props.put(ConsumerConfig.GROUP_ID_CONFIG, "order-feed");
props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
    consumer.subscribe(List.of("orders"));
    while (true) {
        for (ConsumerRecord<String, String> order : consumer.poll(Duration.ofMillis(500))) {
            String id = order.topic() + "/" + order.partition() + "/" + order.offset();
            runner.send("orders", order.value(), "in", SendOpts.of().msgId(id).key(order.key()));
        }
        consumer.commitSync();
    }
}
```

A refused send throws, so nothing after it is committed and the batch is read
again. What `delivered` promises beyond that is the relay's own durability
level, not the sender's.

### The relays bound on this runner

Each runner answers the ids of the relay instances bound on it, without asking
the controller. A partition member is listed by its member id, such as
`orders#p1`; a group id never is.

| SDK | Spelling |
| --- | --- |
| Rust | `runner.relays()?`, a `Vec<String>` |
| Java | `runner.getRelays()`, a `List<String>` |
| Node | `runner.relays()` |
| Python | `runner.relays()` |
| Ruby | `runner.relays` |
| PHP | `$runner->relays()` |
| Elixir | `Flezha.SDK.relays(runner)`, as `{:ok, ids}` |

The list is a moment's answer: the controller may move a relay to another
runner right after it.

### Send locally

The `local` option hands the message to a relay bound on this runner, in the
same process, without the controller. It enters where a frame off the wire enters,
so the channel's decoder, transforms and keying run as for any input, and the state
changes and outputs it causes travel to the controller as any others do. It blocks
while the consumer's mailbox is full and returns once the message is routed,
answering nothing beyond success. In Elixir a relay takes its input as process
messages, so there the send returns at once, as a frame off the wire does.

> #### A local send is not durable {: .warning}
>
> A local message is on no disk: the controller never sees the input, so it takes
> no input-log offset, and a crash before it is handled loses it. On a relay with
> an input log (effectively once, durable steps), the relay's guarantee covers
> only input that came through the controller; a local message is handled at most
> once, and a replay never repeats it. Send locally only what you can afford to lose.

<!-- tabs-open -->

### Java

```java
// Ticks.java: a tick to the ticks relay on this runner
runner.send("ticks", "EURUSD 1.0841", "in", SendOpts.of().local());
```

### Rust

```rust
// ticks.rs: a tick to the ticks relay on this runner
runner.send("ticks", "EURUSD 1.0841", "in", &flezha_runner::SendOpts::new().local())?;
```

### Node

```javascript
// ticks.mjs: a tick to the ticks relay on this runner
await runner.send('ticks', 'EURUSD 1.0841', 'in', { local: true });
```

### Python

```python
# ticks.py: a tick to the ticks relay on this runner
runner.send("ticks", "EURUSD 1.0841", "in", local=True)
```

### Ruby

```ruby
# ticks.rb: a tick to the ticks relay on this runner
runner.send_to("ticks", "EURUSD 1.0841", "in", local: true)
```

### PHP

```php
// ticks.php: a tick to the ticks relay on this runner
$runner->send('ticks', 'EURUSD 1.0841', 'in', local: true);
```

### Elixir

```elixir
# ticks.ex: a tick to the ticks relay on this runner
{:ok, nil} = Flezha.SDK.send(runner, "ticks", "EURUSD 1.0841", "in", local: true)
```

<!-- tabs-close -->

A local send to a relay this runner does not hold is refused, never forwarded:
send it without `local`, and the controller routes it to the runner that holds it.

### Refusals of a send

| Code | When |
| --- | --- |
| `runtime.relay_not_local` | a local send to a relay not bound on this runner, or to a partition group |
| `runtime.channel_undeclared` | a local send to a channel the relay does not declare; the refusal lists its entry channels |
| `runtime.channel_wrong_kind` | a local send to a declared channel that is not an entry channel |
| `usage.options_conflict` | `local` with a message id or a key, refused before anything is sent |
| `usage.not_text` | a message id that is not text, from a caller that builds the request itself |
| `call.target_unkeyed` | a send to a partition group without a key; the refusal names the members |
| `relay.not_found` | a send through the controller to a relay that is not running |
| `authz.role_insufficient` | the namespace's runner principal has no role that allows the send |

[`errors.md`](../../errors.md) lists every code.

## Calls

A call sends one input to a relay and gets one answer back: the first output that input caused
on an output of the relay's graph, or one coded error saying why none came.
There are two kinds:

- An **outside call** comes from outside Flezha and waits for its answer: `flz relay call`,
  runner code through `runner.call`, HTTP and MCP, application code through
  `Flezha.Service.Relay.call/6`, or a plugin feeding the relay it
  is enabled on (the [HTTP server plugin](../../plugins/http_server.md) answers each request
  this way).
- A **relay call** is one relay calling another from its graph, with a `call(...)` end.
  Nothing waits: the outcome comes back later as an ordinary input, a `Response`, on the caller's
  Response channel. A call to a factory can also start the instance it calls.

Flow and steps relays both answer calls and make them. Calls exist so that a request and its
answer can cross relays without the caller polling state or listening to an output: a gateway
calls a user's session and counts the tokens its answer used; an order desk calls a pricing
relay; a script calls a relay and prints what it answered.

### How a call finds its answer

```mermaid
flowchart LR
    P(["request"]) --> G["calling relay"]
    G -->|"call"| S["called relay"]
    S -->|"first output"| A{{"answer"}}
    A -.->|"Response, later, as an input"| G
```

The controller ties an input to the outputs it causes with a token in the input's metadata,
`flz.reply_to`. It creates one for every outside call, after removing any `flz.reply_to` the
caller's metadata carried. Every other way into a relay drops that key, so no hook, plugin or
sender can add, drop or change one. The token is journaled with the input wherever the relay
journals input, so a sleep, a move to another runner or a crash re-run deliver it again. The
runner carries it from the input to every value derived from it and stamps it on each output
the input causes; a tasklet never sees it, and a timer firing carries none. `flz relay
listen --input` shows it on the arriving input, but it is of no use there: an answer only ever
goes to the caller waiting for it, and no plugin or listener sees it on an output.

The answer may come from any tasklet downstream of the input, not only the one that received it:
in `input("in") |> into(Parse) |> into(Price) |> output("quotes")` the value `Price` emits answers
the call. In a steps relay the answer comes from the pass the call's input opened.

### Answering: the relay's outputs

A relay does not say which of its channels answers a caller. Whether an answer is wanted is a
fact of how an input arrived: when it came from a call, the first output it causes on an output
answers the call. That output then goes on to the output's plugins and to `flz relay listen` like
any other output, so one tasklet answers a caller and records the answer with no extra step.
Tasklets do not change.

- **The caller may name the answering channel.** `flz relay call --answer-channel quotes`,
  `answer_channel` in the options of `Relay.call/6`, or the answer channel of a `call(...)` end
  (see [below](#where-a-call-goes)). Then only an output on that channel answers, and every other
  output the input causes is an ordinary output. Without a name, the first output answers.
- **Several outputs per graph.** An accepted path and a refused path may each end in an output;
  whichever output comes first answers, unless the caller names a channel.
- **One answer per call.** Later outputs of the same input are ordinary outputs.
- **Parts never answer.** A [part](sessions.md#streamed-answers) goes to listeners while the tasklet works;
  only a finished emit answers.
- **Every output is keyed under the effectively-once tier**, answering ones included, because an
  answer also reaches the output's plugins, which drop a duplicate by its key.
- **One plugin end per channel.** An output and a `call(...)` on one channel, or two `call(...)`
  ends, are refused when the relay is declared, as `topology.channel_ends_twice`.

#### Where an answer leaves

An answer leaves the runner exactly where an output of the same relay leaves, so a call is
never answered before the state change its turn made is durable, where the relay's
[durability](durability.md) promises that:

| Relay | The answer leaves |
| --- | --- |
| off the effectively-once tier | at settle, with no durability wait |
| under the effectively-once tier | after the effect record that carries it is synced to disk |
| a durable steps relay | when the step that produced it commits (for a one-tasklet turn, the iteration's commit) |
| a steps relay that journals nothing | at settle |

An output that answers a call goes to its caller first, and then on to the `on_output` hooks
without the token, so output plugins and `flz relay listen` see it as an ordinary output. Then:

- the first matching output for a waiting call goes to its caller and ends the call;
- every later output of the same input, and every output whose input nobody waits for (it came
  through `flz relay send`, a plugin or a forward, or its caller already timed out), is an
  ordinary output and nothing more.

## Outside calls

Every outside call carries a positive deadline in milliseconds, measured by the called relay
from the moment the call reached it. With no answer by then, the caller receives `call.timeout`.
The input stays accepted: its turn still runs, and its outputs still reach the relay's plugins. A slow turn
that costs something, such as a paid request to another service, still costs it after its caller
has given up. A call to a sleeping instance is journaled, the append wakes it, and the time the
wake takes counts against the deadline.

### `flz relay call`

```text
flz relay call <relay_id> (--text <payload> | --arg <name=value>...) [--channel <ch>] [--answer-channel <ch>] [--key <k>] [--timeout <ms>] [--namespace <ns>]
```

It prints the answer and a newline on stdout and exits 0. `--channel` names the entry
channel the input arrives on, and `--answer-channel` the channel whose output answers. `--text`
sends the payload as written; `--arg` gives one field of a JSON input each, typed by the
channel's schema in the relay's API ([`sending_and_calling.md`](#a-relay-s-api)). The call
goes through `Relay.call_channel`, so a channel that does not answer prints `accepted`. Without
`--timeout` the deadline is the controller's `call_channel_deadline_ms` (30000 unless set);
`--timeout` has no short spelling, because `-t` is `--text`. A failure is written on stderr with its code
and exits by its class (see [the CLI](../cli/cli.md)):

```sh
$ flz relay call echo --text hello
```

```output
hello
```

```sh
$ flz relay call slow --text x --timeout 100
```

```output
error: relay 'slow' did not answer within 100 ms; the input stays accepted and its turn still runs (call.timeout)
```

A channel called by its fields (each typed `string`, `number`, `integer` or `boolean` by the
schema), and one that does not answer:

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

### From runner code

`runner.call` sits beside `send`: it calls an entry channel of a relay with a
value, by the relay's API ([`sending_and_calling.md`](#a-relay-s-api)),
and answers what the relay answers. It goes through the controller under the
same role check as a send.

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

The value is encoded as an output with no codec encodes it: bytes as they are,
a string as UTF-8, anything else as JSON. The answer is decoded by its format:
JSON into the requested type (Java) or the language's JSON value, text as a
string, bytes as bytes. A channel that answers nothing answers
`{"accepted": true}`. The control handle has no `call`: calling is a data call,
so it sits on the runner.

### Over HTTP and MCP: `Relay.call_channel`

```elixir
Flezha.Service.Relay.call_channel(namespace, relay_id, channel, value, opts, caller_context)
```

The call by value: it reads the relay's API, encodes `value` by the channel's format (any JSON
value for `json`, a string for `text`, base64 for the binary formats), calls the relay when
the channel answers and sends to it otherwise, and answers `%{answer, answer_codec}` or
`%{accepted: true}`. `opts` carries `deadline_ms` (default `call_channel_deadline_ms`), `answer_channel`, `key`, and
`payload` (`text` or `base64`) to send `value` as the payload itself. It is judged as `call/6`.
The [HTTP API plugin](../../plugins/http_api.md) serves it as `POST /v1/relays/<id>/<channel>`,
and the MCP endpoint lists each described channel as a tool an AI agent calls
([below](#ai-agents-and-tool-calling)).

Over HTTP the body is the value as the channel's format reads it, and the answer comes back in its own content type, or `202` for a
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

### `Flezha.Service.Relay.call/6`

```elixir
Flezha.Service.Relay.call(namespace, relay_id, message, channel, opts, caller_context)
```

`channel` is the entry channel, `nil` or `""` for none. `opts` carries `deadline_ms` (required, a
positive integer), `answer_channel` (the channel whose output answers) and `key`. It answers `{:ok, payload, %{"channel" => channel}}`, naming the
channel that produced the answer, or `{:error, %Flezha.Error{}}`. Its own wait lasts the deadline
plus `call_grace_ms`, so the relay's `call.timeout` always reaches the caller first.

A connected runner reaches it through the administration surface, like every published
function. That surface is for application code outside relays: a tasklet never makes a call
from `run`; one relay calls another from its graph.

### A feeding plugin

```elixir
Flezha.Plugin.Host.inject(overwatch, message, metadata, :call, deadline_ms)
```

A plugin calls the relay it is enabled on and receives what `Relay.call/6` answers. Its wait
is bounded the same way, and a relay it cannot reach answers `call.unavailable`. A
`flz.reply_to` in `metadata` is dropped: the answer reaches this call and no other. See
[writing a plugin](../plugins/writing_a_plugin.md).

### A partition group

A [partition group](partitioning.md) is called by key: `--key EURUSD` on the CLI,
`key: "EURUSD"` in `opts`, `?key=EURUSD` on the HTTP route, or `"key": "EURUSD"` among an MCP
relay tool's arguments, reaches the member whose key-group range owns the key, the way
`flz state get` finds it. A group called with no key is refused as `call.target_unkeyed`, naming
its members; a member named directly (`prices#p0`) is called directly. `flz relay send` takes
`--key` the same way.

### Authorization of a call

A call through `flz relay call`, `Relay.call/6` or `Relay.call_channel/6` is judged as `relay.send` in the
relay's namespace and audited like a send, naming the relay, with the error code on a
failure. A caller allowed to send can so read any output its own input causes on an output, and
pick which one with the answer channel; outputs of other inputs stay behind `relay.listen`. A refused call delivers no input and keeps its own `authz.*` code (exit 4). A plugin's
call on its own relay is that relay's own input and is not judged again. See
[authorization](authorization.md).

## AI agents and tool calling

Any relay can be a tool an AI agent calls. Every entry channel that has a description is a tool
on the controller's MCP endpoint (`flz mcp`, and `POST /mcp` of the HTTP API plugin), named
`<factory>_<channel>`, each part in snake case (`placeOrder` is `place_order`), every other
character a tool name cannot hold made `_`, and a name past 64 bytes cut and ended with a short
hash of the whole:

- its arguments are `relay`, `key` (needed only when `relay` names a
  [partition group](partitioning.md)) and the fields of the channel's schema (or `input`, `text`
  or `base64` for a channel without an object schema, and `input` for one whose schema has its
  own `relay` or `key`)
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

## Relay calls: `call(...)`

One relay calls another by ending a chain of its graph in `call(...)`. Every value that
reaches the end, after the chain's transforms and key, is one request. The call waits for
nothing; its outcome comes back later as a `Response` on the relay's **Response channel**. The
tasklet that made the call may read it, and so may any other: wiring the Response channel back
into the calling tasklet is fan-in, not a cycle.

### Where a call goes

| Target | Reaches |
| --- | --- |
| an id, `call("pricing", ...)` | the instance `pricing`, or, when `pricing` is a partition group, the member that owns the request's key (from a `key_by` before the end) |
| a factory after a key, `key_by(...)` then `call(factory: "agent", ...)` | the instance of `agent` whose id is the request's key, started with the request as its first input when no instance holds that id |
| a factory without a key, `call(factory: "triage", ...)` | a new steps instance of `triage` per call, under a generated id |

- The request enters the called relay on the entry channel `channel` names, or on its first
  declared entry channel.
- The called relay's output on the channel the answer channel names answers the call, or, when
  the end names none, its first output for the request.
- A target is always in the caller's namespace.
- Creating and delivering go through the controller's one spawn-or-deliver (the one
  [`ctx.spawn_steps`](workflows.md#starting-one-from-a-tasklet) uses), so two calls racing for one absent id
  start one instance and deliver the other request to it.
- A factory call reaches only an instance of that factory: a key naming a running instance of
  another factory is answered `relay.factory_mismatch` and nothing is delivered, so a key that
  came from a client cannot steer a request into an unrelated relay.
- With `create: false`, an absent id is answered `relay.not_found` and nothing starts. A
  gateway uses it for the turns of a session that may have ended, so an expired session is
  refused rather than silently started again.
- A factory call without a key cannot say `create: false`, and one to a flow factory is refused
  as `relay.factory_not_steps`, since nothing would ever end the instances it started. A key
  may start a flow instance: it runs until it is stopped, and sleeps when idle as its settings
  say.

| SDK | Spelling | Answer channel |
| --- | --- | --- |
| Rust | `.call::<Answer>(CallTarget::id("pricing"), 500)`, `CallTarget::factory("agent").create(false).channel("turn")` | `CallTarget::id("pricing").answer_channel("quotes")` |
| Java | `.call(CallTarget.id("pricing"), Answer.class, 500)`, `CallTarget.factory("agent").create(false).channel("turn")` | `CallTarget.id("pricing").answerChannel("quotes")` |
| Node | `.call("pricing", { deadlineMs: 500, codec })`, `.call({ factory: "agent", create: false, channel: "turn" }, { deadlineMs })` | `{ deadlineMs: 500, answerChannel: "quotes" }` |
| Python | `.call("pricing", deadline_ms=500)`, `.call(factory="agent", deadline_ms=..., create=False, channel="turn", codec=...)` | `answer_channel="quotes"` |
| Ruby | `.call("pricing", deadline_ms: 500)`, `.call(factory: "agent", deadline_ms: ..., create: false, channel: "turn", codec: ...)` | `answer_channel: "quotes"` |
| PHP | `->call(target: 'pricing', deadlineMs: 500)`, `->call(factory: 'agent', deadlineMs: ..., create: false, channel: 'turn', codec: ...)` | `answerChannel: 'quotes'` |
| Elixir | `call(s, "pricing", deadline_ms: 500)`, `call(s, factory: "agent", deadline_ms: ..., create: false, channel: "turn", serialize: f, decode: g)` | `answer_channel: "quotes"` |

The request is written the way an `output` writes, and the answer read the way an `input` reads: the
answer type in Rust and Java, one named codec for both directions in Node, Python, Ruby and PHP
(JSON by default), and optional `serialize:` and `decode:` functions in Elixir (by default the
output rule and the raw binary; the Elixir `Response` carries the request as the bytes that were
sent).

Registration refuses:

| Code | When |
| --- | --- |
| `topology.channel_ends_twice` | the channel already ends in an output or another call |
| `topology.call_deadline_zero` | the deadline is zero |
| `topology.call_without_key` | a factory call with `create: false` and no `key_by` before it |
| `topology.call_response_unread` | the Response channel feeds no tasklet, so every outcome would be journaled and read by nobody; use `relay_forward` when no outcome is wanted |
| `topology.call_outside_per_input_loop` | a call in a `once` or `chained` steps relay, which never reads the later message a `Response` is |

### The `Response`

The Response channel is an entry channel named after the calling channel with `.response`
appended (`gateway.response`). Each `Response` carries the request as the call end wrote it, the
result (the answer, or the error that ended the call, in the SDK's one error type; a timeout is
`call.timeout`), `target` (the instance that answered, a generated one included) and `created`
(whether this call started it), so a gateway can tell a new session from one that expired.

| SDK | Type |
| --- | --- |
| Rust | `flezha_runner::Response<Req, Ans>`: `request`, `result: Result<Ans, FlezhaError>`, `target`, `created` |
| Java | `Response<Req, Ans>`: `request()`, `answer()`, `error()`, `ok()`, `target()`, `created()` |
| Node | `Response<Req, Ans>`: `request`, `value` or `error`, `target`, `created` |
| Python | `Response`: `request`, `value` or `error`, `target`, `created` |
| Ruby | `Flezha::Response`: `request`, `value` or `error`, `ok?`, `target`, `created` |
| PHP | `Flezha\Response`: `request`, `value` or `error`, `ok()`, `target`, `created` |
| Elixir | `%Flezha.SDK.Response{request, result: {:ok, value} \| {:error, error}, target, created}` |

### A steps relay as the caller

A `per_input` steps relay calls without suspending: the request leaves when the step that
emitted it commits, and the `Response` arrives on the Response channel as a later message, run as
a later iteration. An agent session calls a tool relay this way. An instance that ends with
calls pending abandons them; their outcomes are dropped and counted as `unclaimed`.

### Authorization and quotas

A relay call is judged as its namespace's runner identity, as a tasklet's `ctx.spawn_steps`
is: creating the called instance against `relay.start` (audited as a start naming the new
instance), delivering the request against `relay.send` (no record of its own). The runner
identity needs the `spawner` or `app` role in the namespace; a refusal ends the call with its
`authz.*` code and starts and delivers nothing. The `Response`'s way back is the caller's own and
is not judged again.

A session a call creates counts against its namespace's `max_relays` from its start to its
teardown, asleep or awake; a creation past it is answered `quota.exceeded`, and calls to running
sessions are still delivered. A session that ended (it completed, or expired itself on a cell
timer and `ctx.complete`) no longer holds its id: a call creates a new empty instance and says
`created: true`, or, with `create: false`, is answered `relay.not_found` before any turn runs.

## Example: a gateway that calls a session

In the agentdesk example, a `gateway` relay takes HTTP requests from the
[HTTP server plugin](../../plugins/http_server.md). A POST to `/sessions/<user>/messages` is a
message for the user's session. The gateway keeps each user's daily token budget in a `budget`
cell per user, answers the request itself with 202 or 429 on an output, and,
when the message is admitted, **calls** the user's session with it. The tokens the answer used
come back as a `Response` into the same `budget` tasklet.

<!-- tabs-open -->

### Java

```java
// GatewayRelay.java: answers every request, and calls the session a message is for.
        Stream<Verdict> verdicts = http.filter(Request::sends)
                .map(Event.class, request -> new Sent(request.message()))
                .keyBy(Event::user)                          // one cell per user
                .into(Budget.class);
        verdicts.into(Admit.class)
                .keyed()
                .output();                                   // 202, or 429 over the budget
        verdicts.filter(Verdict::admitted)
                .map(Message.class, Verdict::message)
                .keyBy(Message::user)                        // the user's session
                .call(CallTarget.factory("agent").channel("message"), Answer.class, DEADLINE_MS)
                .map(Event.class, Used::of)
                .keyBy(Event::user)
                .into(Budget.class);                         // the tokens the answer used
```

[Full file](https://github.com/flezha/flezha_java_sdk/blob/main/examples/agentdesk/src/main/java/com/flezha/examples/agentdesk/GatewayRelay.java)

### Elixir

```elixir
# lib/agentdesk/gateway_relay.ex: answers every request, and calls the session a message is for.
    verdicts
    |> into(Admit)
    |> keyed()
    # 202, or 429 over the budget
    |> output()

    verdicts
    |> filter(&Budget.admitted?/1)
    |> map(& &1.message)
    # the user's session
    |> key_by(& &1.user)
    |> call(
      factory: "agent",
      channel: "message",
      deadline_ms: @deadline_ms,
      decode: &Jason.decode/1
    )
    |> map(&Budget.used/1)
    |> key_by(& &1.user)
    # the tokens the answer used
    |> into(Budget)
```

[Full file](https://github.com/flezha/flezha_elixir_sdk/blob/main/examples/agentdesk/lib/agentdesk/gateway_relay.ex)

<!-- tabs-close -->

- **`call` after `keyBy`** reaches the instance of `agent` whose id is the key, the user, and
  starts one with the message as its first input when there is none, so a new user needs no
  `flz relay start`. `channel("message")` is the entry channel it arrives on.
- **The call waits for nothing.** Its outcome comes back later as a `Response` on the gateway's
  Response channel, within the 60-second deadline at the latest.
- **Two outputs** in one graph (the other answers the non-message requests): whichever output
  comes first answers the request. The 202 leaves before the session has read the message. Both
  are `keyed()`, as every output of an effectively-once relay is.

The `Response` carries the request it answers, which is where the user comes from. A turn that
failed, or took too long, used no tokens:

<!-- tabs-open -->

### Java

```java
// Budget.java: each user's tokens for the day, and whether a message is admitted.
    public record Used(String user, int tokens) implements Event {    // the tokens an answer used
        static Used of(Response<Message, Answer> response) {          // a failed turn used none
            return new Used(response.request().user(), response.ok() ? response.answer().tokens() : 0);
        }
    }
```

[Full file](https://github.com/flezha/flezha_java_sdk/blob/main/examples/agentdesk/src/main/java/com/flezha/examples/agentdesk/Budget.java)

### Elixir

```elixir
# lib/agentdesk/budget.ex: each user's tokens for the day, and whether a message is admitted.
  # The tokens an answer used; a failed turn used none.
  def used(%Response{request: request, result: result}) do
    %{user: Jason.decode!(request)["user"], tokens: tokens(result)}
  end

  defp tokens({:ok, answer}), do: answer["tokens"]
  defp tokens({:error, _error}), do: 0
```

[Full file](https://github.com/flezha/flezha_elixir_sdk/blob/main/examples/agentdesk/lib/agentdesk/budget.ex)

<!-- tabs-close -->

On the called side, the session takes the gateway's message on an entry channel of its own,
`message`, beside the `turn` channel the CLI uses, and answers from its output `answer`, which
listeners read too. Both channels reach the same `reply` tasklet, with the same conversation:

<!-- tabs-open -->

### Java

```java
// AgentRelay.java: one conversation, answering whoever called.
        c.input(String.class, "turn")
                .into(Reply.class)
                .output("answer");                       // to whoever called, and whoever listens
        c.input(Message.class, "message")                // from the gateway
                .map(String.class, Message::text)
                .into(Reply.class);
```

[Full file](https://github.com/flezha/flezha_java_sdk/blob/main/examples/agentdesk/src/main/java/com/flezha/examples/agentdesk/AgentRelay.java)

### Elixir

```elixir
# lib/agentdesk/agent_relay.ex: one conversation, answering whoever called.
    c
    |> input("turn", codec: :text)
    |> into(Reply)
    # to whoever called, and whoever listens
    |> output("answer")

    # from the gateway
    c
    |> input("message")
    |> map(& &1["text"])
    |> into(Reply)
```

[Full file](https://github.com/flezha/flezha_elixir_sdk/blob/main/examples/agentdesk/lib/agentdesk/agent_relay.ex)

<!-- tabs-close -->

A message for a user with no session starts one:

```sh
$ curl -s -d 'Hi, I am Carol' localhost:8080/sessions/carol/messages
```

```output
sent to carol
```

```sh
$ flz state get carol/reply --namespace agents
```

```output
state get carol/reply:
  change_seq: 1
  state:      {"reply":{"messages":["Hi, I am Carol"]}}
```

The budget is 40 tokens a day, and an answer to a one-word message uses 10, so the fifth one-word
message of the day is refused:

```sh
$ for text in One Two Three Four Five; do curl -s -o /dev/null -w '%{http_code}\n' -d "$text" localhost:8080/sessions/dave/messages; sleep 2; done
```

```output
202
202
202
202
429
```

Tokens are counted when an answer arrives, so a message sent while the previous answer is still
being written is admitted on the count before it: the budget can be overrun by the answers in
flight, never by more. The day is taken from `ctx.now()`, not from the clock, so a replay under
[effectively-once](effectively_once.md) agrees with the live run. The budget is the gateway's: a
message sent with the CLI to the session's `turn` channel does not go through it, and is neither
counted nor refused. The HTTP route authenticates nobody: anyone who reaches the port can write
to any user's session.

## When a call ends early

A call is answered at once, with a code, whenever its answer can no longer come or the call
cannot be taken:

- **The called relay's Overwatch (its controller-side process) moves.** Waiting callers move
  with it to the other node and are still answered there, or time out at their original
  deadline.
- **The Overwatch crashes.** Every caller waiting on it receives `call.unavailable` at once, not at
  its deadline. The journaled input still runs.
- **The input is dead-lettered.** The caller receives `call.failed` once the dead-letter entry is
  durable (for a failed turn of a durable `per_input` `keep_progress` relay, once the turn's
  record is), carrying the entry's reference and the error text. The input stays in the
  dead-letter queue (`flz dlq list`), and a replay can still succeed; its answer then reaches
  nobody.
- **The instance ends.** When a run completes, fails or is stopped, every waiting caller receives
  `call.callee_ended` with the run's final status: `ok` (a steps run), `completed` (a flow
  instance's `complete`), `error` or `cancelled`. The exception is a relay caller of an
  instance that keeps a durable input log and was stopped without ending: its call stays
  journaled and is answered when the instance runs again, or by the caller's deadline.

A relay caller receives each of these as a `Response` carrying the error, and also
`call.not_accepted` from a `once` or `chained` instance that has taken its input, and the
controller's own codes for a creation or delivery it refuses (`quota.exceeded`,
`relay.factory_unknown`, `relay.not_found`, `relay.factory_mismatch`, an `authz.*`
code). A `per_input` `keep_progress` session answers a failed turn `call.failed` and goes on; the
calls queued behind it are answered by their own turns. A rewind of the called relay neither
answers a call again nor takes an answer back; a turn it interrupts before its answer left leaves
its caller to the deadline.

### Error codes

The exit code is `flz`'s for the class ([errors](../../errors.md)).

| Code | Class (exit) | When |
| --- | --- | --- |
| `relay.not_found` | not_found (5) | no relay or member of that id in the namespace |
| `call.timeout` | unavailable (7) | the deadline passed with no answer; `details.deadline_ms` |
| `call.failed` | unavailable (7) | the relay dead-lettered the input; `details.dlq_ref`, `details.error` |
| `call.callee_ended` | unavailable (7) | the instance ended before it answered; `details.status` |
| `call.absorbed` | conflict (6) | the input was dropped as a duplicate, or an `on_input` hook filtered it out |
| `call.not_accepted` | conflict (6) | a `once` or `chained` steps instance has already taken its input; nothing is journaled |
| `call.busy` | unavailable (7) | the relay already owes `call_pending_max` answers; nothing is taken in |
| `call.unavailable` | unavailable (7) | the relay is halted, or not ready with no input log to hold the input, its durable append was refused, or its Overwatch went away |
| `call.target_unkeyed` | invalid (2) | a partition group was called with no key; `details.members` |
| `relay.factory_mismatch` | conflict (6) | a factory call named an instance of another factory; nothing is delivered |
| `topology.channel_ends_twice` | invalid (2) | a channel declares two plugin ends (raised when the relay is declared) |
| `topology.call_*` | invalid (2) | a `call(...)` end the runtime cannot honour (see above) |

## Guarantees

A call and its `Response` are ordinary inputs, and each side keeps the guarantee of its own
durability: the request is journaled in the called relay's input log when it journals input,
the `Response` in the caller's when the caller does.

| Caller and called relay | A call |
| --- | --- |
| both keep a durable input log (an effectively-once flow relay, a durable steps one) | survives a crash of either side's runner, Overwatch or controller, runs its turn at most once to its commit, and ends with exactly one `Response`. A request still on its way when only the controller's call delivery crashes is not sent again, and that call ends in `call.timeout` at its deadline. |
| either keeps none | best effort: a lost request or answer ends in `call.timeout` at the deadline while the caller's Overwatch lives, and a caller off the tier that loses its Overwatch loses its pending calls |

For a durable pair:

- The call output stays held in the caller's outbox until its `Response` is durable in the
  caller's input log, so the caller's recovery sends every call that has no `Response` again.
- The request carries the call id as its message id, so the called relay drops a request sent
  again while the first is retained, and each record carrying a live call is retained until the
  call's deadline plus `call_retention_margin_ms`.
- A recovered call whose `Response` the caller's log holds is settled with nothing sent; one
  whose deadline is ahead is sent again and waits; one past its deadline, within the margin, is
  answered `call.timeout`.
- **The one corner:** a call recovered past its deadline plus the margin (the caller's side down
  that long) is settled with no `Response`, because one may have been appended and truncated
  since, and counted as `[:flezha, :call, :recovered_unanswered]`. There the guarantee is at most
  one `Response`.
- The called relay holds its answer in its own outbox until the caller's Overwatch took it,
  retrying until then or until the deadline plus the margin, when it drops it as `caller_gone`; a
  caller no longer declared in its namespace is dropped at once. An answer whose delivery is
  lost while it holds it (the controller's call delivery crashed) is sent again once from that
  outbox.
- A call to a called relay that has no runner bound yet waits: its input log holds the request,
  the bind delivers it, and the call ends at its answer or its deadline.
- Clocks are assumed synchronized to well within the margin.

The deadline of a relay call is the caller's alone: a wall-clock time taken when the calling
invocation emitted the request. The time a sleeping called relay takes to wake counts against
it. When it passes, the caller receives `call.timeout` and the called turn still runs; its outputs
still reach the called relay's plugins.

## Order and bounds

- **Requests keep their order** from one caller to one target, so a session's turns run in the
  order they were called. `Response`s arrive in the order their appends complete, with no order
  across calls; a caller that needs one restores it from the request each `Response` carries.
- **`call_pending_max` bounds the called relay**: the answers one relay owes the callers
  waiting on it, outside callers and relays alike. A call past it is answered `call.busy` and
  nothing is taken in.
- **A relay's own calls never count against it.** A durable caller's pending calls are
  bounded per cell by its outbox window (`outbox_inflight_max`, 1 000 entries): a cell whose
  window is full of pending calls sends its later outputs only as calls end. That is queueing,
  never loss. A caller off the tier keeps no outbox; its calls are bounded by their deadlines.

| Setting | Default | |
| --- | --- | --- |
| `call_pending_max` (`FLEZHA_CALL_PENDING_MAX`) | 10 000 | the answers one relay owes its waiting callers; past it, `call.busy` |
| `call_grace_ms` | 1 000 | how long an outside caller waits past its deadline, so the relay's own `call.timeout` reaches it first |
| `call_retention_margin_ms` (`FLEZHA_CALL_RETENTION_MARGIN_MS`) | 300 000 | how long an input log keeps a record carrying a relay call past the call's deadline |
| `call_worker_idle_ms` | 30 000 | how long a relay call's delivery worker (one per caller and target) waits with an empty queue before it stops |

All are in [Application env keys](../../configuration/application_env.md#runner-and-relay-lifecycle). A long deadline makes the called relay keep
that much input: the `input_log_frozen_watermark` signal reports a log that keeps growing.

## Observing calls

### Watching a relay's calls

`flz relay listen <relay_id> --calls` prints every call the relay takes, whoever made it, and
how each one ended:

```sh
flz relay listen pricing --calls
# in      {"pair":"EURUSD"}   call=Qm9vVq1x4vHnWQe2Lk3cZg source=cli
# quotes  {"price":1.08}      call=Qm9vVq1x4vHnWQe2Lk3cZg answered ms=14
```

A call prints, separated by tabs, its entry channel (`-` when it names none), its payload and
`call=<id> source=<source>`, with `caller=<relay>` for another relay's call. Its answer prints
the answer's channel, the payload and `call=<id> answered ms=<n>`; a timeout or a refusal prints
the entry channel, the error's message and `call=<id> error=<code> ms=<n>`. With `--json` each
line is an object with `kind` (`call`, `answered`, `timed_out` or `refused`), `call`,
`channel`, `payload` (or `payload_base64`, or `error`) and the event's whole `metadata`.
`--calls` cannot be given beside `--input` (`usage.options_conflict`).

From code, `Relay.listen/5` with the direction `calls` hands the same events to the context's
`:emit`:

| Event | Payload | Metadata |
| --- | --- | --- |
| `{:call, request, md}` | the request, as the entry point took it | `flz.call_id`, `flz.call_source`, `flz.call_caller` (another relay's call), `channel` and `flz.call_answer_channel` when named, `flz.call_at` (the controller's wall clock in ms), `flz.call_deadline_ms` |
| `{:answered, answer, md}` | the answer | `flz.call_id`, `channel` (the answer's), `flz.call_duration_ms` |
| `{:timed_out, error, md}` | `%Flezha.Error{}`, `call.timeout` | `flz.call_id`, `channel` (the entry channel), `flz.call_duration_ms` |
| `{:refused, error, md}` | `%Flezha.Error{}` with the refusal's code | `flz.call_id`, `channel` (the entry channel), `flz.call_duration_ms` |

`flz.call_id` pairs a call with its outcome. It is a digest of the call's token, the same
wherever the relay runs, and the token cannot be read back from it. For every call event a
listener receives exactly one outcome while it stays attached and the relay runs, including
across a move. It may receive an outcome without its call, for a call taken before it attached.
A relay's call that its delivery retries after the callee could not take it shows one pair per
attempt, under one id. A call refused at admission (`call.busy`, `call.unavailable` for a parked
relay, `call.not_accepted`, `relay.factory_mismatch`) is announced with an id of its own and its
`refused` outcome at once.

`flz.call_source` says who called: `cli`, `ui`, `sdk`, `http` (the HTTP API and MCP over HTTP
alike), `mcp` (`flz mcp`) or `internal`, as the caller's context says; `plugin` for a feeding
plugin's call; `relay` for another relay's. Only the controller writes it: a value a caller puts
in the call's metadata is replaced, and the runner never sees it.

A call refused before it reached a relay (`relay.not_found`, `call.target_unkeyed`, a bad
deadline, an `api.*` refusal) is not seen: no relay took it. Payloads are not truncated, and a
listener that falls behind on call events is ended with `listener.overrun`. A relay nobody
listens to for calls builds no event. Watching calls is judged as listening is, by
`relay.listen`.

### Counters

The controller emits `[:flezha, :call, :answered]` with the answer's latency,
`[:flezha, :call, :timeout]`, `[:flezha, :call, :answer_dropped]` with its reason (`unclaimed`,
an outcome no pending call of the caller takes, or `caller_gone`) and `[:flezha, :call, :refused]` with its code, each naming the
namespace and the relay; a relay caller's `timeout` and
`[:flezha, :call, :recovered_unanswered]` also name the call's `target`. The observability source
`flezha_call` exports them as the histogram `flezha_call_answer_duration` and the counters
`flezha_call_timeouts_total`, `flezha_call_answers_dropped_total`, `flezha_call_refused_total`
and `flezha_call_recovered_unanswered_total`. `flz relay info` shows the number of callers
waiting on a relay as `Waiting callers`.

Measured on one machine with debug builds: a call to an awake durable `per_input` steps
relay on the Rust runner took 14 to 32 ms inside the controller, and 29 to 48 ms for the first
call after it slept; through `flz relay call` both are about 300 ms, most of it the CLI's own
start-up. A relay call to a sleeping session, from the input reaching the gateway to the
`Response` reaching it, took 60 to 84 ms on the Rust, Java, Node and Python runners and under
102 ms on the Elixir, Ruby and PHP runners (one sample each), the wake included.

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

- [Running and testing](running_and_testing.md): starting relays and `flz relay info`.
- [Workflows](workflows.md#starting-one-from-a-tasklet): the spawn-or-deliver a factory call
  shares.
- [Sessions](sessions.md): an agent session as a called relay, and its answer streamed.
- [Namespaces](namespaces.md#administering-from-application-code): the `control()` handle and
  the role a namespace needs.
- [Durability and delivery](durability.md), [Effectively once](effectively_once.md): which
  relays keep an input log, and the tier an answer can wait for.
- [Partitioning](partitioning.md): partition groups and their keys.
- [Authorization](authorization.md), [MCP server](mcp_server.md).
- [`relay_commands.md`](../cli/relay_commands.md): `flz relay send`, `call` and their options.
- [HTTP API plugin](../../plugins/http_api.md), [HTTP server plugin](../../plugins/http_server.md).
- [Errors](../../errors.md), [Glossary](https://github.com/flezha/flezha/blob/main/docs/internal/glossary.md): call, answer channel, Response.
- Tutorial: [call the session](tutorials/call_the_session.html).
