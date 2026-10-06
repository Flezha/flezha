# Sending from runner code

Application code in a runner process often holds a source of its own: a Kafka
consumer, a queue client, a poll loop. `runner.send` hands each message it reads
to a relay in the runner's namespace. It goes through the controller by
default, under the same role check as the [`control()`](../basics/namespaces.md#administering-from-application-code)
handle, and answers the controller's verdict. Two options make a source that reads
a record again safe to repeat: a message id and a partition key. A third, `local`,
skips the controller for input you can afford to lose.

**Contents**

1. [Send through the controller](#send-through-the-controller)
2. [Call a channel and wait for its answer](#call-a-channel-and-wait-for-its-answer)
3. [From a Kafka consumer](#from-a-kafka-consumer)
4. [The relays bound on this runner](#the-relays-bound-on-this-runner)
5. [Send locally](#send-locally)
6. [Refusals](#refusals)
7. [See also](#see-also)

## Send through the controller

A send names the relay, the message and the entry channel, and blocks for the
answer: the relay that took the message and its verdict.

| Verdict | Means |
| --- | --- |
| `delivered` | the relay took the message |
| `absorbed` | a repeat of a message id the relay's input log still holds; the message is not handled again |
| `suspended` | the relay is asleep or moving; its input log holds the message until it wakes |

Two options ride along:

- **A message id**, stamped on the input as `flz.msg_id`. A relay with an input
  log (the [effectively-once tier](effectively_once.md), a durable steps relay, a
  class that sleeps) absorbs a repeat of an id it still holds. A relay without
  one handles every send. [`durability.md`](../basics/durability.md#delivery) says
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

## Call a channel and wait for its answer

`runner.call` sits beside `send`: it calls an entry channel of a relay with a
value, by the relay's API ([`../basics/relay_api.md`](../basics/relay_api.md)),
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

## From a Kafka consumer

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

## The relays bound on this runner

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

## Send locally

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

## Refusals

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

## See also

- [`namespaces.md`](../basics/namespaces.md#administering-from-application-code): the
  `control()` handle and the role a namespace needs.
- [`durability.md`](../basics/durability.md): which relays keep an input log, and
  what each level promises.
- [`effectively_once.md`](effectively_once.md): the tier whose input log absorbs a
  repeated message id.
- [`partitioning.md`](partitioning.md): partition groups and their keys.
