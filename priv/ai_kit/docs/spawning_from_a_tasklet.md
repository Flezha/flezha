# Spawning from a tasklet

A running tasklet can start a steps operator with one call: `ctx.spawn_steps`. A stream that
watches prices can start one order workflow per buy; a router can start one triage job per
ticket, or hand a ticket to the job already working on its subject. The tasklet may belong to a
flow or a steps operator; what it starts is always a steps operator, in the tasklet's own
namespace.

The call is spawn-**or**-deliver: given an id that is free, it starts an instance there with the
payload as its first message; given an id that is taken, it delivers the payload to the running
instance as one more message. Without an id, it always starts a new instance under a generated
id.

## How it works

| | |
|---|---|
| Callable from | any tasklet's `run`, in a flow or a steps operator |
| Starts | a **steps operator** only; flow operators are started and kept running by the controller |
| Namespace | the requester's, always; a tasklet cannot reach another namespace |
| Verb | spawn-or-deliver on (namespace, id); with no id, always a spawn under a generated id |
| Blocking | never: the call returns a handle before the controller decides |
| Failure | isolated: a refusal never fails `run`, its emits or its journaling |
| Replay | live processing only; a replayed invocation issues nothing |

The request goes to the controller, which judges and records it the way `flz operator start`
is, so the instance it starts is an ordinary steps operator: namespace quota (`max_operators`,
`max_cells_per_operator`), durable input log, sleeping and waking, its end and teardown,
retention, and a row in `flz operator list`. A delivery is judged as `flz operator send` is,
and recorded only when refused, because it happens once per event.

The payload is the new instance's first message by construction: it is taken in as the instance
binds, ahead of anything else in its mailbox, through the same entrance every message takes. It
names no channel, so it enters the first declared entry channel, is decoded by that channel's
codec or parser, and runs the chain that channel starts, whichever way the operator loops. Every
SDK encodes it the way a sink with no codec encodes a value, the rule `ctx.complete` uses:
bytes as they are, a string as UTF-8, anything else as JSON, so a `text` or `json` entry reads
it. A durable instance whose input log already holds records is a restart, not a spawn, and is
not given the payload again.

The factory is looked up cluster-wide: a controller with no runner for it routes the request to
one that has one, so `operator.factory_unknown` means no runner anywhere serves it.

## How to use it

The factory and the payload come first; the id and the policy are optional:

| SDK | Spawn | With an id and a policy |
| --- | --- | --- |
| Rust | `ctx.spawn_steps("triage", bytes)` | `ctx.spawn_steps_with("triage", bytes, SpawnOptions::id(id).if_running(IfRunning::Deliver))` |
| Java | `ctx.spawnSteps("triage", input)` | `ctx.spawnSteps("triage", input, SpawnOptions.id(id).ifRunning(IfRunning.DELIVER))` |
| Node | `ctx.spawnSteps('triage', payload)` | `ctx.spawnSteps('triage', payload, { id, ifRunning: 'deliver' })` |
| Python | `ctx.spawn_steps("triage", payload)` | `ctx.spawn_steps("triage", payload, id=id, if_running="deliver")` |
| Ruby | `ctx.spawn_steps("triage", payload)` | `ctx.spawn_steps("triage", payload, id: id, if_running: "deliver")` |
| PHP | `$ctx->spawnSteps('triage', $payload)` | `$ctx->spawnSteps('triage', $payload, id: $id, ifRunning: 'deliver')` |
| Elixir | `spawn_steps(ctx, "triage", input)` | `spawn_steps(ctx, "triage", input, id: id, if_running: :deliver)` |

Rust and Java build the options only from an id, so a policy without an id cannot be written
there; the other five SDKs raise an argument error at the call and send nothing.

What the call does:

| The id is | policy `deliver` | policy `error` |
|---|---|---|
| not given | spawned under a generated id | (no policy without an id) |
| free | spawned; the payload is the first message | spawned |
| taken | delivered; the payload is one more message | refused, `operator.already_running` |

A generated id is the factory id, shortened to fit the 32-byte limit, a `-` and 16 characters of
time and randomness, such as `triage-k7m2qzxw4pd3fn2a`.

Concurrent requests for one id resolve to **exactly one** spawn, and deliveries for the rest. A
completed instance's id counts as free again, so a request arriving after the instance finished
starts a fresh one: make the started operator safe to run twice for the same work.

### Permission

With authorization on, the runner's connection acts as `runner@<namespace>` and needs a role
that grants the start. Bind it to `spawner`:

```sh
$ flz role bind runner@trading spawner trading
```

```output
bound runner@trading as spawner@trading
runner@trading  spawner@trading
```

`spawner` also lets that connection call `Operator.start` and `Operator.send` on the
administration surface, and nothing else (see [authorization](../basics/authorization.md)).

### Reading the result

Every failure resolves the handle, never `run`. Reading the handle is optional; each SDK has a
read that waits and one that never waits:

| SDK | Waits | Never waits |
|---|---|---|
| Rust | `wait()` | `try_result()` |
| Java | `await()` | `tryResult()` |
| Node | `await wait()` | `tryResult()` |
| Python, Ruby | `wait` | `try_result` |
| PHP | `wait()` | `tryResult()` |
| Elixir | `Flezha.SDK.Spawn.await/2` | `Flezha.SDK.Spawn.try_result/1` |

A resolved read carries the id the request resolved to (given or generated) and the outcome,
`spawned` or `delivered`. A failure comes out of the read in the form that SDK reports errors (a
returned error, a thrown exception, a rejected promise), never out of the spawn call. The read
that never waits answers that the result is pending until it has arrived.

| Code | Meaning |
|---|---|
| `operator.already_running` | the id is taken and the policy is `error` |
| `operator.factory_unknown` | no connected runner on any controller registers that factory |
| `operator.factory_not_steps` | the factory's runners announced it as a flow operator; decided before anything is placed |
| `authz.role_insufficient` and other `authz.*` codes | authorization is on and `runner@<namespace>` holds no role that grants the spawn |
| `quota.exceeded`, and the controller's other codes | quota used up, controller shutting down, start failed |
| `spawn.timeout` | no answer within 30 seconds |
| `spawn.unavailable` | no control plane to ask: the runner is closed or has no controller |

Read a handle inside the invocation that made it, or not at all. A handle nobody read by the end
of its invocation reports a failure once, as a warning line naming the instance id (empty when
none was given), the factory and the reason, and a later read answers that it was released. In
Elixir the answer is a message to the process that made the request, so an answer arriving
after `run/3` returned is dropped, and the warning is what reaches you.

## Example

In the tradedesk example, `buy_above_average` starts one `order_executor` workflow per buy. The
order is the workflow's first message. It passes no id, so each buy starts a new instance:

<!-- tabs-open -->

### Java

```java
// BuyAboveAverage.java: buys when a price is far enough above its average.
        int quantity = (int) Math.round(percent * UNITS_PER_PERCENT);   // a stronger move buys more

        ctx.spawnSteps("order_executor", new OrderTicket(move.symbol(), quantity));   // one steps operator per order
```

[Full file](https://github.com/flezha/flezha_java_sdk/blob/main/examples/tradedesk/src/main/java/com/flezha/examples/tradedesk/BuyAboveAverage.java)

### Rust

```rust
// buy_above_average.rs: buys when a price is far enough above its average.
        let ticket = OrderTicket {
            symbol: m.symbol.clone(),
            quantity,
        };
        let order = serde_json::to_vec(&ticket).map_err(|e| TaskletError::Custom(e.to_string()))?;
        let _ = ctx.spawn_steps("order_executor", order); // one steps operator per order
```

[Full file](https://github.com/flezha/flezha_rust_sdk/blob/main/examples/tradedesk/src/buy_above_average.rs)

### Node

```js
// buy_above_average.mjs: buys when a price is far enough above its average.
    const order = { symbol: move.symbol, quantity };
    ctx.spawnSteps('order_executor', order); // one steps operator per order
```

[Full file](https://github.com/flezha/flezha_node_sdk/blob/main/examples/tradedesk/buy_above_average.mjs)

### Python

```python
# buy_above_average.py: buys when a price is far enough above its average.
        order = OrderTicket(symbol=move["symbol"], quantity=quantity)
        ctx.spawn_steps("order_executor", order)  # one steps operator per order
```

[Full file](https://github.com/flezha/flezha_python_sdk/blob/main/examples/tradedesk/buy_above_average.py)

### Ruby

```ruby
# buy_above_average.rb: buys when a price is far enough above its average.
    order = { symbol: move[:symbol], quantity: quantity }
    ctx.spawn_steps("order_executor", order)               # one steps operator per order
```

[Full file](https://github.com/flezha/flezha_ruby_sdk/blob/main/examples/tradedesk/buy_above_average.rb)

### PHP

```php
// BuyAboveAverage.php: buys when a price is far enough above its average.
        $order = ['symbol' => $input['symbol'], 'quantity' => $quantity];
        $ctx->spawnSteps('order_executor', $order);        // one steps operator per order
```

[Full file](https://github.com/flezha/flezha_php_sdk/blob/main/examples/tradedesk/src/BuyAboveAverage.php)

### Elixir

```elixir
# lib/tradedesk/buy_above_average.ex: buys when a price is far enough above its average.
      # One steps operator per order.
      spawn_steps(ctx, "order_executor", %{symbol: symbol, quantity: quantity})
```

[Full file](https://github.com/flezha/flezha_elixir_sdk/blob/main/examples/tradedesk/lib/tradedesk/buy_above_average.ex)

<!-- tabs-close -->

The order executor reads the order on its entry channel `order`, the first one it declares. The
runner's connection needs the `spawner` role in `trading`, as shown above.

## Replays and crash re-runs

**A replay issues nothing.** A replayed invocation (a late-input correction re-running a slice of
history) answers the outcome `skipped_on_replay` without asking the controller, so a replay
never doubles the instances a run started. Its id is the one the call gave, or empty when it gave
none: a generated id existed only in the controller's answer to the live run.

**A crash re-run asks again and gets the same answer.** A crash re-run is not a replay: an input
the runner took but had not durably consumed is sent again and runs live, so its spawn is
requested again. Each request a tasklet makes carries an identity: the requesting cell, the
input-log offset it was taking, the call's position in the invocation, the factory and the
payload. The controller answers a request whose identity it has already resolved with the
instance the first request resolved to:

- **Without an id**, the generated id is derived from the identity, so the re-run resolves to the
  instance the first run started (`spawned`, whether it still runs or has completed).
- **With an id**, a delivery into the running instance carries the identity as its message id,
  so the instance's input log drops the second delivery and the instance receives the payload
  once.

A re-run that asks for a different factory or payload at the same position is a new request and
spawns anew. An invocation that took no input offset (a timer firing, or an input to an operator
with no durable input log) carries no identity, and neither does a start from the CLI, the
administration API or a schedule.

## Dangers of a spawn without an id

- **It always starts a new instance.** It can never deliver into a running one, because there is
  no id to find it by.
- **Asked again in the same invocation after a lost or timed-out answer, it starts a second
  instance.** The second request is a later call, so its identity differs from the first's.
- **Skipped on replay, it answers an empty id.** The id was generated during the live run.

A caller that needs at most one instance, or the id on replay, passes its own id.

## Limits

- **A faulty tasklet can spawn in a loop.** The namespace quota is the backstop; there is no
  per-requester rate limit. The telemetry event `[:flezha, :operator, :requested_spawn]`
  carries `namespace`, `instance_id` (the id the request resolved to), `factory_id` and
  `outcome` for each request, for rate alerts.
- **A runner cut off from its controller fails the request later**, like any control-plane
  request (`spawn.timeout` or `spawn.unavailable`).
- **Reusing an id right after its instance completed starts a fresh instance** instead of
  delivering.
- There is no separate registry and no spawn journal; scheduling is `flz schedule`'s job, and a
  spawn never crosses namespaces.

## See also

- [Workflows](../basics/workflows.md): the steps operator a spawn starts
- [Running operators](../basics/running_operators.md): `flz operator start` and the same retry
  danger for a start without an id
- [Authorization](../basics/authorization.md): roles, and `spawner`
- [Operator calls](operator_calls.md): a call to a factory uses the same spawn-or-deliver
- [`operator_forward`](../../plugins/operator_forward.md): feeding an existing operator as
  configuration
- [Errors](../../errors.md)
- Tutorial: [place orders](../../tutorials/tradedesk/place_orders.md)
