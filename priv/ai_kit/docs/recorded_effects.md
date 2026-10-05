# Recorded effects

A tasklet that calls something outside its relay (a model, a tool, a payment
service) runs that call again whenever the invocation runs again: after a
crash, on a replay, when a sleeping instance wakes or a relay moves to another
runner. `ctx.effect` runs the call once and keeps its result. When the same
invocation runs again, it returns the kept result and does not call out.

**Contents**

1. [How it works](#how-it-works)
2. [Call it](#call-it)
3. [Which call gets which result](#which-call-gets-which-result)
4. [At least once at the edge](#at-least-once-at-the-edge)
5. [One call at a time](#one-call-at-a-time)
6. [Where nothing is kept](#where-nothing-is-kept)
7. [Refusals](#refusals)
8. [Limits](#limits)
9. [See also](#see-also)

## How it works

`ctx.effect(name, fn)` asks the runner for a result kept under this call's
identity. If there is one, the call returns it and `fn` does not run. If there
is none, `fn` runs, and its result goes to the controller, which writes it to
the instance's effect log beside its input log and syncs it. Only then does
the call return.

A kept result lives exactly as long as the input it belongs to: when the input
log drops an input below the relay's floor, the results of that input go with
it. The effect log is shipped to the durability backend with the input log,
so a controller that replaces a lost disk still serves them. When an instance
is bound to a runner, the controller sends its kept results first, before
anything runs again.

The result is encoded the way an output with no codec encodes a value: bytes
as they are, a string as UTF-8, anything else as JSON. In Rust and Java it is
the typed value's JSON. Pass a codec to keep a typed result in another form.

## Call it

<!-- tabs-open -->

### Java

```java
// Quote.java, in run(): ask the pricing model once per input
String quote = ctx.effect("model", String.class, () -> pricing.quote(pair));
ctx.emit(quote);
```

### Rust

```rust
// quote.rs, in run(): ask the pricing model once per input
let quote: String = ctx.effect("model", || pricing.quote(&pair))?;
ctx.emit(quote)?;
```

### Node

```js
// quote.mjs, in run(): ask the pricing model once per input
const quote = await ctx.effect('model', async () => pricing.quote(pair));
ctx.emit(quote);
```

### Python

```python
# quote.py, in run(): ask the pricing model once per input
quote = ctx.effect("model", lambda: pricing.quote(pair))
ctx.emit(quote)
```

### Ruby

```ruby
# quote.rb, in run: ask the pricing model once per input
quote = ctx.effect("model") { pricing.quote(pair) }
ctx.emit(quote)
```

### PHP

```php
// Quote.php, in run(): ask the pricing model once per input
$quote = $ctx->effect('model', fn () => $this->pricing->quote($pair));
$ctx->emit($quote);
```

### Elixir

```elixir
# quote.ex, in run/3: ask the pricing model once per input
with {:ok, quote} <- effect(ctx, "model", fn -> Pricing.quote(pair) end) do
  emit(ctx, quote)
  {:ok, state}
end
```

<!-- tabs-close -->

In Python an `async def` function makes the call awaitable
(`await ctx.effect("model", ask)`), and in Node the call is always awaited.
An error `fn` raises reaches the tasklet unchanged, and nothing is kept, so the
next attempt calls out again.

## Which call gets which result

A result is named by where it came from: the Cell, the input the invocation
was taking, whether it ran in `run` or in `compensate`, the name, and how many
calls of that name the invocation made before it. Two consequences:

- Adding a call with another name before an existing one does not move the
  existing one's result. Name each call after what it reaches (`"model"`,
  `"search"`), not after its position.
- Calling one name twice in an invocation keeps two results, and a re-run gets
  them back in the same order.

A name is a non-empty string of at most 255 bytes.

## At least once at the edge

The result is kept when the controller has it on disk. If the runner dies
after `fn` returned and before that, the re-run calls `fn` again: no runtime
can tell whether a call that never answered took effect. Where the outside
service accepts an idempotency key, pass it `ctx.stable_id()`, which a re-run
of the same invocation derives again, so the service can drop the repeat.

A result over `FLEZHA_EFFECT_MAX_BYTES` (4 MiB unless set) is refused rather
than kept. Keep a reference to a large result (a URL, an object key) instead.

## One call at a time

An invocation makes one `ctx.effect` call at a time. A call started while
another of the same invocation is still running (from `fn` itself, a thread,
`Promise.all` or `asyncio.gather`) is refused with `effect.nested`. Make the
calls one after the other, or wrap the concurrent work in one call:

```python
# both searches run together, and their pair is kept as one result
pair = ctx.effect("search", lambda: search_both("EURUSD", "GBPUSD"))
```

## Where nothing is kept

An invocation that took no input-log offset can never run again with the same
input, so `ctx.effect` just runs `fn` and sends nothing:

- local mode,
- a flow relay at the default tier that does not sleep, and a relay with
  `durable: false`,
- an input sent with `runner.send`'s `local` option
  ([`sending_from_runner_code.md`](sending_from_runner_code.md)),
- in a steps relay, a pass a timer fired and the later passes of a chained
  loop, which take no input of their own: a re-run of one of them calls out
  again.

The same tasklet code therefore works everywhere, and keeps results where a
re-run is possible: the [effectively-once tier](effectively_once.md), a relay
that sleeps, and a durable steps relay's message-fed passes.

## Refusals

| Code | When |
| --- | --- |
| `effect.bad_name` | the name is empty or longer than 255 bytes |
| `effect.nested` | another call of the invocation is still running, `fn` itself included |
| `effect.outside_invocation` | the context is used after its invocation returned |
| `effect.too_large` | the encoded result is over `FLEZHA_EFFECT_MAX_BYTES` |
| `effect.not_recorded` | the controller did not confirm it kept the result in time |

`effect.not_recorded` fails the attempt so the invocation runs again; the
others are not retried. [`errors.md`](../../errors.md) has each message.

## Limits

- **Results do not cross inputs.** A result belongs to one input's
  invocation. Two inputs that ask the same question call out twice.
- **The kept result is returned as it reads back.** With no codec, a string
  that is valid JSON reads back as that JSON value, on the first run and on a
  re-run alike. Pass a codec when the type matters.
- **Order matters within a name.** A re-run that calls one name in a
  different order gets the results in the old order.
- **Each SDK's test harness checks it, but Elixir's, which has no replay
  pass.** The harness keeps each result its live pass produced and serves it
  on the replay pass. A function that runs on
  the replay pass, or a kept result the replay never asks for, fails the check
  and names the effect.

## See also

- [effectively_once.md](effectively_once.md): which relays keep an input log.
- [durability.md](../basics/durability.md): what a crash can lose at each durability level.
- [sending_from_runner_code.md](sending_from_runner_code.md): local input, which keeps nothing.
- [errors.md](../../errors.md): the `effect.*` codes.
