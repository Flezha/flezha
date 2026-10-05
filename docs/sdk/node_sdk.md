# Node SDK

`@flezha/sdk` is a poll host over `flz_core`, the runner kernel the Rust and
Java SDKs also drive. The kernel owns transport, ordering, per-cell
exclusivity, durability, retries, timers, credit, routing and the steps relay
driver. This package supplies threads and user code, and nothing else —
[`poll_core.md` → "The core decides, the SDK brings the threads"](poll_core.md#the-core-decides-the-sdk-brings-the-threads).

**Support tier:** [tier 2](../publishing/maturity.md#sdk-node), passes the full
conformance run in CI exactly as tier 1 does, with every capability present, and is
permanently supported; it is not yet run by the maintainer in a live deployment, so a
defect specific to it is likely to be found by its users first.

- **Package**: `@flezha/sdk`, TypeScript-first and usable from plain JavaScript
  with no build step.
- **Source**: `sdk/node/`.
- **Scope**: `test/apps/node_runner/conformance.json` is the scope matrix — the
  capabilities this SDK carries and the ones it defers, checked against the
  Rust reference by `test/sdk_conformance/gate_test.exs`. This page does not
  restate it, because a restatement drifts.

## Supported Node versions

Node **20 or newer**. The binding targets N-API version 9, which every release
in that window speaks. N-API is ABI-stable across Node versions, so one binary
per platform serves every supported release — upgrading Node needs no rebuild.

Both module systems are supported: `import` from an ECMAScript module and
`require` from CommonJS resolve to the same surface.

## Installing

```bash
npm install @flezha/sdk
```

The native binary ships as a per-platform package resolved through
`optionalDependencies` — `@flezha/sdk-linux-x64-gnu`, `-linux-arm64-gnu`,
`-darwin-x64`, `-darwin-arm64`, `-win32-x64-msvc`. One is installed, not five,
and no Rust toolchain is needed. The five are published from one table,
`sdk/node/src/platforms.ts`, which is also the table the loader resolves
against, and the release refuses to pack the parent unless all five were built.

The published package carries no binary of its own, so a platform with no
prebuilt package is a source build. It needs a stable Rust toolchain and the
package itself, not a checkout of this repository: `native/` is a wrapper crate
whose one dependency is the core at a git tag.

```bash
cargo build --manifest-path native/Cargo.toml --release
node scripts/install-native.mjs
```

`FLEZHA_NATIVE_BINARY` points the loader at an already-built addon from
anywhere, which is what this repository's own suites use and what an installed
package on an unsupported platform needs.

npm treats an optional dependency it cannot resolve as a non-event: the install
succeeds and says nothing. An installation that resolved no platform package
therefore looks healthy until the core is first loaded, where the SDK refuses
naming the host it detected, the package it looked for and the build above.

## Writing a relay

```ts
import { createRunner, defineRelay, defineTasklet, type Ctx } from '@flezha/sdk';

interface Trade { symbol: string; price: number; at: number }
interface Candle { symbol: string; close: number }
interface Window { high: number; low: number; close: number }
interface Close { close: number }

const Ohlc = defineTasklet({
  name: 'ohlc',
  idleTtlSeconds: 60,
  initState: () => ({ high: 0, low: 0, close: 0 }),
  applyChange: (state, change: Close) => {
    state.close = change.close;
    state.high = Math.max(state.high, change.close);
    state.low = state.low === 0 ? change.close : Math.min(state.low, change.close);
  },
  run: (trade: Trade, ctx: Ctx<Candle, Window, Close>) => {
    ctx.change({ close: trade.price });
    ctx.emit({ symbol: trade.symbol, close: ctx.state.close });
  },
});

export default defineRelay({
  factoryId: 'candles',
  channels: (c) => {
    c.input<Trade>('trades').keyBy((trade) => trade.symbol).into(Ohlc).keyed().sink('candles');
  },
});
```

```ts
const runner = createRunner('default');
await runner.registerFactory('./candles.js');
await runner.run();
```

**A value leaves a tasklet only through its context.** `ctx.emit(value)`
sends one value on the tasklet's output, with `{ key, channel, metadata }` as
options, and `ctx.emitTo(channel, value)` names one of several. `run`,
`onTimer` and `compensate` are typed to return `void | Promise<void>`, so
TypeScript refuses one that returns a value. Plain JavaScript runs no type
checker, and the SDK ignores what a hook returns: a `.mjs` tasklet whose last
expression was its output emits nothing.

**`ctx.part(value)` sends a preview of the output while `run` is still
working.** It previews the tasklet's own output channel, is encoded as
`ctx.emit(value)` would encode it there, and never waits: a part that cannot
leave at once is dropped. A listener receives it, numbered from 0 within each
attempt; a sink never does, and nothing about it is durable. It works in `run`
and `onTimer`, for a flow tasklet and a steps stage alike. It throws a
`FlezhaError` with code `part.no_sink` when the output channel does not end in
a sink (a call end is not one), and `part.outside_invocation` through a
context kept past its invocation.

**The output type is declared, never read off the body.** Name it with the
type arguments, `defineTasklet<Trade, Candle>({ ... })`, or with the
annotation on the context, as `Ohlc` does. TypeScript infers no type argument
once one is written, so a stateful tasklet that uses type arguments writes all
four (input, output, state, change); `ctx: Ctx<Candle, Window, Close>` names
the last three in one place. A tasklet that names no output type has none, so
an `emit` on it does not compile.

**A tasklet's settings are fields of its definition.**

- `initState(cellKey)` is the cell's starting state, and its absence declares a
  stateless tasklet. `applyChange(state, change)` folds one recorded change,
  live and on restore; a change is plain data, any value MessagePack carries
  (`stateCodec` and `changeCodec` take a codec of your own). `applyChange`
  without `initState` is refused.
- The cell mode is read from the wiring. A tasklet reached through `keyBy`,
  on a stream or on a merge, gets a cell per key; one reached by no `keyBy`
  has one cell; `cells: { keys, cellsFor }` declares a fixed key set, and
  `keyBy` into one is refused as `topology.key_by_on_unkeyable_target`. The mode belongs to
  the node, so one definition can be keyed in one relay and single in
  another.
- `idleTtlSeconds` evicts a keyed cell idle that long, a positive whole number
  of seconds; unset never evicts. `maxCells` caps the keyed cells held at once.
- The clock follows from what the definition writes: `eventTime(input)` puts it
  on event time, `logicalClock: true` on the logical clock, and anything else on
  the wall clock. Writing both is refused. A logical clock needs `applyChange`,
  and without it the core refuses the relay as `topology.unservable_clock`.
- A field `defineTasklet` does not read is refused while the module loads,
  naming it, whether the definition carries it or inherits it. A field an
  earlier release read is refused naming what replaced it.

**A wrong edge is a compile error.** `.into(tasklet)` accepts only a
tasklet whose declared input the producing edge's value satisfies, so a
mismatched wiring is rejected before the program runs. Those refusals are
pinned by fixtures under `sdk/node/types-tests/`, which fail the build if a
refusal stops working. Everything static typing cannot see — cycles, missing
entry channels, unservable clocks, keyed-channel rules, branch arity, duplicate
sinks — is refused at registration, naming the handler and the defect.

**The class settings are one tree.** `defineRelay` takes them beside
`factoryId`, in either form:

```javascript
export default defineRelay({
  factoryId: 'orders',
  steps: { each: 'per_input', onFailure: 'keep_progress' },
  durable: { local: { sleepAfter: 10_000 } },
  maxDurationMs: 600_000,
  channels: (c) => c.input('order', { codec: 'text' }).into(Place).sink('placed'),
});
```

- A relay runs as a flow unless it writes `steps`. The run is written as
  its branch, `flow: {...}` or `steps: {...}`, never both, because a short
  form's `run` is its tasklet body. `steps` takes `each` (`'once'`, the
  default, `'per_input'` or `'chained'`) and `onFailure` (`'undo_all'`, the
  default, or `'keep_progress'`).
- A flow takes `exactly`: `'at_least_once'` (the default),
  `'effectively_once'`, or `{ effectively_once: { ack, correctionMs, beyondWindow } }`,
  with `ack` either `'per_input'` or `'checkpoint'`, or
  `{ checkpoint: { every, maxDelayMs, outputs } }`.
- `durable` is `false`, `'local'` (the default) or `'remote'`, and
  `{ local: { sleepAfter } }` or `{ remote: { sleepAfter } }` with milliseconds
  or `false`. A kept instance sleeps after six idle hours by default.
- `maxDurationMs` stops an instance that long after it starts; `description`
  is free text.

The tree reaches the core as its JSON, members spelled as the core reads
them (`sleepAfter` is `sleep_after`), and the core judges it when the class
registers: a setting under a parent that does not carry it is refused as
`topology.unknown_field`, naming the setting and the parent, and a value
outside its choice as `topology.invalid_setting`.

**Retry belongs to the edge.** `retryMax` and `retryBackoffMs`
on the channel that feeds a tasklet (3 attempts from a 50 ms base by default),
and `retryMax(1)` fails fast.

**A steps tasklet's timers work as a flow tasklet's.** A firing runs as a
pass of its own ([`../topics/advanced/timers.md`](../topics/advanced/timers.md)); only
inside a `compensate` hook do `timerAfter`, `timerAtEventTime` and
`cancelTimer` throw an error beginning `runtime.timers_unavailable`.

**State is mutated only through declared changes.** Assigning to a field of
`ctx.state` throws, naming the field and the change-based path: declared state
is rebuilt from its recorded changes on restart, so a direct assignment is lost
there and reported nowhere.

**Handlers are registered by module specifier**, never as imported values, in
every dispatch configuration. A dispatch loop imports the module itself, which
is what makes enabling worker threads a flag rather than a rewrite.

### The short form

A relay of one tasklet can be one declaration: `defineRelay` carrying
that tasklet's `run` body in place of `channels`. `factoryId` is required, `sink` is optional,
`input` names the entry channel, and `codec` is the entry codec `c.input`
takes. The tasklet's own fields (`initState`, `applyChange`, `cells`,
`eventTime`, `mailboxCapacity`, `schemaVersion` and the rest `defineTasklet`
reads) are written beside `run`, and `run` is a tasklet's: it emits through
`ctx`, and moving its fields into a `defineTasklet` call grows it into the full
form unchanged.

```javascript
// upcase.mjs
export const upcase = defineRelay({
  factoryId: 'upcase',
  sink: 'out',
  codec: 'text',
  run: (value, ctx) => ctx.emit(value.toUpperCase()),
});

// place.mjs
export const place = defineRelay({
  factoryId: 'place',
  steps: {},
  sink: 'placed',
  input: 'order',
  codec: 'text',
  run: (order, ctx) => ctx.emit(order),
});
```

```ts
const runner = createRunner('default');
await runner.registerFactory('./upcase.mjs');
await runner.registerFactory('./place.mjs');
await runner.run();
```

The generated relay is the one you would have written by hand with that
single tasklet: an entry channel named `input`, or the `input` field's value,
decoded by `codec`; the tasklet, named `factoryId` verbatim, so its cells
keep their state path when it grows into the full form; a plugin sink named by
`sink`; and the class settings it writes, as the full form takes them. A steps
short form is a `once` steps relay unless it writes `each`, run once over its first
message: `flz relay start --text '<payload>'`, or a start followed by
`flz relay send`; a second message to a `once` instance is accepted and
never read. With `each: 'per_input'` it runs once per message and does
not complete after the first:

```javascript
export const chat = defineRelay({
  factoryId: 'chat',
  sink: 'replies',
  codec: 'text',
  steps: { each: 'per_input' },
  run: (turn, ctx) => ctx.emit(turn),
});
```

With no `sink` there is no plugin channel, so the tasklet has no output
channel either: its `run` keeps state and emits nothing.

```javascript
export const counter = defineRelay({
  factoryId: 'counter',
  input: 'ticks',
  codec: 'text',
  initState: () => ({ seen: [] }),
  applyChange: (state, change) => {
    state.seen.push(change);
  },
  run: (tick, ctx) => {
    ctx.change(tick);
  },
});
```

Anything more is the full form: a second tasklet, a fan-out, a merge, a keyed
edge, or an edge's retry ladder.

The spec is built where `defineRelay` runs, so a
malformed short form is refused while the module loads rather than inside
`run()`. Two refusals name themselves: `topology.short_form_with_topology` (`run` beside
`channels`), and `topology.duplicate_factory_id`, raised when the runner declares a second
relay under a factory id it holds, whatever either runs. A key written where it
does not belong is refused naming the key: `name` beside `run`, `sink`, `input`,
`codec` or a tasklet field beside `channels`, and `flow`, `steps`, `durable`,
`sink`, `input` or `codec` on a `defineTasklet`. There is no `topology.sink_without_output`: a plain `.mjs` definition
declares no output type the SDK can read, a TypeScript one names it only in
types erased before the SDK runs, and no `run` returns output, so nothing the
SDK can see says whether a short form emits. The manifest leaves
`shorthand_typed_output` off and the conformance row skips by name.

## The parallelism model, as arithmetic

| Number | What sets it | What it bounds |
| --- | --- | --- |
| **1024** | `FLEZHA_MAX_INFLIGHT_ACTIVATIONS`, enforced by the core at handout | Concurrent activations. Not the fetcher count, not the loop count. |
| **2** | fetch threads, `.fetchers(n)` | How many native threads may sit inside `poll` at once. A fetcher never runs user code, so two cover the hand-off latency of one being between polls. Raising it does not raise concurrency. |
| **0** | dispatch loops, `.workers(n)` / `FLEZHA_NODE_WORKERS` | **Compute parallelism.** `0` is the default and means the main event loop: compute parallelism of **one**. `n > 0` means `n` worker threads and compute parallelism `n`. |
| **2**, growing to **20** | `FLEZHA_WORKER_POOL_SIZE`, `FLEZHA_WORKER_POOL_MAX` (default 10× the base) | Native threads for blocking core calls, above all `complete`. It grows on demand and threads above the base retire after a 30-second idle streak. |

Concurrency is bounded by the core's in-flight cap, never by the fetcher or
loop count. I/O-bound handlers reach that cap on one loop. Compute-bound
handlers do not, because they hold the loop while they compute — which is what
the loop count is for.

**250 ms** is the poll timeout, and therefore the bound on how quickly a
shutdown request is observed.

## Which dispatch configuration

**The default is the main loop.** Node's dominant workload is I/O-bound, where
one loop already reaches the core's ceiling, and worker mode taxes that case
with worse debugging, per-worker module state and N× isolate memory.

**Worker mode is the recommended production posture**, for two things the main
loop cannot do:

- **CPU-bound handlers.** A synchronous compute loop blocks its event loop and
  there is no other fix. The SDK observes its own loops and warns, naming the
  tasklet and the observed delay, when a loop is held past 500 ms.
- **Replacement.** A wedged worker can be killed and restarted, and the
  configured loop count restored. A wedged main thread cannot: the runner keeps
  heartbeating, `inflight_oldest_ms` climbs, and the controller's classifier
  mints `cell_dispatch_stalled` and evicts the runner.

Both configurations run the same handler modules, the same dispatch path and
the same registration form. Selecting the loop is the only difference.

### Module-scope state is per dispatch loop

```js
const redis = new Redis();   // .workers(0): one client
                             // .workers(4): four clients, silently
```

Usually benign — a connection pool, a cache. Occasionally wrong — an in-memory
counter, a run-once flag, an `AsyncLocalStorage` expected to span activations.
Durable state is unaffected: the core owns it, through the declared change
fold.

An activation for a given cell always reaches the same loop. That is not a
performance choice: after a restore fold, the loop that folded holds that
cell's materialized state, and a loop is a separate isolate. Work that names
no cell — the prepare crossing, which runs before any key exists —
round-robins instead.

## What must never block the event loop

`flz_core_complete` can park. Under the default `DropNothing` overflow policy a
completion's routing push waits until the consumer's mailbox drains, and the
consumer drains only when its handler runs. If the parked thread were the event
loop, the loop would be the only thread that could run that handler, and
nothing could free it — a wedged **process**, not a wedged worker.

So no blocking core call is reachable synchronously from JavaScript. `poll` is
not exported to JavaScript at all; it is driven from the SDK's own native
threads, never the runtime's shared I/O threadpool. `complete`, `spawnWait`
and `start` return promises whose blocking work
runs on native threads the binding owns; `spawnRequest` and `spawnTryResult`
never wait, so `ctx.spawnSteps` returns its handle synchronously and only
the handle's `wait()` is a promise. A test over the addon's exported
surface asserts this rather than leaving it to review.

## Determinism

`ctx.now()` and `ctx.stableId()` are the replay-stable reads. The engine
replays — a crash-resume, a retry and a correction all re-enter a handler
expecting the same effects — so a handler that reads `Date.now()`,
`new Date()`, `Math.random()` or `crypto.randomUUID()` inside an invocation is
warned about by file and line, once per site. The warning never aborts the
invocation.

## Channel payloads

A channel names the format of its payload with `{ codec }` on `input` and on
`sink`, typed as the `ChannelCodec` union:

| Codec | An entry hands the tasklet | A sink sends |
| --- | --- | --- |
| `'text'` | a `string` (`Stream<string>`); invalid UTF-8 fails the decode | a `string` as UTF-8; any other value fails the emit |
| `'bytes'` | a `Uint8Array` (`Stream<Uint8Array>`) | a `Uint8Array` or `Buffer` unchanged; any other value fails the emit |
| `'json'` | the parsed JSON | any value as JSON, a string quoted |
| `'msgpack'` | the value MessagePack decodes to | any value as MessagePack |

```ts
c.input('in', { codec: 'text' }).into(Echo).sink('out');
```

An entry with no codec and no decoder reads JSON: `{"a":1}` arrives as
`{ a: 1 }`, and plain text fails its decode and rides the retry and dead-letter
ladder. A sink with no codec and no encoder follows the value: a `Uint8Array`
passes through, a string is sent as UTF-8, and anything else as JSON. So a sink
needs no codec for a string, bytes or a JSON value, and names one only to pick
another format, `'json'` to quote a string or `'msgpack'`. A decoder or encoder
function takes the place of a codec for anything else. A codec name outside the
four does not type-check, and from JavaScript it is refused when the relay
is declared, as is a codec given beside a function on one channel, naming the
relay and the channel.

A called input's first sink output answers its caller and still reaches the sink's plugins:
[`../topics/advanced/relay_calls.md`](../topics/advanced/relay_calls.md).
`.call("pricing", { deadlineMs, answerChannel, codec })` or `.call({ factory: "agent", create: false }, { deadlineMs })`
ends a chain on a call to another relay and answers a stream of `Response`s:
[`../topics/advanced/relay_calls.md`](../topics/advanced/relay_calls.md).

A JSON number is a double, so an integer above 2^53 loses precision on the way
in and out. A `bigint`, `undefined` or a function has no JSON form and fails
the emit naming the channel; `'msgpack'` carries a `bigint` at any width.

A value passed between two tasklets of one relay never crosses a channel
codec: it stays MessagePack. An Elixir entry declared with no decoder hands its
tasklet the binary instead, since an Elixir string is its binary, so the same
bytes on an undeclared entry are an object here and a string there.

## Durable state

State and change bodies serialize as MessagePack (`format_id = 5`) with a
specified JavaScript type mapping rather than a library default:

| JavaScript | On the wire |
| --- | --- |
| `bigint` | arbitrary-width integer extension — u64 offsets, sequences and timestamps round-trip with no narrowing, including values above 2^63 |
| `Date` | the MessagePack timestamp extension |
| `Uint8Array` / `Buffer` | `bin`, decoding to `Uint8Array` in both cases |
| absent property | omitted key, so absent stays distinguishable from `null` |

A value the codec cannot represent — a function, a symbol, a cycle, a class
instance, an `undefined` array element — is refused at registration naming the
type and the field. A type that needs another encoding supplies a hand-written
codec.

Durable records are stamped `sdk_lang = 4`. A record another SDK wrote is
refused by name rather than decoded, and the reverse holds too.

## Local mode: run a graph with no controller

`LocalRelay.start` takes the relay definition you deploy and runs its
declared graph in this process — no controller, no socket, no local-mode member
on the definition:

```ts
import { LocalRelay } from '@flezha/sdk';

const run = LocalRelay.start(WordCount);
try {
  run.onSink('totals', (out) => console.log(out.payload.toString('utf8')));
  await run.feed('lines', 'the quick brown fox');
  await run.runUntilIdle(10_000);
} finally {
  await run.close();
}
```

`onSink` registers against the sink's **channel** name, which is the producing
tasklet's name unless the declaration `named` it. `feed`, `runUntilIdle` and
the sink drain are promises because each parks in the core, and the fetchers
that run the tasklets need this event loop free. Worked example:
[`examples/node/local_word_count.mjs`](../../examples/node/local_word_count.mjs).

**State is held in memory and nothing is durable** — no write-ahead log, no
snapshots, no dead-letter queue. Durability, effectively-once delivery, plugins,
placement and migration, the dead-letter queue, observability export, authz and
multi-runner scale need a controller; a declaration asking for one of those is
refused at startup, naming it. Walk-through:
[`../topics/basics/local_mode.md`](../topics/basics/local_mode.md).

## Testing a tasklet

```ts
import { TaskletTestHarness } from '@flezha/sdk/testing';

const harness = TaskletTestHarness.of(Ohlc, { cellKey: 'AAPL' });
const applied = await harness.input({ symbol: 'AAPL', price: 10, at: 0 });
assert.deepEqual(applied.emits, [{ symbol: 'AAPL', close: 10 }]);
harness.replayEquals();
```

`replayEquals()` folds the recorded change log against a freshly constructed
initial state and requires the result to equal the live one — the engine
rebuilds a cell exactly that way on a warm restart, so a difference is what
a restart would lose. A handler is also an ordinary function: a plain unit test
can call it with no runner started and no controller running.

## Configuration

| Variable | Meaning | Default |
| --- | --- | --- |
| `FLEZHA_NODE_WORKERS` | Dispatch loops; `0` is the main loop | `0` |
| `FLEZHA_WORKER_POOL_SIZE` | Base native threads for blocking core calls | `2` |
| `FLEZHA_WORKER_POOL_MAX` | Ceiling the pool and the fetcher set may grow to | 10× the base |
| `FLEZHA_TRANSPORT` | `uds` (default) or `quic` | `uds` |
| `FLEZHA_NATIVE_BINARY` | An already-built addon, skipping resolution | unset |
| `FLEZHA_LOG_LEVEL` | `debug` / `info` / `warn` / `error` for the default logger | `warn` |

Transport configuration rides the same environment every poll host reads —
[Runner environment variables](../configuration/runner_environment.md).

## See also

- [`poll_core.md`](poll_core.md) — the kernel and its host contract
- [`authoring_contract.md`](authoring_contract.md) — the language-agnostic authoring contract
- [`conformance.md`](conformance.md) — the cross-SDK conformance suite
- [`../architecture/runner.md`](../architecture/runner.md) — the per-SDK matrix and the `sdk_lang` table
