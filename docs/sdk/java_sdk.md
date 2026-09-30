# Java SDK

A Java 21 binding hosted on the shared `flz_core` kernel through
`flz_core_ffi`'s C ABI over JNI — the same kernel the Rust crate uses, so
dispatch, overflow, the retry→park ladder, the partition guard, timers and the
effectively-once tier are one implementation, not two.

**Support tier:** [tier 1](../publishing/maturity.md#sdk-java), run by the maintainer in a
live deployment, passes the full conformance run in CI and is permanently supported.

Authoring surface: one tasklet form, a class implementing
`Tasklet<Input, State, Output>` and carrying `@Tasklet.Spec`, wired by an
`UnboundedOperator` / `BoundedOperator` in `channels(ChannelBuilder)`, the one
graph body the `Operator` base declares for both kinds; and the
`@UnboundedOperator` / `@BoundedOperator` short form. Nothing is generated: the
class you write is the class every call names. See
[`authoring_contract.md`](authoring_contract.md) for the rules every SDK shares
and [`state_persistence.md`](state_persistence.md) for the state and Kryo side.

## Tasklets

A tasklet is your own class. It implements `Tasklet` with its three types, and
carries `@Tasklet.Spec` with the name its state is saved under:

```java
@Tasklet.Spec(name = "track_prices")
public final class TrackPrices implements Tasklet<Trade, TrackPrices.PriceWindow, Move> {

    public static class PriceWindow {
        public Deque<Double> prices = new ArrayDeque<>();
    }

    public record PriceSeen(double price) implements Change<PriceWindow> {
        @Override
        public void applyTo(PriceWindow window) {
            window.prices.addLast(price);
        }
    }

    @Override
    public void run(Trade trade, Ctx<PriceWindow, Move> ctx) throws TaskletException {
        ctx.change(new PriceSeen(trade.price()));
        ctx.emit(new Move(trade.symbol(), trade.price()));
    }
}
```

The operator names that class: `.into(TrackPrices.class)`.

- **`run(input, ctx)`** returns nothing. `ctx` is the one way out: `ctx.emit(value)`
  typed by `Output`, `ctx.emit(channel, value)` for a named channel,
  `ctx.emitKeyed(key, value)`, and the rest of the context (`now()`, `stableId()`,
  `cellKey()`, `checkpoint()`, the timer calls, `spawnBounded`, `complete`).
- **`ctx.part(value)`** sends a part: a preview of the output the invocation is
  producing, which leaves now instead of when `run` returns. It previews the
  tasklet's own output channel, is encoded as `ctx.emit(value)` would be, and
  never waits: a part that cannot leave at once is dropped. A listener receives
  it, numbered from 0 within each attempt; a sink never does, and nothing about
  it is durable. It works in `run` and in `onTimer`, for an unbounded tasklet
  and a bounded stage alike. It throws `part.no_sink` when the output channel
  does not end in a sink (a `response()` end is not one), and
  `part.outside_invocation` through a context kept past its invocation.
- **State** is read with `ctx.state()` and changed only with `ctx.change(change)`.
  A change is a record implementing `Change<State>`; its `applyTo(state)` is the
  only place a state field is written, and it runs again for every recorded
  change when a cell is rebuilt. `Void` as the state keeps nothing.
- **A new cell's state** is the state class's no-arg constructor. A state that
  depends on the cell's key comes from an overridden `initState(Optional<String>)`.
  A state class with neither fails registration naming the class.
- **`Void` as the output** declares no output channel.
- **Hooks are declared by writing them**: `onTimer`, `eventTime`, `compensate`,
  `cellsFor`, `initState`, `migrateState`, `migrateChange` and `kryoTypes`.

`@Tasklet.Spec` is read at registration:

| Member | Meaning | Default |
|---|---|---|
| `name` | the durable name the state is saved under, never derived from the class | required |
| `keys` | static cell keys, with a written `cellsFor` | none |
| `capacity` | per-cell mailbox capacity | unset: `FLEZHA_MAILBOX_CAPACITY`, else 1024 |
| `maxCells` | dynamic-mode soft cap on live cells | 100 000 |
| `idleTtlSeconds` | whole seconds a dynamic cell may sit idle before eviction | 0, no eviction |
| `maxTimersPerCell` | pending timers per cell | 10 000 |
| `compactAfterBytes` | change-log size before a snapshot is asked for | 8 MiB |
| `schemaVersion`, `minReadableSchemaVersion` | the record version, and the oldest still read | 1, and the schema version |
| `logicalClock` | serve `ctx.now()` from the durable change seq | false |

A class without it fails registration with `TaskletSpecMissing`, and a negative
count or duration fails naming the member. The clock follows from what the class
declares: the wall clock by default, event time when it writes `eventTime(input)`,
the logical clock with `logicalClock = true`. Writing `eventTime` together with
`logicalClock = true` is refused, and a logical clock over `Void` state is refused
as `topology.unservable_clock`.

### Encoded types

The SDK finds the types a tasklet's state and changes are encoded with: the state
class, every member class (declared in a class body, at any depth) of the tasklet
class, a class above it, or the state class that implements `Change`, and every
non-JDK class their fields reach; a local or anonymous class is not found. Each takes an id derived from its name, so adding a change record never moves
another's id. A type nested in the tasklet class is named by the tasklet's
`@Tasklet.Spec` name and its path inside the class, so renaming or moving the
tasklet class keeps every id; any other type is named by its binary class name,
so renaming it changes its id. Two classes
deriving one id fail registration naming both. A change class declared anywhere
else is not found, and its first flush fails with `UnregisteredClass`, naming the
class and both fixes: declare it as a member class of the tasklet or state class,
or list it.

Writing `kryoTypes()` replaces discovery: the listed types take ids in list order
after the built-ins, so only appending is safe. A list owns the ids from the first
save, so a tasklet that expects to rename or move its state or change classes
lists them from the start. **Switching an existing tasklet between discovery and a
list moves every id**: the saved records no longer decode, so bump
`schemaVersion` and read the old records through `migrateState` /
`migrateChange`.

State saved by an earlier Java SDK, whose tasklets took six type parameters, is
not readable by this one: the ids of user types changed. Clear a Java operator's
saved state once when moving to it.

## Threading

Platform *fetcher* threads loop `poll → decode → hand off → poll`; one virtual
thread per activation runs the dispatch, encodes the result and calls
`complete` itself. Fetchers stay platform threads because `FlzCore.poll` blocks
in native code and a native frame pins its carrier.

One platform **supervisor** thread parks in `FlzCore.waitStarvation` and starts
one more fetcher on every `STARVATION_OBSERVED`, up to a ceiling. It is not
optional: a virtual thread parked inside `complete` — pushing into a consumer
nobody is draining — holds its fetcher's handout, and with every fetcher held
the runner processes nothing and reports nothing. Without the supervisor that
probe delivered 0 of 20 payloads and stayed wedged. Growth is self-limiting:
each new fetcher's own `poll` entry restarts the core's dwell. What it does not
buy is a thread against user code that never returns — that is the conformance
`pool_wedge` condition, and no ceiling outruns it.

**With `FLEZHA_DROP_NOTHING_NONPARKING` on, `complete` does not park on a full
consumer** (the switch is off by default during rollout). The kernel holds an
undeliverable inter-tasklet output in the producing Cell's push holdback
and withholds work from that Cell instead, so the completion returns and the
virtual thread — and the carrier it pinned — is released. This SDK carries **no
Java-side code for it**: `flz_core_complete` is the seam, so the behaviour lives
below it. The `pending_pushes` / `pending_push_oldest_ms` heartbeat keys it adds
are minted by `flz_core`'s heartbeat, which this SDK also uses unchanged. The starvation
supervisor stays exactly as load-bearing: with the flag off it is still the only
rescue, and it is what the holdback's bound falls back on.

| Knob | Meaning | Default |
| --- | --- | --- |
| `FLEZHA_WORKER_POOL_SIZE` | Fetchers started at boot | 2 |
| `FLEZHA_WORKER_POOL_MAX` | Ceiling the supervisor may grow to | 10× the count, never below it |

Every growth is logged at `INFO`; the first refusal at the ceiling is logged at
`WARN` and latched, so a sustained wedge names itself once instead of once a
second.

## Diagnostics

`FLEZHA_LOG_LEVEL` (`debug` | `info` | `warn` | `error`, default `warn`) selects
what the default logger writes to standard error, prefixed `[flezha]`. Unset,
blank and unrecognised all mean `warn`, as in the other five SDKs. There is no
logging dependency: the SDK owns a four-level `Logger` interface and a
stderr-writing default, so a fresh application gets the failure paths with
nothing to add and no missing-provider warning at startup.

A host routes them into its own pipeline by passing a `Logger` to
`FlezhaRunner.logger(...)`, the surface Node (`runner.logger(l)`), PHP
(`Runner->logger()`) and Ruby (`Flezha.log =`) already carry. `Logger` is a
functional interface and the messages carry SLF4J's `{}` placeholders, so an
SLF4J adapter is one lambda:

```java
runner.logger((level, format, args) ->
        slf4j.atLevel(Level.valueOf(level.name())).log(format, args));
```

A supplied logger decides for itself what it writes, so `FLEZHA_LOG_LEVEL`
filters the default sink and nothing else. A sink that renders the line itself
reuses the SDK's substitution through `Logger.render(format, args)` rather than
writing a second one. The sink is JVM-wide and survives `close()`,
because a runner's most interesting diagnostics are the ones it writes while
shutting down. A write that fails is swallowed, so a standard error closed
during a drain cannot turn a clean drain into a crash.

## The short form

A tasklet class annotated `@UnboundedOperator` or `@BoundedOperator` is a whole
operator: one class, no hand-written operator beside it. The annotation names
the kind, and the class you wrote is what you register. `registerFactory` reads
the annotation and declares the operator; nothing is generated.

```java
@UnboundedOperator(id = "upcase", sink = "out")
public class Upcase implements Tasklet<String, Void, String> {
    @Override
    public void run(String value, Ctx<Void, String> ctx) throws TaskletException {
        ctx.emit(value.toUpperCase());
    }
}

@BoundedOperator(id = "place", sink = "placed", input = "order")
public class Place implements Tasklet<String, Void, String> {
    @Override
    public void run(String order, Ctx<Void, String> ctx) throws TaskletException {
        ctx.emit(order);
    }
}

Flezha.createRunner("default")
        .registerFactory(Upcase.class)
        .registerFactory(Place.class)
        .run();
```

The operator is the one you would have written by hand with that single
tasklet: an entry channel named `input`, or the `input` element's value; the
class; a plugin sink named by `sink` when one is written; a `factoryId()` equal
to `id`; `loopMode()` returning the `loopMode` element on `@BoundedOperator`;
and every other operator method at its interface default. A bounded short form
is a `once` bounded operator unless it writes `loopMode`, run once over its
first message: `flz operator start place --text '<payload>'`, or a start
followed by `flz operator send`; a second message to a `once` instance is
accepted and never read. `loopMode = LoopMode.PER_INPUT` runs `run` once per
message, and the instance does not complete after the first. `onFull` sets the
entry channel's overflow policy. `LocalOperator.start(Upcase.class)` runs a
short form locally the same way.

The tasklet is named after the `id`, so a short form needs no `@Tasklet.Spec`.
It carries one only for settings (`keys`, `capacity`, `maxCells`, ...), and then
its `name` must equal the `id`, or registration fails with
`ShortFormNameMismatch`: one operator, one name. The output type is the class's
`Output`, and a keyed emit is the body's `ctx.emitKeyed`.

`sink` is optional. With none, the operator declares the entry channel and the
tasklet and no plugin channel: the operator that only keeps state.

```java
@UnboundedOperator(id = "counter", input = "ticks")
public class Counter implements Tasklet<String, Counter.Seen, Void> {

    public static class Seen {
        public List<String> ticks = new ArrayList<>();
    }

    public record Ticked(String tick) implements Change<Seen> {
        @Override
        public void applyTo(Seen seen) {
            seen.ticks.add(tick);
        }
    }

    @Override
    public void run(String tick, Ctx<Seen, Void> ctx) {
        ctx.change(new Ticked(tick));
    }
}
```

Anything more is the full form: a second tasklet, a fan-out, a merge, a keyed
edge, an edge's retry ladder, the effectively-once tier, a correction window,
any bounded setting other than the loop mode, a `.response()` end. Java names
no entry codec: an entry decodes by its declared type, so the short form takes
no `codec` element.

`.response()` ends a channel on the answer to an outside call instead of a sink,
and `.response(Function<T, byte[]>)` takes a serializer as `.sink` does:
[`calls.md`](calls.md). `.call(CallTarget.id("pricing"), Answer.class, deadlineMs)`
(or `CallTarget.factory("agent").create(false)` after a `keyBy`) ends a tasklet's
output chain on a call to another operator and answers an `EntryChannel` of
`Response<Req, Answer>`: [`calls.md`](calls.md#operator-calls-call).

`id` must be written: the factory id is an address a user types, and a
class rename must not move it. The written `id` also names the tasklet, so a
Java tasklet's state is saved under the same name its Rust twin uses. The
refusals are raised when the class is registered, each naming the class: an
absent `id`; `topology.short_form_with_topology` (an annotated class that also implements
`UnboundedOperator` or `BoundedOperator`); `topology.sink_without_output` (a `sink` on a
class whose `Output` is `Void`); and `ShortFormNameMismatch`. A second operator
of either kind under a factory id the runner holds fails `registerFactory` with
`topology.duplicate_factory_id`.

Nothing registers itself. There is no aggregator class: every operator a
runner declares is one the application handed to `registerFactory`.

## Bounded operators

A `BoundedOperator` declares its graph in `channels`, on the builder an unbounded operator
uses, and its kind is the interface it implements: `flz operator start` names
the factory and nothing else. Retry is the edge's, as on an unbounded operator:
`.retryMax(n)` / `.retryBackoffMs(ms)` on the channel that feeds a tasklet
(default 3 attempts, 50 ms), and `retryMax(0)` is refused as `topology.retry_max_zero`.

A bounded operator carries none of an unbounded operator's delivery settings. The builder's
`effectivelyOnce()`, checkpoint calls and `correctionWindow(ms)` reach the core
as written, and `registerFactory` throws a `FlezhaException` with the code
`topology.effectively_once_on_bounded`, `topology.checkpoint_on_bounded` or
`topology.correction_window_on_bounded`. `syncDurability()` is the bounded operator's own.

Both kind interfaces carry the two class settings: `persistenceEnabled()` (default
`true`, read from the class whatever its tasklets' states) and
`sleepWhenIdle()`, `false` on `UnboundedOperator` and `persistenceEnabled()` on
`BoundedOperator`. An instance whose class sleeps is evicted after
`FLEZHA_PARK_IDLE_MS` of idleness and wakes on its next message with its cells
restored; declaring it with persistence off makes `registerFactory` throw naming
`topology.sleep_without_persistence`.

A bounded operator
tasklet registers timers as an unbounded tasklet does, and a firing runs as a pass
of its own ([`cell_timers.md`](cell_timers.md#timers-in-a-bounded-operator)); only a
`compensate` hook's context carries no timer session, and there every timer
call throws a `FlezhaException` with the code `runtime.timers_unavailable`. A `spawnBounded` naming
an unbounded factory reads `operator.factory_not_bounded`.

## Scope

Every capability of the conformance vocabulary, deferred: none. The one place
that scope is declared is the manifest —
[`test/apps/java_runner/conformance.json`](../../test/apps/java_runner/conformance.json)
— read by the harness, the bench and the static gate; this page does not
restate it. See [`conformance.md`](conformance.md).

The one behavioural gap that is not a capability: a lazily-created dynamic
Cell is not registered with the flusher, so its state does not reach disk.
Dynamic-cell scenarios assert against live state for this runtime.

## Packaging

Build output. None of it is tracked in git.

| Artifact | Path | Produced by |
| --- | --- | --- |
| Native `flz_core_ffi`, host triple | `sdk/java/flezha-sdk-native/src/main/resources/native/<host>/` | `scripts/build-native.sh --output <that dir>` |
| Native `flz_core_ffi`, all 5 platforms | the same tree, one directory each | `scripts/build-dist.sh`, or `mvn -P native-build` fetching the release assets |
| SDK fat jar | `sdk/java/flezha-sdk/dist/flezha-sdk.jar` | `scripts/build-dist.sh` |
| Test runner fat jar | `test/apps/java_runner/target/java_runner.jar` | `mvn -f test/apps/java_runner/pom.xml package` |

Two profiles, two jobs:

- **`native-verify`** asserts the **host triple** is staged and neither builds
  nor downloads. Every test path uses it — `mvn verify`, the e2e harness, CI —
  so the suites run offline against one binary.
- **`native-build`** is portable packaging: it stages every absent triple (the
  host compiled, the rest downloaded from the GitHub Release matching the pinned
  version and checksum-verified), leaves anything already on disk alone, and
  asserts all five before shading.

The e2e harness spawns `target/java_runner.jar`, falling back to `dist/` for a
tree assembled by `scripts/build-dist.sh`. Both launch lines come from the
manifest.

## Testing

```bash
mvn -f sdk/java/pom.xml -P coverage,native-verify verify   # unit + JNI + coverage
FLEZHA_E2E_RUNTIME=java mix test --include sdk_smoke       # the shared smoke tier
FLEZHA_E2E_RUNTIME=java mix test --include system          # the full suite
```

The Java SDK's own repository runs the first on every pull request, and this
repository's `.github/workflows/java-sdk.yml` runs the second on every pull
request that touches the SDK pin, the kernel or the transport; the nightly
matrix runs the third.

## See also

- [`conformance.md`](conformance.md) — the suite, the manifest, adding an SDK
- [`poll_core.md`](poll_core.md) — the kernel and the C ABI this SDK is hosted on
- [`state_persistence.md`](state_persistence.md) — the Java state surface
- [`protocol_spec.md`](protocol_spec.md) — the wire protocol
