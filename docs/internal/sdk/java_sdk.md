# Java SDK

A Java 21 binding hosted on the shared `flz_core` kernel through
`flz_core_ffi`'s C ABI over JNI — the same kernel the Rust crate uses, so
dispatch, overflow, the retry→park ladder, the partition guard, timers and the
effectively-once tier are one implementation, not two.

**Support tier:** [tier 1](../../public/publishing/maturity.md#sdk-java), run by the maintainer in a
live deployment, passes the full conformance run in CI and is permanently supported.

Installing, the supported Java versions, how the native library reaches the host
and the commands that test the SDK are in its [`README.md`](../../../sdk/java/README.md);
[local mode](../../public/topics/core/running_and_testing.md#local-mode) and the
[test harness](../../public/topics/core/running_and_testing.md#check-a-tasklet-on-its-own) are on the Running relays page.

Authoring surface: one tasklet form, a class implementing
`Tasklet<Input, State, Output>` and carrying `@Tasklet.Spec`, wired by a
`Relay` in `channels(ChannelBuilder)` under the settings tree its `run()` and
`durable()` declare; and the `@ShortRelay` short form. Nothing is generated: the
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

The relay names that class: `.into(TrackPrices.class)`.

- **`run(input, ctx)`** returns nothing. `ctx` is the one way out: `ctx.emit(value)`
  typed by `Output`, `ctx.emit(channel, value)` for a named channel,
  `ctx.emitKeyed(key, value)`, and the rest of the context (`now()`, `stableId()`,
  `cellKey()`, `checkpoint()`, the timer calls, `spawnSteps`, `complete`, and
  `effect(name, type, supplier)`, which keeps an outside call's result so a re-run reads it
  instead of calling again; [`effectively_once.md`](../../public/topics/core/effectively_once.md#recorded-effects)).
- **`ctx.part(value)`** sends a part: a preview of the output the invocation is
  producing, which leaves now instead of when `run` returns. It previews the
  tasklet's own output channel, is encoded as `ctx.emit(value)` would be, and
  never waits: a part that cannot leave at once is dropped. A listener receives
  it, numbered from 0 within each attempt; an output never does, and nothing about
  it is durable. It works in `run` and in `onTimer`, for a flow tasklet
  and a steps stage alike. It throws `part.no_output` when the output channel
  does not end in an output (a call end is not one), and
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
| `cellKeys` | static cell keys, with a written `cellsFor` | none |
| `mailboxCapacity` | per-cell mailbox capacity | unset: `FLEZHA_MAILBOX_CAPACITY`, else 1024 |
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
not readable by this one: the ids of user types changed. Clear a Java relay's
saved state once when moving to it.

## Threading

Platform *fetcher* threads loop `poll → decode → hand off → poll`; one virtual
thread per activation runs the dispatch, encodes the result and calls
`complete` itself. Fetchers stay platform threads because `FlzCore.poll` blocks
in native code and a native frame pins its carrier.

How many fetchers is the core's pool governor's
([`poll_core.md`](poll_core.md#the-core-decides-the-sdk-brings-the-threads)),
over this host's base of 2: a fetcher never runs user code, so two saturate the
core's handout. One platform **supervisor** thread answers
`FlzCore.poolSupervise` and starts the fetcher each answer asks for; a grown
fetcher retires after the governor's idle time. It is not optional: a virtual
thread parked inside `complete` — pushing into a consumer nobody is draining —
holds its fetcher's handout, and with every fetcher held the runner processes
nothing and reports nothing. Without the supervisor that probe delivered 0 of
20 payloads and stayed wedged. What it does not buy is a thread against user
code that never returns — that is the conformance `pool_wedge` condition, and
no ceiling outruns it.

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

`FLEZHA_WORKER_POOL_SIZE` and `FLEZHA_WORKER_POOL_MAX` size it; their types
and defaults are in [Runner environment variables](../../public/configuration/runner_environment.md#every-variable).
The governor's notices, a replaced fetcher and the first refusal at the
ceiling (latched), reach this SDK's logger through the supervisor.

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
filters the default output and nothing else. An output that renders the line itself
reuses the SDK's substitution through `Logger.render(format, args)` rather than
writing a second one. The output is JVM-wide and survives `close()`,
because a runner's most interesting diagnostics are the ones it writes while
shutting down. A write that fails is swallowed, so a standard error closed
during a drain cannot turn a clean drain into a crash. The core's notices reach
the same logger, never from a core thread: the poll host's supervisor takes
them (`FlzCore.takeNotices`) on each wake and when a run stops, and writes the
local-mode banner at `ERROR`.

## The settings tree

`Relay` is the one base. Besides `factoryId()` and `channels(...)` it carries
`description()`, `maxDuration()` (a wall-clock stop for either run), `run()` and
`durable()`. Each "one of" is a sealed interface and each nested setting a
record component of its parent only, so a setting under the wrong parent does
not compile:

| Setting | Values | Default |
|---|---|---|
| `run()` | `Run.Flow(exactly)`, `Run.Steps(each, onFailure)` | `Run.flow()` |
| `exactly` | `Exactly.AtLeastOnce()`, `Exactly.EffectivelyOnce(ack, correctionMs, beyondWindow)` | at least once |
| `ack` | `Ack.PerInput()`, `Ack.Checkpoint(every, maxDelayMs, outputs)` | per input |
| `each` | `Each.ONCE`, `PER_INPUT`, `CHAINED` | `ONCE` |
| `onFailure` | `OnFailure.UNDO_ALL`, `KEEP_PROGRESS` | `UNDO_ALL` |
| `durable()` | `Durable.Memory()`, `Durable.Local(sleepAfter)`, `Durable.Remote(sleepAfter)` | local; a flow never sleeps, a steps class after six hours |

```java
public final class Checkout implements Relay {
    @Override public String factoryId() { return "checkout"; }

    @Override public Run run() {
        return new Run.Steps(Each.PER_INPUT, OnFailure.KEEP_PROGRESS);
    }

    @Override public Durable durable() {
        return new Durable.Local(Duration.ofMinutes(10));
    }

    @Override public void channels(ChannelBuilder c) { ... }
}
```

`sleepAfter` is an `Optional<Duration>`: empty never sleeps. A sleeping instance
is evicted from its runner and wakes on its next message with its cells
restored. `EffectivelyOnce` and `Ack.Checkpoint` take `withAck`,
`withCorrectionMs`, `withBeyondWindow`, `withEvery`, `withMaxDelayMs` and
`withOutputs`. The one cross rule the tree cannot express, `EffectivelyOnce` with
`Durable.Memory()`, makes `registerFactory` throw naming
`topology.effectively_once_without_persistence`; a value outside a setting's
range is refused by the core by name.

## The short form

A tasklet class annotated `@ShortRelay` (`com.flezha.sdk.authoring`) is a whole
relay: one class, no hand-written relay beside it. The class you wrote is
what you register. `registerFactory` reads the annotation and declares the
relay; nothing is generated.

```java
@ShortRelay(factoryId = "upcase", output = "out")
public class Upcase implements Tasklet<String, Void, String> {
    @Override
    public void run(String value, Ctx<Void, String> ctx) throws TaskletException {
        ctx.emit(value.toUpperCase());
    }
}

@ShortRelay(factoryId = "place", output = "placed", input = "order", run = @Run(steps = @Steps))
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

The relay is the one you would have written by hand with that single
tasklet: an entry channel named `input`, or the `input` element's value; the
class; a plugin output named by `output` when one is written; a `factoryId()` equal
to `factoryId`; and the settings tree the annotation writes. The short form takes the
class settings the full form takes, as nested annotations whose members exist
only under their parent: `run = @Run(flow = @Flow(effectivelyOnce =
@EffectivelyOnce(checkpoint = @Checkpoint(every = 100))))`, `run = @Run(steps =
@Steps(each = Each.PER_INPUT))`, `durable = @Durable(false)`, `durable =
@Durable(local = @Level(sleepAfterMs = 2_000))`, `maxDurationMs` and
`description`. A "one of" is an array holding at most one branch; two written at
once make `registerFactory` throw naming `topology.invalid_setting`. A steps
short form runs once over its first message unless it writes `each`:
`flz relay start place --text '<payload>'`, or a start followed by
`flz relay send`; a second message to a `once` instance is accepted and never
read. `each = Each.PER_INPUT` runs `run` once per message, and the instance does
not complete after the first. `onFull` sets the entry channel's overflow policy.
`LocalRelay.start(Upcase.class)` runs a short form locally the same way.

The tasklet is named after the `factoryId`, so a short form needs no `@Tasklet.Spec`.
It carries one only for settings (`cellKeys`, `mailboxCapacity`, `maxCells`, ...), and then
its `name` must equal the `factoryId`, or registration fails with
`ShortFormNameMismatch`: one relay, one name. The output type is the class's
`Output`, and a keyed emit is the body's `ctx.emitKeyed`.

`output` is optional. With none, the relay declares the entry channel and the
tasklet and no plugin channel: the relay that only keeps state.

```java
@ShortRelay(factoryId = "counter", input = "ticks")
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
edge, an edge's retry ladder, a `.call(...)` end. Java names no entry codec: an
entry decodes by its declared type, so the short form takes no `codec` element.

A called input's first output answers its caller and still reaches the output's plugins:
[`sending_and_calling.md`](../../public/topics/core/sending_and_calling.md#calls). `.call(CallTarget.id("pricing"), Answer.class, deadlineMs)`
(or `CallTarget.factory("agent").create(false)` after a `keyBy`, and
`.answerChannel("quotes")` to name the callee's answering channel) ends a tasklet's
output chain on a call to another relay and answers an `EntryChannel` of
`Response<Req, Answer>`: [`sending_and_calling.md`](../../public/topics/core/sending_and_calling.md#calls).

`factoryId` must be written: the factory id is an address a user types, and a
class rename must not move it. The written `factoryId` also names the tasklet, so a
Java tasklet's state is saved under the same name its Rust twin uses. The
refusals are raised when the class is registered, each naming the class: an
absent `factoryId`; `topology.short_form_with_topology` (an annotated class that also implements
`Relay`); `topology.output_without_type` (an `output` on a
class whose `Output` is `Void`); and `ShortFormNameMismatch`. A second relay
under a factory id the runner holds fails `registerFactory` with
`topology.duplicate_factory_id`, and a class that is neither a relay nor a
short form with `topology.no_relay_exported`.

Nothing registers itself. There is no aggregator class: every relay a
runner declares is one the application handed to `registerFactory`.

## Relays that run in steps

A class whose `run()` is `Run.Steps` declares its graph in `channels`, on the same
builder a flow uses: `flz relay start` names the factory and nothing else.
Retry is the edge's, as in a flow: `.retryMax(n)` / `.retryBackoffMs(ms)` on the
channel that feeds a tasklet (default 3 attempts, 50 ms), and `retryMax(0)` is
refused as `topology.retry_max_zero`. A steps class has no `exactly`: a durable
one already commits stage by stage.

A tasklet of a steps relay registers timers as a flow tasklet does, and a
firing runs as a pass of its own ([`../../public/topics/core/timers.md`](../../public/topics/core/timers.md));
only a `compensate` hook's context records no timer ops, and there every
timer call throws a `FlezhaException` with the code `runtime.timers_unavailable`.
A `spawnSteps` naming a flow factory reads `relay.factory_not_steps`. A flow
tasklet's `complete` ends its instance once the work its cells hold has run.

## Scope

Every capability of the conformance vocabulary, deferred: none. The one place
that scope is declared is the manifest —
[`test/apps/java_runner/conformance.json`](../../../test/apps/java_runner/conformance.json)
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

## See also

- [`conformance.md`](conformance.md) — the suite, the manifest, adding an SDK
- [`poll_core.md`](poll_core.md) — the kernel and the C ABI this SDK is hosted on
- [`state_persistence.md`](state_persistence.md) — the Java state surface
- [`protocol_spec.md`](protocol_spec.md) — the wire protocol
