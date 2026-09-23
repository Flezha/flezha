# Java SDK

A Java 21 binding hosted on the shared `flz_core` kernel through
`flz_core_ffi`'s C ABI over JNI — the same kernel the Rust crate uses, so
dispatch, overflow, the retry→park ladder, the partition guard, timers and the
effectively-once tier are one implementation, not two.

Authoring surface: `@Tasklet` classes, with `@State` for their state, wired by
a `StreamOperative` / `WorkflowOperative` in `channels(ChannelBuilder)`, the one
graph body the `Operative` base declares for both kinds; the
`Tasklet` interface the annotation processor implements for them; and the
`@StreamOperative` / `@WorkflowOperative` short form. See
[`authoring_contract.md`](authoring_contract.md) for what each tier expands to
and [`state_persistence.md`](state_persistence.md) for the `StateHandle` / Kryo
side.

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

A class annotated `@StreamOperative` or `@WorkflowOperative` is a whole
operative: one class with one `run`, no hand-written operative beside it. The
annotation names the kind, and the class you wrote is what you register:
`registerFactory` resolves the `<Class>StreamOperative` or
`<Class>WorkflowOperative` the processor generates beside `<Class>Tasklet`.

```java
@StreamOperative(id = "upcase", sink = "out")
public class Upcase {
    public String run(String value) {
        return value.toUpperCase();
    }
}

@WorkflowOperative(id = "place", sink = "placed", input = "order")
public class Place {
    public String run(String order) {
        return order;
    }
}

Flezha.createRunner("default")
        .registerFactory(Upcase.class)
        .registerFactory(Place.class)
        .run();
```

The generated operative is the one you would have written by hand with that
single tasklet: an entry channel named `input`, or the `input` element's
value; the tasklet; a plugin sink named by `sink` when one is written; a
`factoryId()` equal to `id`; `loopMode()` returning the `loopMode` element on
`@WorkflowOperative`; and every other operative method at its interface
default. A workflow short form is a `once` workflow unless it writes
`loopMode`, run once over its first message:
`flz operative start place --text '<payload>'`, or a start followed by
`flz operative send`; a second message to a `once` instance is accepted and
never read. `loopMode = LoopMode.PER_INPUT` runs `run` once per message, and
the instance does not complete after the first. Both annotations also
carry the tasklet's own elements, spelled as on `@Tasklet`: `key`, `emits`,
`keys` with an `@CellsFor` method and `capacity`; and `onFull` for the entry
channel.

`sink` is optional. With none, the operative declares the entry channel and the
tasklet and no plugin channel: the operative that only keeps state.

```java
@State
public class CounterState {
    public List<String> seen = new ArrayList<>();
}

@StreamOperative(id = "counter", input = "ticks")
public class Counter {
    public void run(String tick, CounterStateHandle state) {
        state.pushSeen(tick);
    }
}
```

Anything more is the full form: a second tasklet, a fan-out, a merge, a keyed
edge, an edge's retry ladder, the effectively-once tier, a correction window,
any workflow setting other than the loop mode. Java names no entry codec: an
entry decodes by its declared type, so the short form takes no `codec` element.

`id` must be written: the factory id is an address an operator types, and a
class rename must not move it. The written `id` also names the tasklet, so a
Java tasklet's state is saved under the same name its Rust twin uses. An
absent `id` fails compilation naming it, as does `kind`, `sink`, `input` or
`onFull` written on a `@Tasklet`, which declares a tasklet only. Two refusals fail
compilation, each message beginning with its own name: `ShortFormWithTopology`
(an annotated class that also implements `StreamOperative` or
`WorkflowOperative`) and `SinkWithoutOutput` (a `sink` on a `void` `run` with no
`emits`). A second operative of either kind under a factory id the runner holds
fails `registerFactory` with `DuplicateFactoryId`.

Nothing registers itself. There is no aggregator class: every operative a
runner declares is one the application handed to `registerFactory`.

## Workflows

A `WorkflowOperative` declares its graph in `channels`, on the builder a stream
uses, and its kind is the interface it implements: `flz operative start` names
the factory and nothing else. Retry is the edge's, as on a stream:
`.retryMax(n)` / `.retryBackoffMs(ms)` on the channel that feeds a tasklet
(default 3 attempts, 50 ms), and `retryMax(0)` is refused as `RetryMaxZero`.

A workflow carries none of a stream's delivery settings. The builder's
`effectivelyOnce()`, checkpoint calls and `correctionWindow(ms)` reach the core
as written, and `registerFactory` throws `WorkflowOperativeConfigurationException`
naming `EffectivelyOnceOnWorkflow`, `CheckpointOnWorkflow` or
`CorrectionWindowOnWorkflow`. `syncDurability()` is the workflow's own.

Both kind interfaces carry the two class settings: `persistenceEnabled()` (default
`true`, read from the class whatever its tasklets' Change types) and
`sleepWhenIdle()`, `false` on `StreamOperative` and `persistenceEnabled()` on
`WorkflowOperative`. An instance whose class sleeps is evicted after
`FLEZHA_PARK_IDLE_MS` of idleness and wakes on its next message with its cells
restored; declaring it with persistence off makes `registerFactory` throw naming
`SleepWithoutPersistence`.

A workflow
tasklet registers timers as a stream tasklet does, and a firing runs as a pass
of its own ([`cell_timers.md`](cell_timers.md#timers-in-a-workflow)); only a
`compensate` hook's context carries no timer session, and there every timer
call throws `TaskletException.TimersUnavailable`. A `spawnWorkflow` naming a stream factory reads
`SpawnException.FactoryNotWorkflow`.

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
