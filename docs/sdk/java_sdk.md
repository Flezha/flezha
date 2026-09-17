# Java SDK

A Java 21 binding hosted on the shared `flz_core` kernel through
`flz_core_ffi`'s C ABI over JNI — the same kernel the Rust crate uses, so
dispatch, overflow, the retry→park ladder, the partition guard, timers and the
effectively-once tier are one implementation, not two.

Authoring surface: `StreamOperative` / `WorkflowOperative` / `Procedure` with
`ChannelBuilder`, plus the `@Procedure` / `@State` / `@StreamOperative`
annotation sugar. See [`authoring_contract.md`](authoring_contract.md) for what
each tier expands to and [`state_persistence.md`](state_persistence.md) for the
`StateHandle` / Kryo side.

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

**`complete` no longer parks on a full consumer** (`FLEZHA_DROP_NOTHING_NONPARKING`,
default off during rollout). The kernel holds an undeliverable inter-procedure
output in the producing Entity's push holdback and withholds work from that
Entity instead, so the completion returns and the virtual thread — and the
carrier it pinned — is released. This SDK carries **no Java-side code for it**:
`flz_core_complete` is the seam, so the change arrives below it. The
`pending_pushes` / `pending_push_oldest_ms` heartbeat keys it adds are minted by
`flz_core`'s heartbeat, which this SDK also uses unchanged. The starvation
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

## Scope

Every capability of the conformance vocabulary, deferred: none. The one place
that scope is declared is the manifest —
[`test/apps/java_runner/conformance.json`](../../test/apps/java_runner/conformance.json)
— read by the harness, the bench and the static gate; this page does not
restate it. See [`conformance.md`](conformance.md).

The one behavioural gap that is not a capability: a lazily-created dynamic
Entity is not registered with the flusher, so its state does not reach disk.
Dynamic-entity scenarios assert against live state for this runtime.

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
