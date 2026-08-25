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

| Knob | Meaning | Default |
| --- | --- | --- |
| `FLEZHA_WORKER_POOL_SIZE` | Fetchers started at boot | 2 |
| `FLEZHA_WORKER_POOL_MAX` | Ceiling the supervisor may grow to | 10× the count, never below it |

Every growth is logged at `INFO`; the first refusal at the ceiling is logged at
`WARN` and latched, so a sustained wedge names itself once instead of once a
second.

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

Committed artifacts, refreshed by `scripts/build-dist.sh` and never built by
CI:

| Artifact | Path |
| --- | --- |
| Native `flz_core_ffi` (5 platforms) | `sdk/java/flezha-sdk-native/src/main/resources/native/<platform>/` |
| SDK fat jar | `sdk/java/flezha-sdk/dist/flezha-sdk.jar` |
| Test runner fat jar | `test/apps/java_runner/dist/java_runner.jar` |

The test runner jar is what the e2e harness spawns (`test` profile); the bench
prefers a locally built `test/apps/java_runner/target/java_runner.jar`
(`bench` profile). Both launch lines come from the manifest.

## Testing

```bash
mvn -f sdk/java/pom.xml -P coverage,native-verify verify   # unit + JNI + coverage
FLEZHA_E2E_RUNTIME=java mix test --include sdk_smoke       # the shared smoke tier
FLEZHA_E2E_RUNTIME=java mix test --include system          # the full suite
```

`.github/workflows/java-sdk.yml` runs the first two on every pull request that
touches the SDK, the kernel, the transport or the test app; the nightly matrix
runs the third.

## See also

- [`conformance.md`](conformance.md) — the suite, the manifest, adding an SDK
- [`poll_core.md`](poll_core.md) — the kernel and the C ABI this SDK is hosted on
- [`state_persistence.md`](state_persistence.md) — the Java state surface
- [`protocol_spec.md`](protocol_spec.md) — the wire protocol
