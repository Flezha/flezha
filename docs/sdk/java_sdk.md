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
