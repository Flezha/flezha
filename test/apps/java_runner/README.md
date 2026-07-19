# Java Compute Engine

Reference Java test app that mirrors `test/apps/rust_runner/`. Used
by the Java↔controller integration matrix under `mix test --only java_sdk` and
by the `mix bench --runtime java` regression bench.

The app declares its UnboundedOperative classes via
`Flezha.createRunner(ns).defineUnbounded(...)` — the integration-matrix core
being:

- `do_nothing` — `DoNothingUnboundedOperative` + `DoNothingOperation`
- `store_input` — `StoreInputUnboundedOperative` + `StoreInputOperation`
- `compare_filter` — `CompareFilterUnboundedOperative` + `CompareFilterOperation`

The factory id list matches `test/apps/rust_runner/`'s `do_nothing` /
`store_input` / `compare_filter` set; the `operative-message-handlers`
factories are absent from both apps until that capability ships.

It also carries authoring-sugar twins (`@Operation` / `@State` / `@UnboundedOperative`, generated
by `flezha-sdk-processor`): `SugarDoNothing`, `SugarStore`, and the 2-operation
`SugarEntry` → `SugarDownstream` reference operative (`SugarStaticSegmentUnboundedOperative`).
`SugarTwinTest` asserts they register and process input identically to the
hand-written equivalents. See `docs/sdk_authoring_contract.md`.

## Build

```sh
mvn -f test/apps/java_runner/pom.xml package
java -jar test/apps/java_runner/target/java_runner.jar my_namespace
```
