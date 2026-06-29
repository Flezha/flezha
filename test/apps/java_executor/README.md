# Java Compute Engine

Reference Java test app that mirrors `test/apps/rust_executor/`. Used
by the Java↔controller integration matrix under `mix test --only java_sdk` and
by the `mix bench --runtime java` regression bench.

The app registers three ContinuousOperative classes via `Flezha.registerContinuous(...)`:

- `do_nothing` — `DoNothingContinuousOperative` + `DoNothingOperation`
- `store_input` — `StoreInputContinuousOperative` + `StoreInputOperation`
- `compare_filter` — `CompareFilterContinuousOperative` + `CompareFilterOperation`

The factory id list matches `test/apps/rust_executor/`'s `do_nothing` /
`store_input` / `compare_filter` set; the `operative-message-handlers`
factories are absent from both apps until that capability ships.

It also carries authoring-sugar twins (`@Operation` / `@State` / `@ContinuousOperative`, generated
by `flezha-sdk-processor`): `SugarDoNothing`, `SugarStore`, and the 2-operation
`SugarEntry` → `SugarDownstream` reference operative (`SugarStaticSegmentContinuousOperative`).
`SugarTwinTest` asserts they register and process input identically to the
hand-written equivalents. See `docs/sdk_authoring_contract.md`.

## Build

```sh
mvn -f test/apps/java_executor/pom.xml package
java -jar test/apps/java_executor/target/java_executor.jar my_namespace
```
