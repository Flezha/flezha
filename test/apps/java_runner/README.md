# Java Compute Engine

Reference Java test app that mirrors `test/apps/rust_runner/`. Used
by the Java↔controller integration matrix under `mix test --only java_sdk` and
by the `mix bench --runtime java` regression bench.

The app declares its StreamOperative classes via
`Flezha.createRunner(ns).defineStream(...)` — the integration-matrix core
being:

- `do_nothing` — `DoNothingStreamOperative` + `DoNothingProcedure`
- `store_input` — `StoreInputStreamOperative` + `StoreInputProcedure`
- `compare_filter` — `CompareFilterStreamOperative` + `CompareFilterProcedure`

The factory id list matches `test/apps/rust_runner/`'s `do_nothing` /
`store_input` / `compare_filter` set; the `operative-message-handlers`
factories are absent from both apps until that capability ships.

It also carries authoring-sugar twins (`@Procedure` / `@State` / `@StreamOperative`, generated
by `flezha-sdk-processor`): `SugarDoNothing`, `SugarStore`, and the 2-procedure
`SugarEntry` → `SugarDownstream` reference operative (`SugarStaticEntityStreamOperative`).
`SugarTwinTest` asserts they register and process input identically to the
hand-written equivalents. See `docs/sdk/authoring_contract.md`.

## Build

```sh
mvn -f test/apps/java_runner/pom.xml package
java -jar test/apps/java_runner/target/java_runner.jar my_namespace
```
