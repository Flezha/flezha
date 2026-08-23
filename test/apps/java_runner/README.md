# Java Compute Engine

Reference Java test app that mirrors `test/apps/rust_runner/`. Used
by the shared conformance suite under `FLEZHA_E2E_RUNTIME=java mix test
--include system` and by the `mix bench --runtime java` regression bench. The
factory ids it registers, the transports it supports and how it is launched are
declared in `conformance.json` beside this file — see
`docs/sdk/conformance.md`.

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
