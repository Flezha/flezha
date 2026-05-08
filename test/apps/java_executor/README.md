# Java Compute Engine

Reference Java test app that mirrors `test/apps/rust_executor/`. Used
by the Java↔controller integration matrix under `mix test --only java_sdk` and
by the `mix bench --runtime java` regression bench.

The app registers three Relay classes via `Flezha.registerRelay(...)`:

- `do_nothing` — `DoNothingRelay` + `DoNothingShard`
- `store_input` — `StoreInputRelay` + `StoreInputShard`
- `compare_filter` — `CompareFilterRelay` + `CompareFilterShard`

The factory id list matches `test/apps/rust_executor/`'s `do_nothing` /
`store_input` / `compare_filter` set; the `relay-message-handlers`
factories are absent from both apps until that capability ships.

## Build

```sh
mvn -f test/apps/java_executor/pom.xml package
java -jar test/apps/java_executor/target/java_executor.jar my_namespace
```
