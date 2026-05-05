# Java Compute Engine

Reference Java test app that mirrors `test/apps/rust_compute_engine/`. Used
by the Java↔agent integration matrix under `mix test --only java_sdk` and
by the `mix bench --runtime java` regression bench.

The app registers three Relay classes via `Flezha.registerRelay(...)`:

- `do_nothing` — `DoNothingRelay` + `DoNothingShard`
- `store_input` — `StoreInputRelay` + `StoreInputShard`
- `compare_filter` — `CompareFilterRelay` + `CompareFilterShard`

The factory id list matches `test/apps/rust_compute_engine/`'s `do_nothing` /
`store_input` / `compare_filter` set; the `processor-message-handlers`
factories are absent from both apps until that capability ships.

## Build

```sh
mvn -f test/apps/java_compute_engine/pom.xml package
java -jar test/apps/java_compute_engine/target/java_compute_engine.jar my_namespace
```
