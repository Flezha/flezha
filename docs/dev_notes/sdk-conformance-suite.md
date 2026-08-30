# sdk-conformance-suite — implementation notes

The runtime became an axis of the *run*, not of the file. What that cost, and
what is load-bearing about how it landed.

## The manifest reader lives in `PortPilot`, not in the test helper

The obvious home for "read `conformance.json`" was `NodeTestHelper` — it already
owned runtime selection. It could not be: `Mix.Tasks.Bench.Fleet` needs the same
reader, and `lib/` cannot depend on `test/helpers/`. So the reader
(`runtimes/0`, `manifest/1`, `manifest_path/1`, `launch_command/3`) sits on
`Flezha.Service.PortPilot`, beside `open_port/2` — the module that already owned
"how an external runner process is started". `NodeTestHelper` keeps the
*vocabulary* (capabilities, required set, `runtime/0`, `deferred/1`,
`transports/1`, `fixtures/1`), because those are test concepts. Both former
`build_command/3` clause sets are deleted; the drift they had already grown
(`dist/java_runner.jar` in the helper vs `target/java_runner.jar` in the bench)
is now impossible — the two spawners differ only in the profile they render.

## The `test` profile's artifact for Rust is `Cargo.toml`

Not a binary. The e2e harness launches `cargo run --manifest-path=<artifact>`,
so the artifact whose existence is checked is the manifest file, and `{artifact}`
renders into the `--manifest-path=`. The `bench` profile is the ordinary
"first existing binary wins, a debug one warns" list — that is where the old
`IO.warn` about a debug binary went, generalised into the artifact-list rule.

## Capability tags are per-module, but a per-test override is normal

Three files genuinely span two capabilities (`dlq` also drives the tier's park,
`remote_durability` also drives a workflow, the smoke tier pins declarations of
every capability). A per-test `@tag capability:` is the intended escape, and the
gate's literal check accepts *any* capability the file claims — module tag or
test tag. Without that, the alternative was splitting files by capability, which
would have split scenarios that share a setup.

## The gate's fixture-literal check is what forced the smoke tier's shape

The tier used to bind `store_input` for its routing scenario. `store_input` is a
`state_persistence` fixture, so under an SDK that defers persistence the smoke
tier would have bound a fixture that SDK has no reason to register — and the
promise that "`--include sdk_smoke` completes for an SDK carrying only the
required set" would have been false. It now drives `do_nothing` and
`compare_filter` only, and the gate is what noticed.

## The runtime is scoped to the suite, not to the run

`FLEZHA_E2E_RUNTIME=elixir mix test --include system` runs the whole unit suite
too, and the first honest attempt at that was 20 red tests that had nothing to
do with the Elixir SDK: `FlezhaQuicTest`, `RunnerReconnectBindingTest`,
`SocketRoundtripTest` and friends spawn a runner and then drive *reference*
fixtures, most of which a partial SDK does not register. Making the environment
variable a global default for `start_runner_port/2` — which is what the change
originally specified — turns "this SDK carries 18 of 49 fixtures" into a wall
of failures in files that are not conformance tests at all.

So the selected runtime reaches the spawner through the test process
(`NodeTestHelper.put_spawn_runtime/1`, published by `FlezhaTest.SystemCase`'s
`setup`), and `spawn_runtime/0` falls back to the reference. Opting a module
into the conformance axis is exactly `use FlezhaTest.SystemCase` — the same
line that declares its capability.

## The snapshot interception point

`ClassRegistry.decode_declarations/1` is the one place the controller decodes
`AddOperativeResponse.metadata`, so a Mimic pass-through there sees every
declaration without a debug surface in production code. The stub must
`Mimic.call_original/3` — the bind depends on the decoded map — and the module
is `async: false` with `set_mimic_global_until_exit`, because the decode
happens on the Overwatch process, not the test's.

## `max_entities` had no Java spelling

The point of `test/fixtures/host_class_decl_keys.txt` is to catch a
`HostClassDecl` key one SDK cannot say. On its first run it caught one:
`procedure.max_entities` existed in the core and in the Rust builder, and the
Java SDK had no `maxEntities()`. Closed here (`Procedure.maxEntities/0` +
`maxEntitiesOverridden/0`, `ProcedureDescriptor`, the topology builder's probe,
and the emit in `HostClassDeclaration`), mirroring the Rust default of 100 000
and its override sentinel.

Two things the Rust-side test needed: the decl structs are `Deserialize`-only,
so "serialise and compare keys" was not available without adding `Serialize` to
production types for a test's benefit. Instead the test parses the struct
definitions out of `host.rs` via `include_str!` (exact, and it fails when a field
is added) and separately proves a JSON carrying every pinned key parses — which
`deny_unknown_fields` makes an exact spelling check.

Entity mode splits the procedure keys: a static procedure declares `entity_keys`
and `entities_for_overridden`, a dynamic one `idle_ttl_ms` and `max_entities`.
The Java fixture needs both modes to cover the section; one procedure cannot.

## What the e2e files lost

`runtime:` literals, the `(java)` twin tests and their `require_jar!` guards, and
the `[:rust, :java]` enumerations in `credit_flow` and `workflow_crash_resume`.
The crash-resume file's `cell:` tags were `:rust_uds_*`; they are now `:uds_*`,
because the cell is a transport cell. `test/java_sdk/` is gone: `matrix_test.exs`
became `test/system/sdk_smoke_e2e_test.exs` and `compaction_e2e_test.exs` was a
duplicate of the generic one. `test/system/elixir_runner_e2e_test.exs` is gone
for the same reason — connect/place/dispatch/sink is the smoke tier, retry→park
is `dlq`, partition admission is `partitioned_operatives`, all under
`FLEZHA_E2E_RUNTIME=elixir`.

## Where the fixtures actually diverged

The Java and Elixir test apps did not merely use prefixed ids — they registered
*fewer* fixtures. Java advertised 14 of the reference's 48; Elixir 6. Renaming
was the small half of section 4; writing the missing twins was the large half,
and it is what makes `--include system` meaningful for those runtimes at all.
