# One plugin-sink method: `sink` + `sealed` (`unify-plugin-sink-api`)

- **`sink(name)` / `sink_with(name, f)` (Rust) — `sink(name[, serializer])`
  (Java)** is the ONE egress method, always returning the stream handle.
  `output` / `also_output` / `also_output_with` / `alsoOutput` are DELETED, no
  shim. `.sealed()` is the optional terminator returning the non-chainable
  `Channel<T>`.
- **Terminality is DERIVED, not declared.** `build_topology` (Rust) /
  `StreamOperativeTopologyBuilder.build` (Java) sets
  `kind = PluginBound` when a channel has a sink and NO consumers, else leaves
  it `InterProcedure`. That is what lets one method serve both shapes, and it
  fixed a latent hole: `s.output("x"); s.into::<B>();` froze the kind first, so
  B silently received nothing — the same pair is hybrid now.
- **`ChannelDescriptor` lost `plugin_name` (never read by anyone) and
  `plugin_serializer`.** `plugin_sink` is the ONE representation of egress in
  both SDKs; the terminal and hybrid emit paths are now literally one function
  (`EntityEmitSink::emit`; Java `doEmit`'s single non-ENTRY arm), so their
  frames cannot drift.
- **The `EmitSink` trait collapsed to ONE `emit(value, meta, key)`** —
  `emit_plugin` / `emit_inter_procedure{,_with_meta}` / `is_plugin_bound` are
  gone (5 impls updated). `has_plugin_sink()` survives only because `route`
  needs it for the metadata refusal, and `route` asks ONLY when metadata is
  present, so an ordinary emit pays no topology lookup.
  `WorkflowCollectAdapter` keeps a PRIVATE `is_plugin_bound` — the one place
  the shape still matters: a plugin-bound channel must not `collect` into the
  next stage's handoff batch.
- **GOTCHA — `sealed()` must NOT touch the builder.** An early version called
  `with_channel`, which runs `ensure_output_channel`, so sealing a bare leaf
  node CREATED the phantom output channel the lazy creation exists to avoid.
  `Channel<T>` is now a zero-state marker in both SDKs (Rust lost `pub id`,
  Java lost `id()` / `payloadType()`), so "sealing changes no topology" is true
  by construction. Pinned by `sealing_a_leaf_declares_no_channel` /
  `sealingALeafDeclaresNoChannel`.
- **GOTCHA — `.on_full(...)` after `sink(...)` now COMPILES** (it could not
  after `output`, which returned `Channel<T>`). It is still never consulted on
  a channel with no mailbox; `.sealed()` is what refuses it, and the trybuild
  fixture moved accordingly (`sealed_disallows_on_full.rs`).
- **GOTCHA — the trybuild wiring refusal is E0107, not "method not found".**
  `Channel<T>` picks up `Into::into` from the std blanket impl, so
  `.sealed().into::<Next>()` complains about the turbofish instead. Documented
  in `sealed_disallows_wiring.rs`; the clean E0599 shape is in the sibling.
- **GOTCHA — `tests/ui/*.rs` are NOT formatted by `cargo fmt`.** They are
  trybuild data files, unreachable from any target, so they keep the compact
  house style. Running rustfmt on them by hand renumbers every line and
  invalidates the `.stderr` snapshots.
- **KNOWN, NOT FIXED — the `sink(name)` argument is inert at runtime in BOTH
  SDKs.** The frame's routing key is the CHANNEL name (`channel: String`
  metadata, from `descriptor.name`); `PluginSink::name` / `pluginSink().name()`
  is never read on either emit path. `.named("out").sink("out")` is the common
  idiom for that reason. Pre-existing (the removed `output(name)` set the
  equally-unread `plugin_name`), and the spec for this change keeps the
  parameter — but it is the obvious next cleanup.
- **Tests**: `sdk/rust/tests/hybrid_channel.rs` gained
  `terminality_is_derived_from_the_wiring_and_sealed_changes_nothing`,
  `terminal_and_hybrid_sink_frames_match_the_pre_rename_fixture` (the
  wire fixture: body + `{"channel": name}` and nothing else) and
  `sealing_a_leaf_declares_no_channel`; Java mirrors the first and last in
  `StreamOperativeTopologyBuilderTest$GraphAuthoring`, and
  `OldApiNotImportableTest` gained in-process javac cases for the removed
  spellings — with a POSITIVE CONTROL, because a classpath that resolved
  nothing would make every refusal vacuously true.
- **Docs**: `docs/sdk_naming_conventions.md` §9 (rule + migration table),
  `docs/operative_architecture.md` "Plugin egress: `sink`, hybrid and terminal"
  (renamed section — `docs/sdk_protocol_spec.md` links it by name), both
  READMEs, `sdk/rust/public-api.txt`, root + `sdk/rust/CHANGELOG.md`.

