# Schema evolution (`state-evolution`)

- **The gap was not the version — it was everything behind it.**
  `Procedure::SCHEMA_VERSION` was stamped on every record and compared on
  change records only, on the Rust typed path only. Reproduced at
  `init_entity_with_payload` before any code changed: a v1 snapshot
  `{items: 7, coupons: 2}` restored into a v2 struct declaring the same two
  fields in the other order came back `{coupons: 7, items: 2}`, `Ok`, silent —
  **even with `SCHEMA_VERSION` bumped**, because the snapshot was decoded with
  no header at all. `validate_header_for_state` had zero callers and
  `Restore.assemble_payload/1` read the on-disk header only to drop it
  (`{:ok, _header, bytes}`). Compaction is what turns guarded change records
  into an unguarded snapshot, so the longer an Entity lives the less of it was
  covered.
- **One decision, in the core.** `flz_core::header::decide_schema/3` +
  `resolve_header_for_*` is the only place a stamped version is resolved:
  `== running` decode, `[floor, running)` migrate, below-floor and
  above-running refuse (`SchemaTooOld` / `SchemaFromTheFuture`, replacing
  `SchemaMismatch`, which could not say which direction the mismatch ran).
  Both folds reach it — the typed one and `host::restore_fold_exclusive` — so
  Java and every future FFI SDK inherit the guard instead of reimplementing it.
  The host crossing carries `schema::RestoreBody { bytes, from_version }`: a
  host only ever sees a body to decode at its own version or migrate from a
  named one, and a refusal never crosses.
- **The resolvers claim the format the running procedure declares**, not
  `FORMAT_ID_POSTCARD` — the old `validate_common` hardcoded postcard, which
  would have refused every Kryo record the moment the host path started
  checking.
- **Per record, not per restore.** A WAL written across a deploy holds v1
  records under v2 records; the arm is chosen per record, and an effect
  record's enclosed change bodies inherit the enclosing record's version (one
  invocation wrote them).
- **Migrating is paid once.** `RestoreInitStateResponse.migrated` → the
  controller drives the `CompactEntity` it already drives on size/age. That
  reply only exists if someone awaits it: the ordinary stream bind restore was
  fire-and-forget (`replay.ex` `spawn` + `dispatch_restore_init_state`), so it
  now awaits inside the same spawned process, over a bounded
  `Task.async_stream` (`:bind_restore_concurrency`, default 16) rather than
  serialising a large rebind.
- **`schema_version` moved to the per-Procedure declaration.**
  `HostClassDeclaration.base` collapsed a Java class to `max(schemaVersion)`,
  re-stamping every sibling Procedure with a version it never declared. Both
  keys (`schema_version`, `min_readable_schema_version`) now ride
  `procedures[]`; `test/fixtures/host_class_decl_keys.txt` pins them and the
  `flz_core` side asserts exact key-set equality (the Java side asserts only
  that it emits every pinned key — a stale class-level key fails at the core's
  `deny_unknown_fields`, not in the Java test).
- **The activation schema version bumped 2 → 3** (`flz_core::schema` +
  `FlzCore.SCHEMA_VERSION`, negotiated at context creation) because
  `RestoreFold` changed shape. The committed dist artifacts had to be rebuilt
  (`scripts/build-dist.sh --host`) — five native triples + both jars — or a
  Java runner negotiates against a stale core.
- **The Java host never re-materialized an operative instance.**
  `instances.computeIfAbsent(operativeId, …)` kept the class the id was FIRST
  seen with, so an id rebound to a different factory — a deploy — kept running
  the old class; the conformance fixture surfaced it as "declares no state
  migration" from a procedure that declares one. Now keyed on the activation's
  factory. Still open and pre-existing: nothing ever removes an instance (no
  removal signal crosses the ABI), so the map grows for the process's life.
- **One walk over a change log.** The typed fold and the host fold were two
  copies of it — and only the typed one verified the record framing CRC, so a
  corrupt record folded silently on Java and every FFI host. Both are now sinks
  over `flz_core::restore::fold_change_log` (`ChangeLogSink`), which owns the
  framing, the CRC, the header resolution and the effect-record unwrapping. The
  cursors walk (`restore_cursors`) deliberately stays separate: it is advisory
  and breaks on a malformed record where the fold must fail.
- **The e2e's first input needs a residency gate.** `start_operative` returns
  when the operative is PLACED; its Entity materializes on the runner a moment
  later and an input sent into that gap reaches nothing. A live state read is
  the readiness signal (it answers only once the Entity exists). This is the
  same race behind `FlezhaTest` "runner disconnect migrate proc", a documented
  member of the load-sensitive flake population — it sends into that gap with a
  2 s `send_sync_io` deadline.
- **A deploy in a test is `stop_operative(preserve_state: true)`** — the plain
  stop is a hard teardown that deletes the state dir, which is why the first
  e2e restored a cold Entity. The `preserve_state` option already existed for a
  rescale handing files to another member.
- **Rust re-exports its codec** as `flezha_runner::state::postcard`: a
  migration must decode last version's bytes with the codec that wrote them,
  and a separately-pinned postcard in the author's crate is not that guarantee.
  Java authors construct a `KryoCodec` over the OLD version's type list in the
  OLD order (ids are positional).
- **The Elixir SDK carries none of this** — it persists nothing in v1, stamps
  no headers and has no restore fold, so `schema_evolution` is deferred in its
  conformance manifest. It is owed by Elixir SDK persistence, not by this
  change.
- **Not here:** rolling upgrade (version-aware placement, canary/blue-green) is
  `state-rolling-upgrade`, deferred by name — it extends the
  `placeable_runners/3` filter `guard-cross-sdk-placement` is rewriting. And a
  refused restore still leaves the Entity cold rather than failing the bind:
  `_ = Commands.await_restore_init_state(...)` at `replay.ex` is that change's
  to fix. The e2e asserts only what holds either way (v1 must not read v2's
  value).
