# Live Entity read (`entity-state-live-query`)

- **`flz state get <operative/procedure[/entity_key]>`** — the CURRENT
  in-memory state of one live Entity (or one Procedure's Entities) as JSON,
  plus the `change_seq` it was read at. `CliManager.state_get/3` →
  `OperativeManager.get_entity_state/3` → Overwatch → the EXISTING `EntityState`
  control round-trip.
- **`EntityState` was EXTENDED, not duplicated.** Two optional proto strings
  (`procedure = 2`, `entity_key = 3`; empty = absent, so a selector-less command
  encodes byte-identically and the migration/handover caller is untouched). No
  new command, no new payload-id, no second serializer. **Why the SDK produces
  the JSON at all: the controller CANNOT decode durable state** — WAL bytes are
  postcard (Rust) / Kryo (Java) and it stores them opaquely, the same property
  that makes `EffectRecordBody.changes` SDK-owned. `EntityState` is the
  SDK-produced readable answer that already existed.
- **The seq rides the FRAME METADATA (`flz.change_seq`), not the JSON body** —
  the body's shape is specified and already has a caller. Plumbing: a default
  `CommandAction::reply` (core-side, `flz_core/src/command/` — the Java
  `CommandHandler` is gone; Java handles activations, not control commands)
  returning
  `(payload, metadata)` and forwarding to `handle` with no metadata, so ONE
  handler overrides it and the other ~19 are untouched; `ControlChannel` now
  passes the frame metadata to `ControlUnit`, and `send_control_await` takes
  `opts` (`:timeout`, `:metadata`) instead of a positional timeout.
- **A read is NOT a compaction.** It takes the state lock, serializes, releases —
  it does NOT drain pending changes, so the value can be ahead of the WAL (that
  is the point, and why the seq is stamped). Compaction must drain because its
  `snapshot_seq` becomes a truncation point. Pinned by
  `a_read_leaves_the_entity_undrained_and_its_seq_unchanged` (Rust — the Java
  twin went with the kernel mirror; nearest Java coverage is
  `ActivationDispatcherTest.restoreSnapshotAndLiveRead`), which drops the
  flusher registration so "unflushed" is a fact of
  the test rather than a race against the 5 ms linger.
- **The old Java keyset trap is GONE with the kernel mirror**: Java once kept
  `entityKeyToIdx` beside a slot list the reaper did not prune; today ONE
  shared `flz_core::DynamicEntityRegistry` serves both SDKs and `reap_sweep`
  removes the whole entry, so there is no stale-slot walk to fall into.
- **`Keyset` (one Rust enum in `flz_core/src/command/entity_state.rs`, serving
  both SDKs) is the one place "what Entities does this
  Procedure have right now" is answered** — declared keys for static, the live
  registry/key-index for dynamic — and it answers BOTH a single-key lookup and a
  walk-all. That is load-bearing, not tidiness: materializing the keyset to find
  one Entity would make a narrowed read cost a whole walk, which is the entire
  point of the selector. The host/Java restore path (`apply_state_locked`)
  runs on the same seam.
- **Eviction is a distinct diagnosis, so it needs a trace.** Dynamic eviction is
  destructive and leaves nothing on disk, so "idle-evicted" vs "never existed"
  cannot be told apart after the fact — the shared core keeps ONE capped
  (1024) FIFO of evicted keys (`EVICTED_TRACE_CAP`, `flz_core/src/dynamic.rs`)
  purely to answer that refusal. Beyond the cap it degrades to
  `entity_not_found`, the safe direction.
- **Refusal tokens lead the error string** (`entity_key_without_procedure`,
  `procedure_not_found`, `entity_not_found`, `entity_evicted`) so the controller
  maps them without parsing prose; the controller adds `{:not_resident,
  :suspended | :unbound}` and the `{:oversized_state, bytes}` ceiling
  (`:wal_high_watermark_bytes`, no new config key). Every non-resident refusal
  names `flz state at` — there is deliberately NO durable fallback.
- **Deleted in passing**: `ControlUnit.handle_call({:get_entity_state, id})` —
  dead since the Overwatch started going through `send_control_await`, and a
  second, now-wrong way to ask the same question.
- **Tests**: `sdk/rust/tests/state_serialize.rs` (selector narrowing, dynamic
  coverage, refusals, unflushed-change seq, no-side-effects, the proto
  byte-identity pin, and the cross-language JSON fixture — the Java
  `StreamOperativeStateSerializerTest` went with the kernel mirror),
  `test/flezha/service/cli_manager_state_get_test.exs` (every verdict as the
  operator sees it), e2e `dynamic_entity_e2e_test.exs` "a live dynamic Entity is
  readable by name, narrowed to one key" on BOTH runtimes. GOTCHA: those two e2e
  cases take a namespace PER RUNTIME — placement is HRW over every runner in the
  namespace, so a peer case's runner (which lacks the factory) must not be a
  candidate.
- **Docs/examples**: `docs/internals/state_persistence.md` (the capability page —
  selector, freshness contract, refusal table, the programmatic seam a later
  HTTP surface consumes), `docs/sdk/protocol_spec.md` (`EntityState` selector +
  metadata + wire refusal tokens), `docs/internals/state_persistence.md`
  "Live read", `docs/glossary.md` "Live read vs time-travel read", root +
  `sdk/rust/CHANGELOG.md`. Examples: `sdk/rust/examples/live_state_read.rs`,
  Java `E11_LiveStateRead` (both declare a `.key_by` Operative and show the
  exact `flz state get` invocations and refusals against it).

