# Deterministic effects + output identity (`deterministic-effects-and-output-identity`)

- **Determinism contract** (documented on the `Procedure` rustdoc): `run`
  / `apply_change` / `compensate` obtain time via `ctx.now()` and
  identifiers via `ctx.stable_id()` — no ambient `SystemTime::now()` /
  `Uuid::new_v4()` / external reads. Verified by
  `ProcedureTestHarness::replay_emits_equal` (output-side sibling of
  `replay_equals`; injectable fixed/scripted wall clock).
- **Clocks** (`sdk/rust/src/procedure/clock.rs`): no static
  `Clock::now()` — the dispatcher evaluates the declared clock once per
  invocation via `ClockFor<Input>` (all `Time = u64`). `WallClock` =
  epoch ms at dispatch entry; `EventTimeFrom<F>` = `F:
  TimeExtractor<Input>` over the current input (compile-enforced);
  `LogicalClock` = the raw durable `change_seq` (re-armed `terminal+1` on
  restore). Build rejects `LogicalClock` + `Change = NoChange`
  (`TopologyIssue::UnservableClock`).
- **`ctx.stable_id()`**: frozen FNV-1a-128 over
  `entity identity ‖ change_seq ‖ per-invocation ordinal`.
- **Keyed channels**: `.keyed()` (plugin wire required, else
  `KeyedChannelWithoutPluginWire`) + `ctx.emit_keyed(key, value)`; plugin
  frames stamped `flz.entity` (`instance/procedure/entity_key`),
  `flz.seq` (`<change_seq>.<emit_ordinal>`, read after flush-before-emit),
  `flz.key`. User `flz.*` metadata rejected (`ReservedMetadataKey`).
  Non-keyed channels byte-identical (guarded by test + criterion bench).
  `effectively-once-output` has since journaled `flz.seq`, so
  `(flz.entity, flz.seq, flz.epoch)` IS the exact dedup identity a
  conforming sink deduplicates on (`Wire.output_stamps`). A workflow keyed
  emit is accepted under the effectively-once tier (it rides the effect
  record into the staged outbox); only the plain non-tier workflow plugin
  wire still refuses a key. Controller untouched —
  stamps pass through `StreamChannel` → `Overwatch` `:on_output`.
- **Downstream — since landed**: Java SDK ctx parity (`java-sdk-parity`)
  and the Kafka producer using `flz.key` as the record key (the dedup
  identity moved to the `flezha.dedup_key` header). Still out-of-repo:
  the flezha_market migration to the ctx APIs.
- **Docs**: `docs/sdk/protocol_spec.md` "Output identity stamps",
  `docs/resilience/data_durability.md` "Output identity".

