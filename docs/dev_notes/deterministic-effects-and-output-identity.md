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
  (`BuildTopologyError::UnservableClock`).
- **`ctx.stable_id()`**: frozen FNV-1a-128 over
  `entity identity ‖ change_seq ‖ per-invocation ordinal`.
- **Keyed channels**: `.keyed()` (plugin wire required, else
  `KeyedChannelWithoutPluginWire`) + `ctx.emit_keyed(key, value)`; plugin
  frames stamped `flz.entity` (`instance/procedure/entity_key`),
  `flz.seq` (`<change_seq>.<emit_ordinal>`, read after flush-before-emit),
  `flz.key`. User `flz.*` metadata rejected (`ReservedMetadataKey`).
  Non-keyed channels byte-identical (guarded by test + criterion bench).
  `flz.seq` is NOT an exact append-dedup handle until
  `effectively-once-output` journals it; workflow operatives reject keyed
  emits (their plugin wire carries no metadata). Controller untouched —
  stamps pass through `StreamChannel` → `Overwatch` `:on_output`.
- **Downstream (not this change)**: flezha_market migration to the ctx
  APIs; Java SDK ctx parity; Kafka producer adopting `flz.key` as message
  key.
- **Docs**: `docs/sdk_protocol_spec.md` "Output identity stamps",
  `docs/resilience/data_durability.md` "Output identity".

