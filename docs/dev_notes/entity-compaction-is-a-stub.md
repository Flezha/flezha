# Snapshot compaction is now real (`entity-compaction-is-a-stub`)

- **The controller half was always built; both SDKs answered `CompactEntity`
  with a STUB** (`command/compact.rs` → `snapshot_seq: 0`, no frame; Java
  `CompactEntityHandler` → `new byte[0]`). Measured live before the fix: 9
  `compaction_trigger`, 8 `compaction_timeout`, 0 `compaction_completed`, 0
  snapshot files, flat WAL. So the earlier "Java at full parity / persistence
  green" claims did NOT hold for this path on EITHER SDK until this change.
- **SDK emit** = the producing half. Rust adds `ProcedureDispatcher::snapshot_frame`
  (the type-erased MIRROR of `restore_state`, NOT a third `build_state_fns` fn
  pointer — reuses the same postcard machinery restore decodes); Java's
  `CompactEntityHandler` shares the `EntityLocator.findStream` scan with
  `RestoreInitStateHandler` (cx-refine extraction) + Kryo. Both
  DRAIN pending changes first (`flush_one` / new `EntityFlusher.flushEntity/1`)
  so state == shipped WAL, then serialize under the state lock and ship
  `entity_state_snapshot` (new payload-id const in both sinks) stamped with
  `change_seq - 1` = the highest seq the snapshot folds. **GOTCHA — the codec
  is the trap**: postcard (Rust) / Kryo (Java), NOT the JSON migration
  serializer, or restore can't decode it (and compaction being unexercised, it
  would pass review). Java `NativeTransport.sendEntityStateSnapshot` DELETED
  (0 callers, javadoc lied about stamping the payload-id).
- **Seq semantics (load-bearing)**: controller `truncate_up_to(seq)` =
  `rewrite_keep_seq_gt` (keeps `seq > snapshot_seq`), and `establish_baseline`
  sets `last_persisted_seq = snapshot_seq`, so `snapshot_seq` MUST be the
  highest ALREADY-shipped seq (`change_seq - 1` after draining) — stamping
  `change_seq` (the next-to-assign) would dedup-drop the next real record.
- **Controller now inspects `CompactEntityResponse`** (`dispatch_compact_entity`
  → the shared `ControlUnit.send_control_await/3` request/reply off-process so a
  slow snapshot never blocks the Watchdog sweep / WalWriter pull; **falls back to
  a direct `send_control` when no ControlUnit exists** — isolated unit tests).
  cx-refine unified that request/reply seam: `send_control_await` now backs
  `send_operative_message`, the credit-window handshake, Overwatch's private
  `send_control`, and the compaction inspection. `{0,0}` against a
  non-empty WAL (`Restore.has_persisted_state?`) = failed handshake →
  `[:flezha, :state, :compaction_failed]` → `Watchdog.compaction_failed`.
- **The timeout handler was itself a silent no-op** (`Recovery.resync/2` with no
  `runner_id` → `{:error, :no_runner_id}` discarded with `_ =`). Now the
  `compactions` map carries `{started_at, runner_id}`; the Watchdog holds one
  `Convergence.Backoff` keyed `{:compaction, entity_id}` (added ONE tiny
  `attempt/2` accessor — no strike map). Below give-up: record + backoff-gated
  retry, NO disruptive resync of a live Entity. At give-up (attempt ≥ 4):
  `[:flezha, :state, :compaction_gave_up]` alarm + a real resync WITH the
  runner_id. Completion resets the strike.
- **`compact_after_bytes` was unreachable by ANY caller** (`_procedure_id`
  discarded). Now `PersistenceResolver.compact_after_bytes(operative_class,
  metadata)` routes through the `resolve/5` cascade → `ClassRegistry.compact_
  after_bytes` (SDK declaration) → global default; the SDK ships it in
  `AddOperative` metadata (smallest across procedures, only when ≠ 8 MiB
  default — byte-identical otherwise) on BOTH arms; decoded in
  `decode_declarations/1` + persisted via `put_compact_after_bytes`. Watchdog
  now passes the CLASS, not the procedure id.
- **e2e on both arms**: `test/system/compaction_e2e_test.exs` (Rust) +
  `test/java_sdk/compaction_e2e_test.exs` assert a snapshot FILE appears and the
  WAL strictly shrinks against a real runner — the `assert_receive
  compaction_completed` is what fails against a stub. Round-trip unit tests:
  Rust `snapshot_frame_round_trips_through_restore`, Java
  `compactEntityShipsASnapshotThatRoundTripsThroughRestore`. Controller units:
  `watchdog_test` (backoff/give-up), `persistence_resolver_test`,
  `class_registry_test`, `add_operative_response_test`.
- **NOTE**: `snapshot-baseline-durability`'s baseline-seed pull (a cold
  controller pulls a full-state snapshot via `CompactEntity`) is now genuinely
  reachable for the first time — its `dispatch_resync` empty-WAL branch was
  inert while the SDK stubbed. See that change's spec.

