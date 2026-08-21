# close-silent-data-loss-defaults — implementation notes

Three drop-by-default sites and one recovery latch that, together, let an
at-least-once operative lose a double-digit share of its input with every
health surface green. Nothing new was built: two defaults inverted, one latch
became a rate-limited retry, one counter became readable, two inert options
were deleted.

## Load-bearing decisions

- **`Block` is the default for every channel kind, in both SDKs and the FFI
  host.** `default_policy_for_kind` (`sdk/rust/src/channel/builder.rs`),
  `defaultPolicyForKind` (`StreamOperativeTopologyBuilder.java`) and the
  serde `#[default]` on `HostOverflow` (`flz_core/src/host.rs`) all say
  `Block`. The three must stay in lockstep — a Java operative that drops where
  a Rust one blocks is exactly the defect this change closes. The co-tenancy
  cost is real and documented: a blocked entry parks the shared wire reader,
  so siblings on the same connection wait. An operative that prefers to shed
  declares `.on_full(DropNewest)`; the in-tree fixture that wants shedding
  (`bounded_mailbox_drop_newest`) now says so.
- **The Kafka default is `:block` for every record.** `overflow_strategy/2`
  (the per-record derivation from `Outbox.guaranteed?/1`) is gone;
  `admit_record/3` reads `state.queue_overflow`, which `init/1` resolves with
  `|| :block` so an explicit `nil` (the provider passes one through
  `coerce_atom/1` when the operator set nothing) means "unset". The
  alarm-grade log for a dropped guaranteed record is still specific to
  guaranteed records; the default is not.
- **The baseline pull reuses the `:seq_gap` rate limiter.** `await_baseline/3`
  and the gap branch of `do_append_async/6` both call `resync_rate_limited/3`
  (one `last_resync_at` field, one `:wal_resync_retry_interval_ms` knob,
  default 1 s). `establish_baseline/2` resets `last_resync_at` so a healed
  episode ends cleanly. `shed/3`'s `shedding?` latch is deliberately NOT a
  retry: `append_async` clears it on any successful append, so relieved
  back-pressure re-arms it without the resync having to succeed — the
  baseline pull has no such clearing append, which is why it needed the
  interval.
- **`dropped_total` is worker state, not a telemetry reduction.**
  `record_drop/3` (the old `drop_telemetry/3`) returns the state with the
  counter bumped; both drop clauses of `overflow/4` thread it. `stats/1` now
  matches the spec shape `test/plugin/kafka/integration_test.exs:92` was
  written against.
- **Removed, not implemented:** `:partitioner` (carried, never read) and the
  documented `kafka_msg_partition` metadata override (never existed). One
  plugin instance serves one `(topic, partition)`; partition routing arrives
  as its own change if wanted.

## Gotchas

- The main spec had a requirement whose premise was the drop default
  ("Mailbox-full handling does not stall sibling Operatives at the entry edge
  under default policy") and two Kafka requirements whose text presumed the
  derived default. The delta REMOVES/ADDS the first and MODIFIES the other
  two; a spec that still promised a non-parking reader by default would have
  been a documented lie.
- `sdk/rust/tests/operative_dispatch.rs` needs `--features bench-internals`
  (`cargo test -p flezha_runner --features bench-internals --test
  operative_dispatch`). The two entry-edge `Block` scenarios share one body,
  `entry_block_parks_reader_until_pool_drains::<A>(tag)`; the declared
  `DropNewest` scenario keeps the old non-parking assertion.
- No startup log line for the flip: "now blocks where it previously dropped"
  is not knowable after the flip, so the line would fire for every operative
  forever. `pushes_blocked_total` rising where `drops_newest_total` used to is
  the observable.
- `:queue_block_timeout_ms` applies only to synchronous `enqueue/4` callers;
  a cast-admitted record waits in the `blocked` queue with no timeout and is
  admitted when a batch drains.

## Test map

- `test/flezha/state/wal_writer_test.exs` "awaiting baseline" — gap flood
  pulls once per interval; a failed pull is retried after the interval; a
  healed Entity stops pulling. The describe sets
  `:wal_resync_retry_interval_ms` to 200 ms.
- `test/plugin/kafka/producer_worker_test.exs` — `cast_path_waits_for_room/1`
  runs for `:block` and for `nil`; the drop-oldest/drop-newest cast tests
  assert `dropped_total`.
- `sdk/rust/tests/operative_dispatch.rs` — `default_entry_policy_blocks_and_loses_nothing`,
  `declared_entry_block_parks_the_reader_until_the_pool_drains`,
  `declared_drop_newest_entry_does_not_stall_sibling_operatives`.
- `BoundedMailboxTopologyTest` (Java) — default BLOCK on both kinds; entry
  `onFull(DROP_NEWEST)` overrides.
- Kafka integration (`KAFKA_INTEGRATION=1 … --include kafka_integration`)
  against the bench broker: `KAFKA_BROKER_HOST=localhost KAFKA_BROKER_PORT=39092
  KAFKA_COMPOSE_FILE=bench/flink/compose.yml`.
