# Amortised effectively-once output confirmation (`amortise-effectively-once-output-confirmation`)

Read before touching the effectively-once outbox dispatch, the settlement /
confirmation reports, or a sink's batch-completion point.

Both parts are implemented: part 1 batches the reports (always on, no semantic
change), part 2 adds the `outputs: :at_checkpoint` opt-in that batches the
records too.

## The rule: the reports are batched, never the facts

Per-entry completion is untouched. An entry still completes when every output
has settled and every hold has answered; the cursor still advances over the
leading run of completed entries; a report naming an entry a resettle
superseded is still dropped per key. What changed is only how many messages
carry those facts:

| per output, before | per dispatch pass / sink batch, now |
|---|---|
| one `{:io_out, payload, metadata, ack}` cast | one `{:io_out_batch, outputs}` cast |
| one `{:outbox_settled, key, holds}` report | one `{:outbox_settled_many, [{key, holds}]}` |
| one `{:outbox_confirmed, key}` report | one `{:outbox_confirmed_many, keys}` |
| one `InputLog.record_consumed/3` per entry | one per window, at its furthest offset |

`Outbox.dispatch/3` became `stamp/2` + `dispatch/2`: the caller stamps its
outputs and hands the whole pass over in one cast. There is no single-output
dispatch left — the 4-tuple `:io_out` is gone, and the 3-tuple stays for the
plain (untokened) `StreamChannel` path.

## Gotchas

- **`Outbox.run/2` has no `try/after`, and now that rule is per BATCH.** A chain
  that crashes mid-batch settles nothing — not its own entry and not the
  entries already run in that batch. That is deliberate: the resettle only
  re-dispatches entries with `pending > 0`, so settling a crashed batch would
  wedge every entry it had reached. The tick re-drives all of them under
  identical identity.
- **Registration now precedes the cast by construction.** The window registers
  every entry, then one cast leaves. The old per-output code relied on mailbox
  ordering for the same guarantee.
- **The resettle sweep folds over `outbox_awaiting`, a map** — its order is
  term order only while the map is a flatmap (≤ 32 keys). A test asserting on a
  re-driven batch must sort it, never assume seq order.
- **`confirm_many/1` groups by owner; `run/2` shares that grouping.** One Kafka
  `ProducerWorker` is per (plugin_id, topic, partition), so one acked batch
  legitimately spans several Entities — several `WalWriter`s. A dispatch batch
  is single-owner by construction, but `run/2` uses the same
  `report_per_owner/2` so it cannot be wrong if that ever changes.
- **A park stays per record and answers through `confirm/1`.** It carries a
  payload, so there is nothing to batch. Anything asserting on confirmations
  must accept BOTH report shapes — `TestHelpers.assert_confirmed/2` does.
- **The consumed-offset advance is per window, folded to the furthest offset;
  `-1` means the window acknowledged nothing.** Checkpoint-ack mid-interval
  records carry no offset, so a whole window of them must not touch the
  watermark. This is a second fold on top of `InputLog`'s own
  `collect_consumed/2` and it is not duplication: this one removes the per-record
  `:ets.insert` in the writer, the log's removes the queued casts on receipt.
- **`DurabilityTier.on_effect_flushed/2` stays PER ENTRY.** It is the
  interval-cut signal for the remote tier, not a watermark — folding it would
  drop cuts.
- **The token names the ENTRY, not the output.** A stream writer's owner is also
  its target, so one `Outbox.token/2` covers every output of an entry and
  `entry_outputs/3` mints it once; `stamp/2` honours a supplied `:ack`
  (`Map.put_new_lazy`). `ProcJournal` cannot hoist it — its `target` is
  `item.entity_id`, which varies per outbox item.
- **Head-of-line: one batch is one uninterruptible run of chains in the
  Overwatch.** Ingest landing mid-dispatch used to be able to interleave between
  per-output casts. Measured: no tail growth at a 2 500 rec/s/partition pace
  (p99 38→44 ms identity, 85→89 ms keyed), so the trade is paid for.

## Part 2: the interval owns its outputs

`checkpoint_ack()` gained two options, in both SDKs:

- `checkpoint_every(n)` / `checkpointEvery` — close the interval after `n`
  inputs, so the body no longer has to count for itself. An explicit
  `ctx.checkpoint()` still closes it early.
- `outputs_at_checkpoint(max_delay_ms)` / `outputsAtCheckpoint` — hold the
  interval's outputs with its changes and ship them inside its checkpoint
  record. One WAL append per interval instead of one per invocation.

### Gotchas

- **The delay is a PARAMETER of the option, not an option beside it.** There is
  no way to spell "hold outputs, unbounded" — `outputs_at_checkpoint` takes the
  delay, so the refusal is structural rather than a validation message. The
  FFI's JSON declaration cannot express it that way, so there the refusal IS a
  validation: `checkpoint_outputs: at_checkpoint` without
  `checkpoint_max_delay_ms` is rejected by name.
- **`StagedInterval::fold` is the ONE place the ack granularity's rules live.**
  Before this change the typed dispatcher and the C-ABI host each carried their
  own copy of the checkpoint branch; they are now one generic core
  (`C = S::Change` typed, `C = Vec<u8>` bytes), so the two cannot drift. Same
  for the shipped-record mapping: `EffectRecordBody::from_shipped` and
  `EffectEntry::from_shipped` are the only places those fields are spelled out.
- **A deadline close must not reach author code.** It rides a new
  `EntityItem::CloseInterval` + `Wake::CloseInterval`, NOT a `TimerKind` — that
  is a wire enum the host sees, and a synthetic timer would have invoked
  `on_timer`. The close only ships an accumulation that is already computed.
- **The close lands in mailbox ORDER.** `assemble_batch` consumes it: at the
  head of a batch it closes immediately, behind live items it is deferred and
  ends the batch (`Staged::BatchClosed`), because an input queued ahead of it
  belongs to the interval it closes — and anything popped after it belongs to
  the next one. It therefore never reaches the host or the typed
  `dispatch_item`; the arms there are unreachable-path benign values.
- **A spent deadline entry is left armed.** When an interval closes early the
  wake for its old deadline stays on the list, comes due, finds nothing open
  (`close_on_deadline` returns `None`) and does nothing. Tracking the previous
  deadline to remove it would buy nothing; `discard_entity` still cleans up on
  eviction.
- **`checkpoint_every` moved the bench fixture's counter into the SDK.**
  `bench_checkpoint` used to count inputs and call `ctx.checkpoint()` itself;
  it now declares the cadence, which is what the bench measures.
- **The FFI decl is additive, but only forward.** `HostClassDecl` is serde JSON
  with `deny_unknown_fields`, so a NEW shim against an OLD core is a hard
  refusal. Both SDKs therefore omit every new key at its default, exactly as
  `sync_durability` did.

## Measured (task 1.6)

`bench.sh --only flezha --repeat 3 -p 4 -m 15000 --latency 2500 --workloads
identity,keyed --durabilities on`, medians of 3, **zero loss and zero
duplicates on all 24 runs**:

| cell | before (`a69aabbc`) | after part 1 | delta |
|---|---|---|---|
| identity / EO throughput | 84 857 rec/s | **97 008** | +14 % |
| keyed / EO throughput | 24 090 rec/s | 24 429 | +1.4 % (noise) |
| identity / EO paced p50 / p99 | 10.3 / 38.2 ms | 11.3 / 43.7 ms | ~same |
| keyed / EO paced p50 / p99 | 22.2 / 85.4 ms | 23.1 / 89.2 ms | ~same |

**Batching the reports is not the keyed/EO ceiling.** It shows on the identity
tier, where the rest of the per-record path is cheap; keyed/EO did not move,
because its cost is the per-record WAL append and the synchronous ingest chain,
not the report traffic. The keyed/EO throughput lever is part 2 — one WAL append
per interval.

## Measured (task 2.5) — the opt-in

Same command with `--workloads keyed --durabilities checkpoint`, medians of 3,
zero loss, `max_delay_ms: 50`, `checkpoint_every: 500`:

| cell | `as_produced` (control) | `at_checkpoint` | delta |
|---|---|---|---|
| keyed / checkpoint throughput | 20 895 rec/s ±28 % | **45 009** ±2 % | **+115 %** |
| unpaced p50 / p99 | 1894.6 / 2047.6 ms | 567.6 / 866.9 ms | **2.4× better** |
| paced p50 | 22.3 ms | **70.7 ms** | **+48.4 ms** |
| paced p99 | 71.8 ms | 89.9 ms | +18.1 ms |

**This is the trade, stated plainly.** At a paced 2 500 rec/s/partition the
interval closes on the 50 ms delay (500 inputs would take ~200 ms), so an
output waits a uniform 0–50 ms and p50 rises by ~48 ms — almost exactly the
declared window. Under saturation the sign flips: throughput more than doubles,
the backlog drains faster, and latency ends up 2.4× BETTER than the control.
Batching the WAL *records* is where the keyed/EO win is; batching the messages
(part 1) was not.

**MEASUREMENT TRAP that invalidated a first attempt at this table.**
`Fleet.build_command/3` prefers `target/release/rust_runner` and only warns when
it falls back to a *debug* binary — a STALE release binary is used silently. The
first 2.5 sweep ran an SDK binary built before part 2 existed in BOTH arms, so
it measured nothing (it reported a spurious +15 %). Any bench touching SDK code
must `cargo build -p rust_runner --release` first. The tell is the paced p50: if
`at_checkpoint` is really holding, p50 must rise by roughly `max_delay_ms / 2`.
A 2 s-delay probe is the cheap direct check — first-out should be ~2 s.

**Power state dominates this box.** The same code measured 54 K identity /
17.4 K keyed on battery and 97 K / 24.4 K plugged in. Any bench comparison must
hold the power state fixed; the recorded reports in `bench/flink/reports/` do
not record it.

## Tests

`test/flezha/state/wal_writer_outbox_test.exs` — the four batch-report tests are
under `---------- batched reports ----------`: one settlement + one confirmation
report complete a ten-entry batch, an out-of-order batch report cannot pass an
unconfirmed entry, a crashed batch run settles none of its entries and the
resettle re-drives them, and a report naming entries the writer no longer owes
changes nothing. Also `proc_journal_test`, `control_channel_proc_commit_test`,
`producer_worker_test`, `postgres_sink_test` (needs `POSTGRES_INTEGRATION=1`),
`operative_forward_test` (still per-record `confirm/1` — the forward sink runs
one delivery in flight, so it has no batch to confirm).

Part 2: `flz_core/src/flusher.rs` `staged_interval_tests` (the fold's rules —
no output before the checkpoint, one ordered batch at the close, the quiet-Entity
delay, a park closing the interval, a restore discarding it),
`flz_core/src/host.rs` `checkpoint_interval_decl_tests` (the FFI declaration and
its refusals), `sdk/rust/src/operative/builder.rs` (the typed declaration),
`HostClassDeclarationTest` (the Java declaration, and that a bare
`checkpointAck()` still emits no new keys), `wal_writer_outbox_test`
("a checkpoint record's outputs flush as one batch with sequential ordinals" and
the identical-identity re-drive), and
`test/system/checkpoint_batched_outputs_e2e_test.exs` (`:system` — the three
declared behaviours against a live runner).

The dispatch-batch test vocabulary lives in ONE place,
`Flezha.State.TestHelpers`: `next_output/1` (the outputs the chain was handed,
in order, across however many batches the fsync windows cut), `assert_batch/1`
(the whole batch — for tests about the batching itself), `refute_dispatched/1`,
`run_batch/2` (stand in for the Overwatch's handler), `metadata_of/1`,
`in_one_window/1` (freeze the 5 ms batcher so appends land in one window) and
`assert_confirmed/2`.
