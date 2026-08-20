# durable-path-throughput — implementation notes

Read before touching the effectively-once outbox, the input-log watermark, the
fsync tick, or the Kafka producer's client sharing. Found by profiling the
Flink round-trip bench (`bench/flink/`, `mix bench.roundtrip --profile` /
`--tprof`); exactly-once keyed throughput went 1.3K rec/s (batch ingest,
stalling) → 29K rec/s at 4 pipelines, zero loss, with the controller's
Elixir code — never the disk, the transport or the SDK — as the ceiling the
whole way.

Interleaved against Flink 2.2.1 at 4 pipelines (Flezha sink linger 0, Flink
checkpoint 200 ms), per-record ingest: throughput identity 35–37K vs 53–63K,
keyed/state 33K vs 60K, exactly-once 11K vs 56K; p50 at 9.4K/s paced: 11.3 vs
9.6 ms, 9.6 vs 49.5 ms, 37.4 vs 141.5 ms; first record out 0.13–0.25 s vs
0.9–3.5 s. With the (removed) batch ingest the box gave 93K / 42K / 29K at
the default linger — the per-record ceiling is the synchronous ingest chain,
one record in flight per pipeline (`HANDOFF.md`).

## Profile first, and profile the right thing

Peak mailbox depth and cumulative reductions name *who is backed up*, not
*who burns CPU*. Two things were decisive: `:msacc` (schedulers 84% in
emulator — so Elixir code, not NIF/GC/port) and a 2 s `tprof` call-time window
over every process, which ranked functions. `:eprof` cannot trace `all` and
races process churn on a pid list; `tprof` does `all` natively. The
per-thread `/proc/<pid>/task/*/stat` sampler in the harness wrapper is what
showed the Rust runners idle at 0.07 cores while the BEAM ran 3–5.

## A confirmation refills the outbox window

`try_dispatch_outbox/1` ran only on fsync and on the 1 s `:outbox_retry`
tick. Once ingest finished (no more appends → no more fsyncs) the tail of a
burst drained one `outbox_inflight_max` window per retry tick — 1 000
records/s per Entity. That was the entire "batch ingest stalls on
exactly-once" symptom, and why a `mark_dirty` dedup measured 2.3× worse (the
fsync tick was the only pump). `complete_entry/2` now dispatches after
`drain_confirmed/1`. The window is measured as `map_size(outbox_awaiting)` —
exactly "entries inside the chain unconfirmed" — because `:queue.len/1` on the
inflight queue is O(n) and made every refill quadratic.

## The InputLog owns its watermark

`record_consumed/3` inserted into one `read_concurrency` ETS table per
record, and every `:watermark_advance` full-scanned it with a partially bound
`:ets.match`. At 8 pipelines that was 43% of all controller CPU
(`:ets.whereis` 160 µs/call, `:ets.insert` 293 µs/call under the scan locks)
and the reason throughput *fell* as pipelines were added. The log now keeps
`consumed: %{entity => offset}` (seeded from the table at open), the cast
carries the offset, bursts of advances fold into one recompute
(`collect_consumed/2`, the `collect_appends/2` pattern), and the table is the
published view for the rare readers (sweep, cursors, CLI). The table is
created by `Flezha.State.Supervisor.init/1` — before, its owner was whichever
process touched it first, and it died with that process.

`pop_freed/3` pops a prefix of a queue: offsets are monotonic, so the freed set
is always a prefix. The handoff's "queue measured worse" was the tail-drain
defect above dominating once the fold stopped throttling ingest.

## The fsync tick casts

`FsyncBatcher.drain/2` called every dirty writer in series; one writer behind
a deep mailbox delayed every other writer's durability, credit release and
first outbox dispatch, and a 5 s call timeout dropped the dirty mark. The tick
now casts `:fsync` (writers sync concurrently), `flush_now`/`flush_all` still
call, and a writer marks dirty once per window (`pending_since_us == nil`). A
(re)started batcher seeds its set from `Supervisor.registered_ids/0` — with
one mark per window, a dirty set lost with a crashed batcher would never be
rebuilt.

## The dispatcher's chain run owns its tally

The `flz.ack` token was Base64-decoded three times per record (hold, settled,
confirm) — 9% of CPU — two of them by the process that minted it.
`Outbox.dispatch/3` now casts `{:io_out, payload, metadata, ack}` with the
decoded ack, the Overwatch runs the chain inside `Outbox.run/3`, `hold/1`
counts into the run (process dictionary, scoped to the run), and one
`{:outbox_settled, key, holds}` report carries the count. `hold/1` outside a
run (a test standing in for the dispatcher) still reports through the token.
`run/3` deliberately has no `try/after`: a crashed chain must leave
`pending > 0`, because the resettle only re-dispatches entries with pending
work — settling a crashed run would wedge its entry for good.

## A transport reader never runs consumer code

`routing::enqueue`'s `Block` arm served the full slot inline on the blocked
producer's thread. For an entry channel that producer is the runner's primary
stream reader, so a full mailbox stopped the thread feeding *every* operative
on the runner for one consumer's 1 024-item round while the poll pool idled.
Inline serving exists for one case — a pool worker that may be the lone thread
able to drain the consumer it is blocked on (one-worker self-deadlock, prod
2026-06-10) — so it is now gated on `routing::in_activation/1`, which the Rust
poll pool wraps around `run_activation`. Any other producer parks (bounded,
woken per pop). +15% at 4 pipelines with the default 1 024 mailbox; a 32K
mailbox had shown +34%, so the remaining gap is park/wake cadence.

## Kafka: one client per scheduler per cluster

`KafkaEx.Client` answers one request at a time and encodes the batch in its
own process. Sharing one per cluster serialised every partition's batches
behind one core. The provider's connection identity now carries
`lane: rem(partition, System.schedulers_online())` and `canonical_key/1`
includes it; everything else in `WorkerRegistry` is unchanged.

## Harness corrections that changed conclusions

- `tier_store` (the exactly-once bench factory) never declared
  `.on_full(Block)`; the `DropNewest` entry default silently shed whatever the
  1 024 mailbox could not hold once the bridge ran ahead of the runner. Every
  lost-record run was this.
- Session 1's batch-ingest path (`:ingest_batch` / `:io_in_batch` /
  `append_batch`) was removed: a second ingest path beside `ingest/3`, and
  bursty by construction against an entry default that still sheds. The
  InputLog's folding of already-queued per-record appends into one write
  (`collect_appends/2`) stays — it refines the one path.
- The generator shared one kafka_ex client across partitions — identity/off
  was measuring the generator. One client per partition now.
- `ProducerWorker.drain/4` re-summed the accumulator per record (O(n²) batch
  assembly).

## The InputLog syncs on a companion handle

`:file.datasync` ran inline in the log's message loop, ~3 ms per 5 ms tick on
this disk, and an effectively-once ingest call waits on that loop one record
at a time. A linked companion opens a second raw handle on the same path
(fdatasync syncs the inode, not the descriptor) and takes the batcher's
`:fsync`; the synchronous flush (`flush_now`, shutdown) round-trips to it.
`truncate_below` renames the file, so it tells the companion to reopen; a
`:normal` exit would not take a linked process along, so `terminate/2` stops
it explicitly. EO per-record 11.2K → 15.6K/s.

## One source, many requests in flight

`OperativeManager.send_io_confirming_many/5` keeps a window (default 64) of
ordinary `{:ingest, ..}` requests in flight with `:gen_server.send_request`
and a request-id collection. The operative runs its single `ingest/3` once per
input, in order (one caller, one server); only the round trips overlap. This is
the seam a source holding a batch (a Kafka source, the bench bridge) drives —
not the removed batch path, which ran a second server-side ingest. Paced
latency is unchanged by it.

## The Overwatch sends the frame itself

`Overwatch.state.channel` is the primary `StreamChannel`'s registered name
(`Lifecycle.attach_channel/2`), and every ingest used to `GenServer.call` it
just to learn bound-or-not while the channel's own work was one NIF push —
a round trip per record through the one process every operative on the
runner shares. The channel now publishes `{stream_ref, liveness_ref}` under
its name in `:persistent_term` (written at init, bind and close; erased at
terminate) and `StreamChannel.io_in/3` reads it and calls
`Native.send_frame/4` from the caller. Unbound, unknown and `nil` channels
answer `{:error, :no_stream}` and emit the drop themselves, so
`Ingest.forward_input/2` and `Overwatch.deliver/2` are one line each. Tests
stand in for the stream by stubbing `io_in/3` (`StreamChannel` is Mimic-copied
now) instead of faking a channel process.

With the hop gone (per record, 4 pipelines): identity/on 42.9K → 72.7K/s,
keyed/off 41.7K → 63.9K, keyed/state 31.9K → 42.2K, identity/state 36K →
43K; exactly-once unchanged at 15.6K — its ceiling is the `InputLog.append`
round trip per record and the `io_out` chain.

## Not the disk, still

With the ETS contention gone the profile shows `:prim_file.sync_nif` at 30%
of traced time — but that is the companion's *blocked* wall time (tprof counts
time inside a BIF), not CPU: tmpfs (`FLEZHA_STATE_DIR=/dev/shm/..`) gives the
same throughput as disk, and a 25 ms tick only +12%. The per-record ceiling is
wall time inside the Overwatch: two synchronous hops per ingest (`InputLog`,
the shared primary `StreamChannel` — consulted only to learn bound/unbound),
the `io_out` chain, ~7 messages per record.

## The sink's one batch in flight

`ProducerWorker` keeps one batch in flight per partition. At linger 0 a short
broker RTT (`acks=1`) makes each batch whatever arrived during the last
round trip — a handful of records — and the per-batch cost (the send `spawn`,
Kayrock encode, the kafka_ex client's `port_command`/recv/decode, per-record
completion) caps a partition near 10K/s; a longer RTT (`acks=-1`) accidentally
grows the batches, which is why identity/on out-ran identity/off 73K to 41K on
an identical path. A 2 ms linger (identity/state 42.9K → 83.4K) is the
harness's setting; several batches in flight per partition, safe under
idempotent sequence numbers, is the fix and a spec change
(`kafka-producer-plugin` pins the singular in-flight batch).

## Left open, measured or seen, not fixed

- WAL appends are one `:file.write` each (`:prim_file.write_nif` ~4% + kernel
  time); the InputLog already folds queued appends into one write, the writer
  could too.
- The effect-body postcard decode (`Wire.decode_*`, ~7%) runs in the append
  path; `changes` are materialised only to be ignored.
- `DurabilityTier.on_effect_flushed/2` calls `ClassRegistry.checkpoint_ack?/1`
  per record; with a remote backend configured that misses ETS for every
  non-checkpoint class and reads a sidecar file per record.
- The UDS driver drops past an 8 MB per-stream write buffer (`DropNewest`,
  `write_buffer_dropped`), with no back-pressure onto `io_in` — a fourth silent
  loss site for `close-silent-data-loss-defaults`, now printed by
  `--profile`.
- `BLOCK_PARK_TIMEOUT` (50 ms) and the 1 024 default mailbox bound a parked
  reader's cadence; a larger mailbox is memory per Entity, not a free default.

## Tests

`wal_writer_outbox_test` (window refills on a confirmation alone, 500 ms),
`input_log_test`, `fsync_batcher_test` (seed drained before counting marks),
`proc_journal_test` / `control_channel_proc_commit_test` (4-tuple dispatch),
`producer_worker_test`, `worker_registry_test`; `cargo test -p flz_core
routing`, `-p flezha_runner poll_pool`.
