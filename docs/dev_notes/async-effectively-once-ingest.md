# The host never waits on the input log (`async-effectively-once-ingest`)

- **The cycle this closes.** An effectively-once append ran as an `:infinity`
  call inside the Overwatch. At zero source credit the log parked it, so the
  Overwatch blocked — and the Overwatch is the only process that runs the
  `{:io_out}` chains whose sink confirmations advance the watermark that mints
  the credit. Any confirmation lag longer than `window / ingest rate` (~29 s at
  4 × 2 500 rec/s with the 16 MiB default) wedged the operative for good. Now
  `Ingest` hands the append to the log with `InputLog.append_async/5` (the same
  `{:append, body}` request as `append/3`, issued with
  `:gen_server.send_request/4`), returns `:pending`, the entry point answers
  `{:noreply, state}`, and `Ingest.journal_reply/2` forwards the input and
  settles the reply when the log's answer arrives. The park lands on the
  source's call and on nothing else.
- **The owed set IS the request collection.** The proposal sketched a separate
  FIFO of `{label, from, payload}`; it is unnecessary. Each request's label
  carries `{reply_to, payload, retries_left}`, so the answer has everything it
  needs, and order needs no bookkeeping: the log answers appends in admission
  order (a park keeps arrival order, `unpark_waiters/1`), the Overwatch handles
  answers in arrival order, so forward order is offset order and verdicts
  settle in call order for free.
- **`reply_to` names the entry point, not just the caller**:
  `:cast | {:io_in | :ingest, from}`. The blocking `:io_in` call is still told
  `:ok` whatever the verdict (`overwatch_io_drop_test` pins `{:reply, :ok,
  ^state}`); the confirming `:ingest` call is told the verdict. `told/2` is the
  one place that decides, for the synchronous tier-off path and the deferred
  path alike.
- **A dead log's `:DOWN` must be checked before the plugin `:DOWN` clause.**
  `send_request` monitors the log; its exit arrives as a plain
  `{:DOWN, ref, :process, pid, reason}` that `check_response/3` recognises.
  The Overwatch's generic `:DOWN` clause would otherwise feed it to
  `Table.handle_down/4`, log "Binding not found", and leave the verdict owed
  forever. The normal reply shape is opaque, so it is matched by the last
  `handle_info` clause, which is now a catch-all: an unknown message logs and is
  dropped instead of crashing the host with a `FunctionClauseError`.
- **Re-issue once on ANY error reply, not only `:noproc`.** A log that dies
  mid-append reports its exit reason (`:killed`, `:shutdown`, …), not
  `:noproc`; the spec's "dies mid-append → re-issued → refused" needs the
  re-issue either way. The second failure answers `{:error, :refused}` and
  forwards nothing, so the sender retries an input that was never delivered.
- **`receive_response/3` abandons the request on timeout** — a later reply is
  silently discarded. `wait_response/3` does not (which is why
  `send_io_confirming_many/5` uses it). The first version of the tests probed
  "still parked?" with `receive_response(reqids, 150, true)` and then never saw
  the unparked reply. Probe with `wait_response`; the Overwatch itself uses
  `check_response` from `handle_info`, which has no such issue.
- **Suspension and handover keep their paths.** A suspended instance's input
  still runs `ingest/4` with `:cast` as its `reply_to` and then rehydrates; if
  the class is also effectively-once, the reply forwards later and a forward
  the rehydrating replay already made is absorbed by the SDK's consumed-offset
  guard — the same dedup the min-watermark replay relies on. A handover stops
  the old Overwatch with appends owed: its callers exit with it and retry by
  the confirming contract; a retried input may be journaled and forwarded twice,
  which the keyed sink's `(epoch, seq)` guard absorbs — the same window a crash
  between append and reply always had.
- **`journal_reqids` defaults to `:gen_server.reqids_new()` inside
  `defstruct`** — evaluated once at compile time, so every struct built by hand
  in a test already holds a valid empty collection.
- **Sizing a park in a test.** `CreditLedger.admit/2` admits anything on an
  empty buffer, so one oversized record (200 bytes against a 128-byte window)
  fills the window by itself and every short record behind it parks until it is
  consumed; `Ingest` stamps `"input_type"` before journaling, which adds ~25
  bytes to every body — size the window for the stamped bodies, not the strings.

## Soak (2026-08-21, `HANDOFF.md` recipe, keyed/exactly-once, 4 × 2 500 rec/s, keys 100)

`:source_credit_window_bytes` has no config knob; pass it on the VM flags:
`elixir --erl "-flezha source_credit_window_bytes 8192" -S mix bench.roundtrip …`.

- **256 KiB window, 5 min** (`flezha-keyed-on-TK48IG`): 3 000 000/3 000 000,
  0 dup, all signals 0, `write_buffer_dropped` 0; p50 19.2 / p95 50.5 / p99
  78.4 / p99.9 154.9 / max 265 ms; BEAM flat 145–153 MiB. At this rate the
  bytes in flight per partition (~20–80 KB) stay under 256 KiB, so that window
  parks rarely — it proves the tier at the proposal's setting, not the park.
- **8 KiB window, 2 min**: 1 200 000/1 200 000, 0 dup, all signals 0; still
  rate-capped at 9.7 K rec/s (34 stamped records per partition cover the
  append→confirm round trip), p50 34 / p99 112 ms.
- **1 KiB window, 240 K records** — the park made visible: ~4 records in flight
  per partition, throughput throttled to 1 734 rec/s against a 10 K pacing,
  240 000/240 000 received, 0 dup, all signals 0, no stall, the status line
  and fsync p99 reported throughout. The p50 of 32 s is queueing in the Kafka
  `in` topic while the source is parked — the back-pressure landing at the
  origin. The synchronous append wedged an operative at its first park; a run
  like this used to end `STALLED`.
- **Default window, 30 min** (`flezha-keyed-on-TK4961`, 18 M records):
  18 000 000/18 000 000, all signals 0, `write_buffer_dropped` 0, BEAM flat
  155–162 MiB; 800 duplicates = the bench generator's 4 produce retries × 200
  into the `in` topic after the Dockerised broker timed out once (source-side,
  attributed by the status line, the same shape as the pre-change 30-min
  baseline); p50 41 / p95 175 / p99 946 / p99.9 4 994 ms — the tail is that
  broker stall plus the host suspending for ~3 h mid-run (fsync p99 ≤ 21 ms
  outside the retention ticks' 166–169 ms).

## Tests

`input_log_test` — "append_async/5": the same offset `append/3` gives, a park
answered in arrival order by one mint, a stopped log answering the collection
with `{:error, {:normal, log}}`; the tier-on `ingest/4` case now drives the
asynchronous shape end to end. `overwatch_confirming_ingest_test` — "the
effectively-once journal": a live `Host` GenServer around the struct (the unit
is the host's loop) with a real input log and a 128-byte window: zero credit
parks the caller while `:get_overwatch_state`, an `{:io_out}` chain and `:ping`
are answered; forward order across a park; casts held in order with none
dropped; a log dying mid-append answering refused after exactly one re-issue.
Tier-off and durable-workflow tests are unchanged.
