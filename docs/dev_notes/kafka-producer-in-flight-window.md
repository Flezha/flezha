# kafka-producer-in-flight-window — implementation notes

The producer kept one Produce request in flight per partition because the
`kafka_ex` client could carry only one outstanding request per broker
connection. The client learned to pipeline; the worker learned to keep a
window of batches and to put a failed one back in front of everything behind
it.

## The two protocol facts the whole design rests on

- **A broker answers one connection's requests in the order it received
  them.** That is what makes "the frame belongs to the oldest outstanding
  request" a correct demultiplexer, and it is why a correlation id that says
  otherwise is treated as a broken stream rather than as a routing hint.
- **Under the idempotent producer the broker rejects a batch whose
  `base_sequence` does not continue the last one it stored**
  (`out_of_order_sequence_number`). That is what makes an ordered retry
  possible at all: batch *k* failing means *k+1…* are rejected for the gap
  *k* left, so resending *k, k+1, …* in order is the only sequence the broker
  will accept. Nothing preserves order under retry without it, which is why
  `:max_in_flight > 1` with `:idempotent = false` is refused at plugin
  preparation (`Kafka.Producer.validate_window/1`) instead of warned about.

The same reasoning applies at runtime: when `InitProducerId` fails and the
worker falls back to non-idempotent mode, it narrows `max_in_flight` to 1 in
the same breath. A five-wide window without a producer session is exactly the
silent reordering the configuration check exists to prevent.

## `kafka_ex` fork: pipelined requests

Branch `feat/pipelined-requests` on `alfetahe/kafka_ex`, pinned by ref in
`mix.exs`/`mix.lock`.

- `KafkaEx.Client.send_request_async/3,4` serialises, sends on the (active)
  broker socket, registers `{correlation_id, ref, caller, request}` on that
  socket's FIFO in `KafkaEx.Client.State.pending`, and replies `{:ok, ref}`.
  `handle_info({:tcp | :ssl, socket, frame})` answers the FIFO head and sends
  `{:kafka_ex_response, ref, result}` to the caller. The pending FIFO is keyed
  by the **raw** socket, because that is what the message carries.
- A mismatch closes the connection and fails everything outstanding on it with
  `:correlation_mismatch`; `{:tcp_closed | :ssl_closed, socket}` fails it with
  `:closed`. So does a metadata merge that closes a broker.
- **The sync-behind-async rule is a read-past, not a registration.** The
  design called for registering `{correlation_id, from}` and replying later,
  but the client's own recursive callers — `update_metadata/2`,
  `get_api_versions/1`, `update_consumer_group_coordinator/2`, all reached
  through `first_broker_response/5` — cannot suspend mid-function, and the
  periodic 30 s metadata refresh WILL hit a pipelined socket in a
  single-broker cluster. One mechanism serves both: when a socket has pending
  requests, `send_sync_request_pipelined/5` reads frames until the one
  carrying its own correlation id and hands each earlier frame back to the
  client's own mailbox as `{:tcp, socket, frame}`. Frames already delivered
  stay ahead of the reposted ones, so `handle_info/2` still sees connection
  order. A socket with nothing pending keeps the untouched passive path.
- `first_broker_response/5` therefore has to know which sockets are
  pipelined, and `send_request.(wire, correlation_id)` grew the id argument
  the read-past needs.

## The worker's window

`ProducerWorker.in_flight` is a list of batches, oldest first (not `:queue` —
it is bounded by 5 and every operation is an ordered scan or rewrite).

- Two identities per batch: `id` is stable across retries and routes
  everything the worker sends itself (`{:retry_window, id}`,
  `{:topic_create_result, id, _}`, `{:metadata_refresh_done, id, _, _}`);
  `ref` is the client's handle for the *current* dispatch and routes
  `{:kafka_ex_response, ref, _}` and `{:dispatch_timeout, ref}`. Clearing
  `ref` is how a dispatch is abandoned — a late outcome then names no batch
  and is ignored, which is the old fresh-ref-per-dispatch rule generalised.
- `status` has two values and they are about *outcomes*, not about the wire:
  `:sent` means an outcome is outstanding — a response, **or** a CreateTopics
  / metadata refresh issued for that batch. `:pending` means the batch is owed
  a resend. Folding the recovery actions into `:sent` is what stops a sibling
  batch's completion from resending a batch whose topic is still being
  created.
- **Nothing new is cut and nothing is resent while any batch is `:pending`**
  (`slot_free?/1`, `maybe_resend/1`). One retry timer covers the whole resend
  run; `resend_window/1` sends every owed batch oldest first under its
  original sequence numbers.
- **`do_send/2` is total and never re-entrant.** A send that returns an error
  posts a synthetic `{:kafka_ex_response, ref, error}` to the worker instead
  of calling `handle_batch_result/3` inline. Without that, a synchronous
  failure inside `resend_window/1`'s reduce would schedule a retry while the
  reduce was still resending later batches, and the wire order would break.
  It is also why the `spawn` around the send could go: the client answers by
  message, so the worker sends from its own process.
- **A fence is only a fence for the oldest batch.** `out_of_order_sequence_number`
  behind an unresolved batch means "failed behind it" and takes the ordinary
  retry path. For the oldest batch it re-inits the producer session;
  `rebind_worker/1` then runs `realign_window/1`, which renumbers every batch
  still owed from the new base **in window order**, increments their attempts,
  drops their dispatch handles and marks them `:pending`. The same realignment
  runs on a forced broker replace — a fresh client means a fresh session, and
  the sequences the window is holding are gone with the old one.
- Worker death rewinds the whole window to the head of the queue, oldest
  batch first, as loose records. They re-batch normally on the way out, so the
  observable guarantee is admission order, not batch identity.

## Test seam

The scripted broker in `test/plugin/kafka/producer_worker_test.exs` answers by
message, because the client does: a send returns `{:ok, ref}` and the scripted
outcome is posted to the worker's own mailbox. `:never` is a dispatch that is
never answered (the batch stays in flight — this replaces the old
`Process.sleep(:infinity)` fakes, which would now block the worker itself) and
`{:delayed, value, ms}` answers after a delay. `Fakes.relay_send_fn/1` hands
every dispatch to the test as `{:dispatched, ref, request}` so a test can
answer any batch in any order; that is what the window scenarios drive.

## Measurement

`./bench/flink/bench.sh --only flezha --repeat 3 -p 4 -m 15000 --latency 2500`
with `--linger-ms 2 --idempotent`, run three times — once per width. Medians of
three, rec/s, unpaced:

| cell | w=1 | w=2 | w=5 |
|---|---|---|---|
| identity/off | 68.1K | 104.5K | 104.8K |
| identity/state | 73.6K | 102.1K | 101.3K |
| identity/on | 63.2K | 93.5K | 101.1K |
| keyed/off | 70.4K | 77.3K | 75.5K |
| keyed/state | 39.5K | 45.0K | 49.2K |
| keyed/on (exactly-once) | 25.1K | 25.6K | 25.7K |

**That table is warm-up drift, not the window.** The three widths ran as three
consecutive sweeps, and this box climbs over the first several minutes of load.
Alternating the widths *inside* each round — the same discipline `bench.sh`
already applies to engines, for the same reason — the difference disappears
(medians of three, rec/s):

| run | w=1 | w=2 | w=5 |
|---|---|---|---|
| linger 2, `-p 4`, identity/off | 108.3K | — | 101.1K |
| linger 2, `-p 4`, keyed/state | 49.7K | — | 50.3K |
| linger 0, `-p 4`, identity/off | 97.4K | — | 98.4K |
| linger 0, `-p 4`, identity/state | 98.0K | — | 99.8K |
| linger 0, `-p 1`, identity/off | 65.4K | 61.9K | 65.9K |

`linger 0` and `-p 1` are there because they are where the width *should* show:
without the linger's own amortisation each batch is only what arrived during the
last round trip, and one partition removes the four-way concurrency that hides a
per-partition ceiling. Neither moves.

The honest reading is that **this box cannot resolve the axis**: the broker runs
in a container on the same host, so the round trip is short enough that the
batch which leaves already carries everything queued and a second outstanding
request has nothing to carry. The proposal's evidence (identity/off 41K at
linger 0) predates the outbox-report batching and the durable-path pass, which
moved the same cell to ~97K at width 1 — the per-batch cost the window was aimed
at is no longer what binds here. The width is worth having for a broker whose
round trip is real, and it is what makes the ordered-retry machinery meaningful,
but no local number argues for it.

The default stays 5 (Kafka's own ceiling for ordered idempotent retries): no
measured cost, and the width is only reachable under idempotence. The one real
price is the duplicate bound on producer-id rotation, which grows from one batch
to five — `docs/kafka-producer.md` says so beside `:max_in_flight`, and
`:max_in_flight = 1` restores the old bound.

Integrity across the 91 sweep runs: zero duplicates and zero lost records at
every width, and the paced pass (2 500 rec/s per partition) shows p50 unchanged.
