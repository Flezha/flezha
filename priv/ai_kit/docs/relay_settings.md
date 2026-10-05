# Relay settings

What a relay declares in its own code. These settings are frozen when the
class registers with the controller and have no node-level override, because
they change what the relay's output means, not just how fast it comes; to
change one, redeploy the runner. How to choose them is taught in
[Configuring a relay](../topics/basics/configuring_a_relay.md).

Declared in code and shipped to the controller at registration: the class
settings on the relay, the channel options on the graph its `channels`
builds, the tasklet options on each tasklet. Rust names first, Java in
parentheses; each SDK spells the tree in its own idiom
([`sdk/authoring_contract.md`](https://github.com/flezha/flezha/blob/main/docs/sdk/authoring_contract.md)).

## Class settings

One settings tree per relay class (Rust `fn settings()`, Java `run()` and
`durable()`). A setting written under a parent that does not carry it is
refused by name (`topology.unknown_field`), and a value outside its choice as
`topology.invalid_setting`.

| Setting | Default if absent | What it does |
|---|---|---|
| `run` | `flow` | How an instance runs: `flow` takes inputs for as long as it lives, `steps` runs a chain of tasklets whose every step is journaled. |
| `run.flow.exactly` | `at_least_once` | `effectively_once` puts every emit on an atomic effect record through the WAL; the outbox cursor advances only on a sink's confirmation. Registration refuses it unless every plugin-bound channel is `.keyed()`, and refuses it on a class with `durable: false`. |
| `exactly.effectively_once.ack` | `per_input` | `checkpoint` holds the consumed offset until the interval closes, so state and offset advance together; its members are `every` (close after `n` inputs; an explicit `ctx.checkpoint()` still closes it early), `outputs` (`at_checkpoint` holds the interval's outputs and ships them inside its checkpoint record) and `max_delay_ms` (required with `at_checkpoint`; bounds how long a quiet Cell holds an output). |
| `exactly.effectively_once.correction_ms` | absent — a late input is just an input | A late input inside the window rewinds the Cell to a baseline and replays the affected slice in event-time order. Needs an event-time clock. |
| `exactly.effectively_once.beyond_window` | `process_as_arrival` | Fate of an input later than the window: process it as an arrival, or `drop` it. |
| `run.steps.each` | `once` | `once`, `per_input` or `chained` looping over the instance's messages (its durable input log when durable). |
| `run.steps.on_failure` | `undo_all` | How a rewind undoes journaled tasks: `undo_all` or `keep_progress`. |
| `durable` | `local` | Where an instance keeps its work: `false` in memory only (a crash re-runs from the top), `local` on the controller's disk, `remote` acknowledging each commit point (a steps stage, a flow input, a flow checkpoint) only after its remote ship (RPO 0; refused at start without `FLEZHA_DURABILITY_BACKEND`). |
| `durable.<local\|remote>.sleep_after` | `false` for a flow class, six hours for a steps class | An instance idle this long is written out and evicted from its runner, and wakes on its next message; `false` never sleeps. A sleeping flow instance has its input journaled before it is forwarded, so a flow class pays for that only when it sets an interval. |
| `max_duration_ms` (`maxDuration`) | none | Wall-clock bound on one run; an instance still running then ends `cancelled`. |

`ack.checkpoint.outputs: at_checkpoint` is a dial, not a switch: the interval
closes on whichever comes first of `every` inputs or `max_delay_ms`, so you add
up to `max_delay_ms` of median output latency and buy one WAL append per
interval instead of one per invocation. Leave it undeclared for the
latency-optimal setting; declare a small delay when throughput near the ceiling
matters more. How the numbers trade: [`operations/benchmarking.md`](https://github.com/flezha/flezha/blob/main/docs/operations/benchmarking.md).

## Channel and edge options

Declared on the stream handle returned by `input(...)` or a node's output.

| Declaration | Default | What it does |
|---|---|---|
| `.on_full(OverflowPolicy)` (`onFull`) | `DropNothing` | What a full Cell mailbox discards: `DropNothing`, `DropNewest`, `DropOldest`. Nothing drops by default; `DropNothing` slows the producer to the consumer's rate instead. |
| `.retry_max(n)` (`retryMax`) | `3` attempts | Attempts before a failing payload is parked to the dead-letter queue, or, into a steps tasklet, before the tasklet fails and the run compensates (a `per_input` keep-progress session parks the turn instead and goes on). `0` is refused (`topology.retry_max_zero`). |
| `.retry_backoff_ms(ms)` (`retryBackoffMs`) | `50` | Base of the exponential backoff between attempts, for both runs; the wait holds no thread. |
| `.keyed()` (`keyed`) | off | Every plugin emit must carry a natural key; the runtime stamps `flz.cell` / `flz.seq` / `flz.key`. Requires a `sink`; required on every plugin channel under `effectively_once`. |
| `.key_by(f)` (`keyBy`) | absent — one Cell | Route by a key function, making the consumer dynamic-cell. |
| `.sink(name)` / `.sink_with(name, f)` (`sink`) | — | Wire the channel to a named plugin; a second sink on one channel is a registration error. |

## Tasklet-level options

Overridden on the `Tasklet` trait (Rust) or Tasklet class (Java).

| Override | Default | What it does |
|---|---|---|
| `compact_after_bytes()` (`compactAfterBytes`) | 8 MiB, in lockstep with `:default_compact_after_bytes` | Per-Cell WAL size reported to the controller as the snapshot-compaction trigger. |
| `mailbox_capacity()` (`mailboxCapacity`) | `1024`, range `1..=1_048_576` | Bounded mailbox depth per Cell; precedence is the override, then `FLEZHA_MAILBOX_CAPACITY`, then the default. Rust sets it with the `capacity` key on `#[flezha::tasklet]`. |
| `cell_idle_ttl()` (`cellIdleTtl`; Elixir `idle_ttl_seconds/0`) | `None` — no eviction | Evict a dynamic Cell after this idle period; eviction is destructive (slot, WAL and snapshot dropped). |
| `max_cells()` (Elixir `max_cells/0`) | `100_000` | Soft cap on live dynamic Cells per Tasklet; new keys past it are dropped. |
| `CLOCK` (set by a written `event_time`, or the `logical_clock` key) | wall clock | Wall clock, event time or logical clock; decides `ctx.now()` and event-time ordering. |

A steps tasklet's retry ladder is its input edge's (above), and a commit
barrier whose ack timed out is re-sent three times, a constant, before the run
ends abnormally.
