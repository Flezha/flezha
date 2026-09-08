# Controller memory drift

For operators investigating sustained Controller BEAM memory growth under
steady load. This is the procedure to follow when the Controller's RSS rises
monotonically over hours without a matching rise in inbound message rate: turn
on the memory reporter, snapshot the hot spots with `flz controller mem-top`,
localise the subsystem, then prove the fix with the load harness.

## When this page applies

- `kubectl top pod` / `docker stats` shows a monotonically rising RSS over
  several hours with no parallel growth in inbound rate.
- A node has OOM-killed the BEAM after hours of uptime under a workload the
  same image survives indefinitely when idle.

High but **flat** memory under steady load is the workload's resident
set-point, not a leak — this page does not apply.

## Step 1 — Enable the memory reporter

`Flezha.Diagnostics.MemoryReporter` is off by default and starts only when
`FLEZHA_MEMORY_REPORT_INTERVAL_MS` parses as a positive integer (the interval
between readings). 60 000 suits production; the load harness uses 10 000.

```bash
export FLEZHA_MEMORY_REPORT_INTERVAL_MS=60000
flz controller start --mode daemon
```

Each interval the Controller emits one `[info]` line of space-separated
`key=value` pairs after the module marker:

```text
module=Flezha.Diagnostics.MemoryReporter
  erlang_memory_total=<bytes> erlang_memory_binary=<bytes>
  erlang_memory_processes=<bytes> erlang_memory_ets=<bytes>
  erlang_memory_atom=<bytes> erlang_memory_code=<bytes>
  process_count=<n> port_count=<n>
  binary_alloc_carriers_size=<bytes>
```

`grep MemoryReporter <file> | cut -d ' ' -f 2-` extracts the series.
`binary_alloc_carriers_size` (from `:recon_alloc`) diverging from
`erlang_memory_binary` separates in-use refc binaries from carrier waste.

## Step 2 — Snapshot the hot spots

When the series shows growth (tens of MiB over a multi-hour run), capture the
breakdown:

```bash
flz controller mem-top --json > /tmp/mem-top-$(date -u +%Y%m%dT%H%M%SZ).json
```

The JSON document (`flz controller mem-top` without `--json` prints the same
as text) carries:

| Key                           | Contents                                                                                                                                                     |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `captured_at`, `node`         | UTC timestamp and node name.                                                                                                                                 |
| `erlang_memory`               | `total`, `binary`, `processes`, `ets`, `atom`, `code`, `system` in bytes.                                                                                    |
| `top_processes`               | Top 20 by `:erlang.process_info(_, :memory)`: `pid`, `memory`, `registered_name`, `initial_call`, `current_function`, `message_queue_len`, `heap_size`, `total_heap_size`. |
| `ets_tables`                  | Every table as `name`, `size`, `memory_bytes`, sorted by memory.                                                                                             |
| `dets_files`                  | Every `*.dets` under `./app_data/process_hub/` (relative to the Controller's cwd) as `path`, `size_bytes`, sorted by size.                                    |
| `recon_alloc_allocated_types` | `:recon_alloc.memory(:allocated_types)` — allocator carrier waste, refc binary carriers especially.                                                            |

Run `mem-top` three times across the suspect window — start, midpoint, end.
The deltas localise the leak.

## Step 3 — Localise

Read the three snapshots together with the reporter series:

| Signal                                       | Likely culprit                                        | First place to look                                                              |
| -------------------------------------------- | ----------------------------------------------------- | -------------------------------------------------------------------------------- |
| `erlang_memory_binary` dominates growth      | Refc binary not released — frame payloads pinned      | `recon_alloc_allocated_types.binary_alloc`; top processes by `message_queue_len` |
| `erlang_memory_ets` dominates growth         | An ETS table accumulating without bound               | the `ets_tables` row that grew the most; look for missing eviction               |
| `dets_files[...]` size dominates growth      | ProcessHub durable registry not compacting            | whether registry writers drop tombstones                                         |
| One `top_processes` row grows linearly       | GenServer state or mailbox leaking                    | `:sys.get_state/1` on the named pid; its `message_queue_len`                     |
| `process_count` grows linearly               | Process leak — missing terminate/link/monitor         | `Process.list/0` diff; a supervisor whose child count is unbounded              |

The goal is a named subsystem with quantitative evidence — "ETS table X grew
from 4 MB to 142 MB; no other table grew by more than 1 MB" — not "ETS is up".

## Step 4 — Fix and verify

The fix is subsystem-specific. Two VM-level remedies belong to this page rather
than to the release's shipped flags, because neither is right by default and
both are chosen against a measured series:

- `-env ERL_FULLSWEEP_AFTER 0` makes every process GC a fullsweep, which
  reclaims refc binaries a generational collection leaves referenced. It is the
  first thing to try when `erlang_memory_binary` and
  `binary_alloc_carriers_size` diverge and the divergence is in carriers rather
  than in live data. It costs CPU on every collection, which is why it is a
  remedy and not a default.
- The binary allocator's own strategy (`+MBas`, `+MBlmbcs`) addresses carrier
  waste rather than live binaries. Reach for it only when Step 2's
  `recon_alloc_allocated_types` shows carrier waste dominating, and change one
  thing at a time.

Both are set the way any VM flag is, through `ERL_FLAGS`
([`../configuration.md`](../configuration.md#erlang-vm-arguments)), so a trial
run needs no rebuild and no image change.

Prove any fix with the load harness:

```bash
MIX_RUN_MEMORY_DRIFT=1 \
FLEZHA_MEMORY_REPORT_INTERVAL_MS=10000 \
MEMORY_DRIFT_DURATION_MIN=240 \
  mix test test/load/memory_drift_test.exs
```

`test/load/memory_drift_test.exs` drives a WebSocket source (the synthetic
testnet or a live exchange websocket, e.g. Kraken) and optionally a Kafka
producer plugin on `:on_input`, writes `mem_top_<isoz>.json` snapshots and a
`memory_drift_series_<isoz>.json` series to `test/load/artifacts/`, and prints
the linear-fit growth rate in MiB/hour. It is excluded from `mix test` unless
`MIX_RUN_MEMORY_DRIFT=1` is set.

| Env var                                           | Default               | Meaning                                             |
| ------------------------------------------------- | --------------------- | --------------------------------------------------- |
| `MEMORY_DRIFT_DURATION_MIN`                       | `240`                 | Wall-clock budget                                   |
| `MEMORY_DRIFT_SNAPSHOT_MIN`                       | `30`                  | `mem-top` snapshot cadence                          |
| `FLEZHA_MEMORY_REPORT_INTERVAL_MS`                | `10000`               | In-process sampling cadence                         |
| `MEMORY_DRIFT_WORKLOAD`                           | `combined`            | `ws`, `kafka` or `combined`                         |
| `MEMORY_DRIFT_WS_SOURCE`                          | `testnet`             | `testnet` or `kraken` (a live exchange websocket)   |
| `MEMORY_DRIFT_WS_INTERVAL_MS`                     | `3`                   | Testnet echo interval (~333 msg/s)                  |
| `MEMORY_DRIFT_WS_URI` / `MEMORY_DRIFT_WS_SUBSCRIBE` | Kraken book feed    | Override the live endpoint and subscribe message    |
| `MEMORY_DRIFT_PAYLOAD`                            | built-in              | Testnet echo body (>= 64 B to exercise refc binaries) |
| `MEMORY_DRIFT_KAFKA_HOST` / `MEMORY_DRIFT_KAFKA_PORT` | `localhost:9092`  | Broker for the producer plugin                      |
| `MEMORY_DRIFT_KAFKA_TOPIC`                        | `memory_drift_test`   | Destination topic                                   |

Compare the printed rate against the pre-fix baseline under the same
workload; a sustained drop into the single-digit MiB/hour range is the success
signal. The leak-vs-noise threshold depends on the workload — judge the rate
and the artifacts together.

## See also

- [`observability.md`](observability.md) — the signal bus the reporter line rides to Loki.
- [`../internals/process_hub.md`](../internals/process_hub.md) — the durable registry behind `dets_files`.
