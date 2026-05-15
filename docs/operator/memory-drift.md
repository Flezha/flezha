# Controller memory-drift operator runbook

How to investigate sustained Controller BEAM memory growth under
steady load. The runbook is the procedure to follow whenever the
Controller's RSS rises monotonically over hours without a matching
rise in inbound message rate.

## When to reach for this page

- `kubectl top pod` / `docker stats` on the Controller shows a
  monotonically rising RSS over several hours, with no parallel
  growth in inbound message rate.
- A node has OOM-killed the BEAM after several hours of uptime under
  a workload that the same image survives indefinitely when idle.

If memory is high but **flat** under steady load, this page does NOT
apply — that is most likely the resident set-point of the workload,
not a leak.

## Step 1 — Enable the memory reporter

Set `FLEZHA_MEMORY_REPORT_INTERVAL_MS` to a positive integer (the
interval in milliseconds between readings). 60 000 (one reading per
minute) is appropriate for production; the load harness uses 10 000
(every 10 s) for finer-grained data.

```bash
export FLEZHA_MEMORY_REPORT_INTERVAL_MS=60000
flz controller start --mode daemon
```

Each interval the Controller emits a single `[info]` log line:

```
module=Flezha.Diagnostics.MemoryReporter
  erlang_memory_total=<bytes> erlang_memory_binary=<bytes>
  erlang_memory_processes=<bytes> erlang_memory_ets=<bytes>
  erlang_memory_atom=<bytes> erlang_memory_code=<bytes>
  process_count=<n> port_count=<n>
  binary_alloc_carriers_size=<bytes>
```

The line is space-separated `key=value` after the module marker; it
greps cleanly with `grep MemoryReporter <file> | cut -d ' ' -f 2-`.

## Step 2 — Snapshot the hot spots

When the reporter line shows growth (a multi-hour run with
`erlang_memory_total` rising by tens of MiB), capture the current
breakdown with the operator CLI:

```bash
flz controller mem-top --json > /tmp/mem-top-$(date -u +%Y%m%dT%H%M%SZ).json
```

The JSON document contains:

- `erlang_memory` — the per-subsystem byte counts (`total`, `binary`,
  `processes`, `ets`, `atom`, `code`, `system`).
- `top_processes` — top-20 processes by `:erlang.process_info(_,
  :memory)`, with registered name / initial call / message-queue
  length / heap size.
- `ets_tables` — every ETS table sorted by `:ets.info(tab, :memory)`
  in bytes.
- `dets_files` — every `*.dets` file under `app_data/process_hub/`
  sorted by file size.
- `recon_alloc_allocated_types` — `:recon_alloc.memory(:allocated_types)`
  output, useful for spotting allocator carrier waste (refc binary
  carriers especially).

Run `mem-top` three times across the suspect window: at the start, at
roughly the midpoint, and at the end. The deltas localize the leak.

## Step 3 — Localize

Read the three `mem-top` snapshots together with the reporter series:

| Signal | Likely culprit | First place to look |
|---|---|---|
| `erlang_memory_binary` dominates growth | Refc binary not released — frame payloads pinned somewhere | `recon_alloc_allocated_types.binary_alloc` carriers; top processes by message-queue length |
| `erlang_memory_ets` dominates growth | An ETS table is accumulating without bound | the `ets_tables` row that grew the most; check for missing eviction |
| `dets_files[...]` size dominates growth | ProcessHub durable registry not compacting | confirm there is no scheduled compaction; check whether registry-writers are dropping tombstones |
| One `top_processes` row's `memory` grows linearly | GenServer state / mailbox is leaking | `:sys.get_state/1` on the named pid + check `message_queue_len` |
| `process_count` grows linearly | Process leak — terminate/link/monitor missing | `Process.list/0` diff; look for a supervisor whose child count is unbounded |

The goal of localization is a named subsystem with quantitative
evidence: "ETS table `flezha_state_registry` grew from 4 MB to
142 MB; no other table grew by more than 1 MB", not just "ETS is up".

## Step 4 — Fix and verify

The fix is subsystem-specific. Once a fix is in place, re-run the
load harness against it:

```bash
MIX_RUN_MEMORY_DRIFT=1 \
FLEZHA_MEMORY_REPORT_INTERVAL_MS=10000 \
MEMORY_DRIFT_DURATION_MIN=240 \
  mix test test/load/memory_drift_test.exs
```

The harness drives a WS source (synthetic testnet or live Kraken WS)
and an optional Kafka producer plugin on `:on_input`, captures a
`mem-top` snapshot every 30 minutes, and prints the linear-fit growth
rate at the end. Compare the rate against your pre-fix baseline; a
sustained drop into the single-digit-MiB-per-hour range under the
same workload is the success signal. The numerical threshold for
"leak vs noise" depends on the workload — read the printed rate and
the artifacts together to judge.

See the harness `@moduledoc` in `test/load/memory_drift_test.exs` for
the full list of environment-variable knobs (workload selector, WS
source, broker address, payload override, etc.).
