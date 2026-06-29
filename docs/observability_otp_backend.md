# Observability OTP backend

Built-in process-backed sources for the BEAM VM, OS host, ProcessHub,
transport, and bounded-mailbox subsystems. Sits under
`Flezha.Observability.OtpBackend.Supervisor` and feeds the foundation
bus / ring buffer / Prometheus / OTLP sinks defined in
`platform-observability-foundation`.

## Shipped sources

| Source                    | Kinds                | Default poll | Cardinality budget |
| ------------------------- | -------------------- | -----------: | -----------------: |
| `:beam_vm`                | `[:metric]`          | 1_000 ms     | 50                 |
| `:host`                   | `[:metric, :event]`  | 5_000 ms     | 50                 |
| `:processhub`             | `[:metric]`          | 1_000 ms     | 200                |
| `:flezha_transport`       | `[:metric]`          | 1_000 ms     | 20                 |
| `:flezha_bounded_mailbox` | `[:metric, :event]`  | 1_000 ms     | 5_000              |

### `:beam_vm`

Emits per-tick:

- `:metric :beam_vm/:memory_{total|processes|atom|binary|ets}` —
  gauge, `measurements: %{value: bytes}`. Sourced from
  `:erlang.memory/0`.
- `:metric :beam_vm/:run_queue_total` — gauge, from
  `:erlang.statistics(:total_run_queue_lengths_all)`.
- `:metric :beam_vm/:gc_count` and `:gc_words_reclaimed` — cumulative
  counters from `:erlang.statistics(:garbage_collection)`.
- `:metric :beam_vm/:process_count` and `:port_count` — gauges from
  `:erlang.system_info/1`.
- `:metric :beam_vm/:scheduler_utilisation` — gauge in `[0.0, 1.0]`
  computed from `:erlang.statistics(:scheduler_wall_time)` deltas
  across consecutive polls (emitted from the second poll onward).

### `:host`

Linux `/proc` reader. Emits per-tick:

- `:metric :host/:rss_bytes` — gauge.
- `:metric :host/:cpu_user_us` and `:cpu_system_us` — cumulative
  counters (jiffies converted to microseconds at 10 ms/jiffy).
- `:metric :host/:cpu_utilisation` — gauge in `[0.0, 1.0]` from the
  delta of `utime + stime` between consecutive polls.
- `:metric :host/:open_fds` — gauge from `length(File.ls(/proc/self/fd))`.
- `:metric :host/:load_avg_{1m, 5m, 15m}` — gauges.

On non-Linux platforms (or when `/proc/self/stat` is unreadable) the
source emits exactly one `:event :host/:host_source_degraded` signal
and then no-ops every subsequent poll.

### `:processhub`

Enumerates `Flezha.Service.HubManager.get_running_namespaces/0` and
emits per-hub:

- `:metric :processhub/:registered_processes` — gauge of
  `length(ProcessHub.process_list(hub, :local))`.
- `:metric :processhub/:pending_recoveries` — gauge `0` / `1` mapped
  from `ProcessHub.recovery_state(hub)` (`:normal` → 0,
  `:recovering` / `:recovery_pending` → 1).

Metadata carries `%{hub: hub_id}`.

### `:flezha_transport`

Reads `Flezha.Transport.Server.list_active_transports/0` +
`get_stats/1` and emits per active transport:

- `:metric :flezha_transport/:open_connections` — gauge sourced from
  the GenServer's pending-streams count.

Metadata carries `%{transport: :uds | :quic}`. Bytes-in / bytes-out
are deferred until the NIF surfaces them in `get_stats/1`.

### `:flezha_bounded_mailbox`

Registered for future per-Operative mailbox metrics. Bounded-mailbox
state currently lives on the Rust SDK side; until a Rust→Elixir bridge
exposes it the source emits a single `:event
:flezha_bounded_mailbox/:bounded_mailbox_unavailable` event on its
first poll and no metrics. Cardinality budget `5_000` is sized for
`~50 namespaces × ~100 operatives`.

## Registering custom sources

A third-party source registers via the foundation's public API:

```elixir
Flezha.Observability.Registry.register_source(%{
  name: :my_source,
  kinds: [:metric],
  flavour: :process_backed,
  cardinality_budget: 100
})

# Then emit:
Flezha.Observability.Bus.emit(
  Flezha.Observability.Signal.new(%{
    kind: :metric, source: :my_source, name: :hits,
    measurements: %{count: 1}, metadata: %{path: "/x"}
  })
)
```

To host the source under the OTP backend's `:one_for_one`
supervisor, append its module to
`Flezha.Observability.OtpBackend.Supervisor.source_modules/0` via a
follow-up change, or run it under your own supervisor.

## Configuration

```elixir
config :flezha, :observability,
  otp_backend_enabled: true,
  otp_backend: [
    poll_intervals: %{beam_vm: 1_000, host: 5_000, ...}
  ]
```

Setting `:otp_backend_enabled` to `false` starts the
`OtpBackend.Supervisor` with zero children and registers no built-in
sources.
