# Runner environment variables

Read by the runner process, not the controller node, so they are set where the
runner runs. A runner in any language reads them from its own environment; the
shared `flz_transport` crate also reads its rows inside the controller's NIF. An
Elixir runner reads the connection rows from the OS environment, then
`config :flezha_sdk, ...`, and hands them to the core, so a
runner in the controller's own BEAM can be given its own values.

What each variable does is below; the type and default of every one is in
[Every variable](#every-variable), generated from the configuration table.

- `FLEZHA_RUNNER_START_WAIT_MS`: How long a runner's start waits for the controller to confirm or refuse its connect; past it the start fails with `runtime.connect_unconfirmed` and leaves no runner dialling. An unregister after start waits as long for the runner's answer; past it the unregister fails with `runtime.runner_unavailable` and the factory stays registered. `0` waits for ever. The Elixir SDK reads it as `config :flezha_sdk, runner_start_wait_ms: ...` too and hands it to the core. Default: `10000` (10 s).
- `FLEZHA_RUNNER_ID_PATH`: The durable stable identity; an unwritable path degrades to a fresh per-boot id with one warning, never a refusal.
- `FLEZHA_MAILBOX_CAPACITY`: The per-Cell mailbox bound for a Tasklet that declares none, in every SDK: the core reads it when a class registers (the Elixir SDK resolves it from its own configuration and hands the value over). A Tasklet that overrides `mailbox_capacity()` wins over it. The range is `1..1_048_576`.
- `FLEZHA_SOCKET_FILES_PATH`: Read through the `flz_transport` path helper, which every SDK reaches. Default: resolved by the [socket-directory ladder](files_and_sockets.md#the-socket-directory-ladder).
- `FLEZHA_TRANSPORT`: `uds` or `quic`, which is [experimental: quic](../publishing/maturity.md#quic).
- `FLEZHA_QUIC_ADDRESSES`: A comma list of controller endpoints, each a name or a literal with an optional port (default `4433`), re-resolved on **every** attempt. `FLEZHA_QUIC_ADDRESS` is not honoured and, set alone, refuses startup naming this one; [experimental: quic](../publishing/maturity.md#quic).
- `FLEZHA_QUIC_CLIENT_CERT` / `FLEZHA_QUIC_CLIENT_KEY`: The client pair presented to a QUIC server that requires peer authentication; [experimental: quic](../publishing/maturity.md#quic).
- `FLEZHA_MAX_FRAME_BYTES`: Pre-auth frame-length limit enforced by both frame parsers, cached on first read. Default: `67108864` (64 MiB).
- `FLEZHA_EFFECT_MAX_BYTES`: The largest encoded result `ctx.effect` keeps; a larger one is refused `effect.too_large` ([recorded effects](../topics/core/effectively_once.md#recorded-effects)). Default: `4194304` (4 MiB).
- `FLEZHA_MAX_WRITE_BUFFER_BYTES`: Per-stream send-buffer cap. A sender that honours the send gate never reaches it; what it refuses is answered to the caller, never discarded. Default: `8388608` (8 MiB).
- `FLEZHA_SEND_GATE_HIGH_WATER` / `FLEZHA_SEND_GATE_LOW_WATER`: The send gate's watermarks, as **fractions** of `FLEZHA_MAX_WRITE_BUFFER_BYTES` so lowering the cap moves them with it. The gate rises when a stream's buffered plus queued bytes cross the high mark and falls when they drop under the low one; the sender holds in between. **The pair is taken whole or refused whole**: anything but two fractions with `0 < low < high < 1` leaves both defaults standing, because one configured mark beside one default is how a low-water mark ends up above its high-water one. Widening the gap costs memory and buys fewer wake-ups; narrowing it does the reverse. Neither is a throughput dial — the frames are sent either way. Default: `0.8` / `0.5`.
- `FLEZHA_CREDIT_PARK_INTERVAL_MS`: How long a sender parked on an exhausted credit window waits before re-checking it. **A lost-wake fallback and a diagnostic, not a pacing dial**: `arm` and `grant` both `notify_all`, so the wait normally ends on the event and this bounds only a wake that never came. Lowering it buys no latency in the normal case and costs wakeups; raising it only lengthens recovery from a wake that was lost. `0` or an unparseable value reads as unset. Read inside `flz_core`, so a runner in any SDK satisfies it from its own environment.
- `FLEZHA_CONTROL_ACK_TIMEOUT_MS`: First-frame ack deadline.
- `FLEZHA_CONTROL_IDLE_TIMEOUT_MS`: Steady-state idle window.
- `FLEZHA_CONTROL_KEEPALIVE_MS`: Control-channel ping interval.
- `FLEZHA_DATA_HEARTBEAT_MS`: `data_heartbeat` interval, jittered into `[base/2 + 1, base/2 + base]` ms. Every runner wait is spread by this one rule, `d/2 + u` with `u` uniform in `1..=d`, the same draw as the controller's own backoff.
- `FLEZHA_RECONNECT_MAX_BACKOFF_MS`: The cap on a runner's wait between control-channel connect attempts (`100 ms × 2^attempt`, jittered, then held at the cap); the controller reads the same variable for its own reconnects.
- `FLEZHA_WORKER_POOL_SIZE`: The base of the poll-worker pool, read by the core's pool governor for every SDK: the threads that never retire. Default: each runtime's own: Rust: host parallelism; Java and Node: `2` fetchers (Node's `.fetchers(n)` names another); Python and Ruby: the processor count; Elixir: the schedulers online; PHP: `1` thread under `SwooleExecutor` (`SyncExecutor` runs one loop and no pool).
- `FLEZHA_WORKER_POOL_MAX`: The ceiling the governor grows the pool to, one worker per starvation observation; a worker above the base retires after 30 s of timed-out 250 ms polls, a pool below its base is topped up within the supervisor's 1 s wait, and a stop waits at most 5 s for the workers to leave. Node's completion pool takes the same base, ceiling and retire time. Default: `10 × FLEZHA_WORKER_POOL_SIZE` (derived, never below it).
- `FLEZHA_MAX_INFLIGHT_ACTIVATIONS`: How many cells are handed out at once, a steps stage's as well as a flow relay's.
- `FLEZHA_DROP_NOTHING_NONPARKING`: **Temporary rollout switch** (`flz_core::routing`). On, a `DropNothing` push a producing Cell cannot deliver is held in that Cell's push holdback and the Cell is handed no further work until it lands, instead of parking the calling host thread. Off, the producer parks as before. Reaches every SDK, since all complete through `flz_core`. Nothing an author declared changes either way — the guarantee is "nothing discarded, order preserved" in both. **It buys the freed thread and costs throughput under saturation**: free at the default mailbox capacity (p = 0.46), but **30–36% slower than the park** on a mailbox held continuously full (measured at `FLEZHA_MAILBOX_CAPACITY=4`). Turn it on where a host cannot afford to hold a thread — a single-threaded FFI host, a Java carrier pinned by a native frame — not for speed. Flipping the DEFAULT is gated on three things: a ≥ 2 h soak with it on against a genuinely saturated consumer, the Java round-trip bench under both settings (owed), and that 30–36% cost closed or accepted in writing; the flag is deleted in the change that flips it or the next one. Default: `false` (off).
- `FLEZHA_PUSH_HOLDBACK_MAX`: How many undelivered pushes one producing Cell may hold before an emit waits for its own earlier ones to land. Only an in-process typed emit loop can reach it; an FFI host's outputs are materialized before the core sees them. Reaching it logs one throttled `flezha::mailbox` warning naming the producer and the consumer.
- `FLEZHA_FLUSH_BEFORE_EMIT`: `0`/`false`/`off`/`no` reverts to inline emit. Default: `true` (on).
- `FLEZHA_REAP_INTERVAL_MS`: How often a dynamic tasklet's idle Cells are swept. The sweep is an entry on the poll core's deferred-wake list, re-armed after each sweep; no thread exists for it. Default: `min(idle_ttl / 4, 30 s)` (derived), at least 1 ms.
- `FLEZHA_RECONNECT_RING_CAPACITY`: Frames held while no stream will take them, replayed when one binds, oldest dropped on overflow and counted as a drop-oldest shed. Only a frame whose guarantee class is held enters it: a durable record or a plugin/steps output, never a control frame and never the heartbeat.
- `FLEZHA_BROKEN_PIPE_RECONNECT_THRESHOLD` / `FLEZHA_BROKEN_PIPE_WINDOW_MS`: Write failures inside the window that request a reconnect.
- `FLEZHA_RECONNECT_REQUEST_RETRY_MS`: An unanswered reconnect request re-arms after this, so it cannot disarm the detector for good.
- `FLEZHA_STEPS_ACK_TIMEOUT_MS`: How long one barrier send waits for its ack before it is re-sent (three sends in all), a deadline on the pass sheet; the controller reads the same value as its deadline for the effect records a barrier names to become durable, and fails the barrier with `record_not_durable` past it.
- `FLEZHA_FIXTURE_SLEEP_AFTER_MS`: Test apps only: the `sleep_after` the reference runners' sleeping fixtures declare, so a drill or `mix bench.steps` can watch an instance sleep. A relay's sleep interval is its class's `durable` setting (`sleep_after`; by default a flow class never sleeps and a steps class sleeps after six hours), never the environment.
- `FLEZHA_NATIVE_LIB_DIR`: Where the bundled `flz_core_ffi` library is extracted. Default: `java.io.tmpdir`.
- `FLEZHA_LOG_LEVEL`: The lowest level a runner's log writes, the core's own notices included (a retry, a reconnect, a refusal); the local-mode banner is written at `error`, which no level hides. Rust: the filter `diag::init_default_tracing` installs when `RUST_LOG` is unset. Java: the default `ConsoleLogger`. Node: the default console logger. Python: the handler the SDK installs on the `flezha` logger when the application configured none. Ruby and PHP: the default logger. Elixir: the core notices it logs under the domain `[:flezha]`. Default: `warn`.

## Every variable

<!-- config-keys:begin runner -->
| Variable | Type | Default |
|---|---|---|
| `FLEZHA_BENCH_MEASUREMENT_SECS` | int | `30` |
| `FLEZHA_BROKEN_PIPE_RECONNECT_THRESHOLD` | positive_int | `10` |
| `FLEZHA_BROKEN_PIPE_WINDOW_MS` | ms | `5000` |
| `FLEZHA_CA_CERT_PATH` | path | `storage/app/certs/ca.pem` |
| `FLEZHA_CONTRACT_DIR` | path | unset |
| `FLEZHA_CONTROL_ACK_TIMEOUT_MS` | ms | `10000` |
| `FLEZHA_CONTROL_IDLE_TIMEOUT_MS` | ms | `45000` |
| `FLEZHA_CONTROL_KEEPALIVE_MS` | ms | `15000` |
| `FLEZHA_CREDIT_PARK_INTERVAL_MS` | ms | `50` |
| `FLEZHA_DATA_HEARTBEAT_MS` | ms | `10000` |
| `FLEZHA_DROP_NOTHING_NONPARKING` | bool | `false` |
| `FLEZHA_EFFECT_MAX_BYTES` | int | `4194304` |
| `FLEZHA_FLUSH_BEFORE_EMIT` | bool | `true` |
| `FLEZHA_FLUSH_LINGER_MS` | ms | `5` |
| `FLEZHA_LOG_LEVEL` | one of `debug`, `info`, `warn`, `error` | `info (controller), warn (runner)` |
| `FLEZHA_MAILBOX_CAPACITY` | positive_int | `1024` |
| `FLEZHA_MAX_ACTIVATIONS` | positive_int | unset |
| `FLEZHA_MAX_FRAME_BYTES` | int | `67108864` |
| `FLEZHA_MAX_INFLIGHT_ACTIVATIONS` | positive_int | `1024` |
| `FLEZHA_MAX_WRITE_BUFFER_BYTES` | int | `8388608` |
| `FLEZHA_NAMESPACE_SECRET` | string | unset |
| `FLEZHA_NATIVE_BINARY` | path | unset |
| `FLEZHA_NATIVE_LIB_DIR` | path | unset |
| `FLEZHA_NODE_WORKERS` | positive_int | unset |
| `FLEZHA_PHP_EXECUTOR` | string | unset |
| `FLEZHA_PUSH_HOLDBACK_MAX` | positive_int | `4096` |
| `FLEZHA_PYTHON_EXECUTOR` | string | unset |
| `FLEZHA_QUIC_ADDRESS` | address | unset |
| `FLEZHA_QUIC_ADDRESSES` | string | `127.0.0.1:4433` |
| `FLEZHA_QUIC_CLIENT_CERT` | path | `storage/app/certs/client.pem` |
| `FLEZHA_QUIC_CLIENT_KEY` | path | `storage/app/certs/client.key` |
| `FLEZHA_REAP_INTERVAL_MS` | ms | unset |
| `FLEZHA_RECONNECT_MAX_BACKOFF_MS` | ms | `30000` |
| `FLEZHA_RECONNECT_REQUEST_RETRY_MS` | ms | `30000` |
| `FLEZHA_RECONNECT_RING_CAPACITY` | positive_int | `1024` |
| `FLEZHA_RELEASE_BASE_URL` | string | unset |
| `FLEZHA_RUBY_EXECUTOR` | string | unset |
| `FLEZHA_RUNNER_ID_PATH` | path | `/var/lib/flezha-runner/id` |
| `FLEZHA_RUNNER_START_WAIT_MS` | ms | `10000` |
| `FLEZHA_SDK_BUILD_NIF` | bool | `false` |
| `FLEZHA_SEND_GATE_HIGH_WATER` | int | unset |
| `FLEZHA_SEND_GATE_LOW_WATER` | int | unset |
| `FLEZHA_SOCKET_DIR_GROUP` | string | unset |
| `FLEZHA_SOCKET_FILES_PATH` | path | derived per run |
| `FLEZHA_STEPS_ACK_TIMEOUT_MS` | ms | `30000` |
| `FLEZHA_TRANSPORT` | one of `uds`, `quic` | `uds` |
| `FLEZHA_WORKER_POOL_MAX` | positive_int | unset |
| `FLEZHA_WORKER_POOL_SIZE` | positive_int | unset |
| `FLEZHA_YJIT` | bool | `false` |
<!-- config-keys:end -->

## `FLEZHA_DROP_NOTHING_NONPARKING`: read this before you set it

**What it does.** A `DropNothing` channel never discards anything, so a push
into a full consumer mailbox has to wait. This flag changes *what* waits.

- **Off (the default):** the thread doing the push parks until the consumer
  frees a slot. That thread is stuck. If it happens to be the only thread that
  could have run the consumer, nothing ever frees the slot and the process is
  finished.
- **On:** the value goes onto a per-producer holdback queue and the pushing
  thread returns immediately. The *producing Cell* is handed no further work
  until its queue drains. Nothing is discarded and order is preserved either
  way — only the waiting moves, from the thread to the Cell.

**The default is off. Nothing changes until someone sets it.** Setting it is a
deployment decision, not an authoring one: no channel declaration, wire literal
or conformance fixture differs between the two.

**What it costs.** Measured, not estimated: **30–36% slower under sustained
saturation**. On a mailbox pinned full (`FLEZHA_MAILBOX_CAPACITY=4`, 5 000
messages per iteration) one consumer went 18.92 ms → 25.78 ms and three
consumers 46.10 ms → 59.75 ms. Read that narrowly — it is the cost when a
mailbox is *continuously* full. At the default capacity of 1024 it is within
noise (7.36 ms → 7.19 ms, p = 0.46). It is not a general slowdown.

The reason is structural: parking woke the producer straight back into its
emit, while the holdback re-arm is a scheduling round-trip — consumer pops,
wait list fires, holdback drains, gates go back, the producer's round is
rescheduled, a poller picks it up.

**What it buys.** The producing thread is never held. That is the entire point,
and for some hosts it is the difference between backpressure and a dead
process. A host that cannot grow a thread — PHP's `SyncExecutor`, a Node host on
the event loop — turns the default park into a guaranteed freeze, every time,
not sometimes. The starvation supervisor cannot rescue those: it buys threads,
and a single-threaded runtime has none to sell. Turn this on where a thread
cannot be spared. Do not turn it on for speed.

**Debugging note.** With the flag on, a producer waiting on a full consumer
holds no activation, so `inflight` / `inflight_oldest_ms` cannot see it. Look at
`pending_pushes` and `pending_push_oldest_ms` on the data heartbeat instead, and
at the runner's own `flezha::mailbox` warning, which names both the producer and
the consumer that is behind.

**Before the default is ever flipped**, three things have to be true, and none
of them is true today:

1. the Java round-trip bench run under both settings (owed — the numbers above
   are Rust-only, and one runtime proves one runtime);
2. a soak of at least two hours with the flag on, against a workload that
   genuinely saturates a consumer, at 100% data integrity;
3. the 30–36% saturation cost either closed or accepted in writing.

The flag is deleted in the change that flips it, or the next one.
