# Runner environment variables

Read by the runner process, not the controller node, so they are set where the
runner runs. A standalone Rust, Java or
Elixir runner reads them from its own environment; the shared `flz_transport`
crate also reads its rows inside the controller's NIF.

| Variable | Default | Read by |
|---|---|---|
| `FLEZHA_RUNNER_ID_PATH` | `/var/lib/flezha-runner/id` | `flz_core::persistent_id`, `Flezha.SDK.PersistentId` — the durable stable identity; an unwritable path degrades to a fresh per-boot id with one warning, never a refusal |
| `FLEZHA_MAILBOX_CAPACITY` | `1024` (range `1..1_048_576`) | per-Cell mailbox bound in all three SDKs; a Tasklet that overrides `mailbox_capacity()` wins over it |
| `FLEZHA_SOCKET_FILES_PATH` | resolved by the [socket-directory ladder](files_and_sockets.md#the-socket-directory-ladder) | `flz_transport` path helper — every SDK reaches it, Elixir through `flz_nif` |
| `FLEZHA_TRANSPORT` | `uds` | `flz_transport` (`quic` is the only other value, [experimental: quic](../publishing/maturity.md#quic)) |
| `FLEZHA_QUIC_ADDRESSES` | `127.0.0.1:4433` | `flz_transport` — a comma list of controller endpoints, each a name or a literal with an optional port (default `4433`), re-resolved on **every** attempt. `FLEZHA_QUIC_ADDRESS` is not honoured and, set alone, refuses startup naming this one; [experimental: quic](../publishing/maturity.md#quic) |
| `FLEZHA_CA_CERT_PATH` | `app_data/certs/ca.pem` | `flz_transport`, `Flezha.SDK.Transport` |
| `FLEZHA_QUIC_CLIENT_CERT` / `FLEZHA_QUIC_CLIENT_KEY` | `app_data/certs/client.pem` / `app_data/certs/client.key` | `flz_transport` — the client pair presented to a QUIC server that requires peer authentication; [experimental: quic](../publishing/maturity.md#quic) |
| `FLEZHA_MAX_FRAME_BYTES` | `67108864` (64 MiB) | `flz_transport` — pre-auth frame-length limit enforced by both frame parsers, cached on first read |
| `FLEZHA_MAX_WRITE_BUFFER_BYTES` | `8388608` (8 MiB) | `flz_transport` — per-stream send-buffer cap. A sender that honours the send gate never reaches it; what it refuses is answered to the caller, never discarded |
| `FLEZHA_SEND_GATE_HIGH_WATER` / `FLEZHA_SEND_GATE_LOW_WATER` | `0.8` / `0.5` | `flz_transport::transport::send_gate` — the send gate's watermarks, as **fractions** of `FLEZHA_MAX_WRITE_BUFFER_BYTES` so lowering the cap moves them with it. The gate rises when a stream's buffered plus queued bytes cross the high mark and falls when they drop under the low one; the sender holds in between. **The pair is taken whole or refused whole**: anything but two fractions with `0 < low < high < 1` leaves both defaults standing, because one configured mark beside one default is how a low-water mark ends up above its high-water one. Widening the gap costs memory and buys fewer wake-ups; narrowing it does the reverse. Neither is a throughput dial — the frames are sent either way |
| `FLEZHA_CREDIT_PARK_INTERVAL_MS` | `50` | `flz_core::credit` — how long a sender parked on an exhausted credit window waits before re-checking it. **A lost-wake fallback and a diagnostic, not a pacing dial**: `arm` and `grant` both `notify_all`, so the wait normally ends on the event and this bounds only a wake that never came. Lowering it buys no latency in the normal case and costs wakeups; raising it only lengthens recovery from a wake that was lost. `0` or an unparseable value reads as unset. Read inside `flz_core`, so a runner in any SDK satisfies it from its own environment |
| `FLEZHA_CONTROL_ACK_TIMEOUT_MS` | `10000` | `flz_transport` — first-frame ack deadline |
| `FLEZHA_CONTROL_IDLE_TIMEOUT_MS` | `45000` | `flz_transport` — steady-state idle window |
| `FLEZHA_CONTROL_KEEPALIVE_MS` | `15000` | `flz_transport` — control-channel ping interval |
| `FLEZHA_DATA_HEARTBEAT_MS` | `10000` | `flz_transport`, `Flezha.SDK.RunnerServer` — `data_heartbeat` interval, jittered into `[base/2, base]` |
| `FLEZHA_WORKER_POOL_SIZE` | Rust: host parallelism; Java: `2` fetchers | `flezha_runner::relay::poll_pool`, `FlezhaEnv` |
| `FLEZHA_WORKER_POOL_MAX` | `10 × FLEZHA_WORKER_POOL_SIZE` (derived, never below it) | Rust and Java — ceiling the starvation supervisor may grow the worker/fetcher set to (`flezha_runner::relay::poll_pool`, `FlezhaEnv`) |
| `FLEZHA_MAX_INFLIGHT_ACTIVATIONS` | `1024` | `flz_core::poll_core` — how many cells are handed out at once, a steps stage's as well as a flow relay's |
| `FLEZHA_DROP_NOTHING_NONPARKING` | `false` (off) | **Temporary rollout switch** (`flz_core::routing`). On, a `DropNothing` push a producing Cell cannot deliver is held in that Cell's push holdback and the Cell is handed no further work until it lands, instead of parking the calling host thread. Off, the producer parks as before. Reaches Rust and Java (both complete through `flz_core`); the Elixir SDK already defers and reads nothing here. Nothing an author declared changes either way — the guarantee is "nothing discarded, order preserved" in both. **It buys the freed thread and costs throughput under saturation**: free at the default mailbox capacity (p = 0.46), but **30–36% slower than the park** on a mailbox held continuously full (measured at `FLEZHA_MAILBOX_CAPACITY=4`). Turn it on where a host cannot afford to hold a thread — a single-threaded FFI host, a Java carrier pinned by a native frame — not for speed. Flipping the DEFAULT is gated on three things: a ≥ 2 h soak with it on against a genuinely saturated consumer, the Java round-trip bench under both settings (owed), and that 30–36% cost closed or accepted in writing; the flag is deleted in the change that flips it or the next one (`docs/dev_notes/nonblocking-drop-nothing-backpressure.md`) |
| `FLEZHA_PUSH_HOLDBACK_MAX` | `4096` | `flz_core::routing` — how many undelivered pushes one producing Cell may hold before an emit waits for its own earlier ones to land. Only an in-process typed emit loop can reach it; an FFI host's outputs are materialized before the core sees them. Reaching it logs one throttled `flezha::mailbox` warning naming the producer and the consumer |
| `FLEZHA_FLUSH_LINGER_MS` | `5` | `flz_core::flusher` |
| `FLEZHA_FLUSH_BEFORE_EMIT` | `true` (on) | Rust dispatcher; `0`/`false`/`off`/`no` reverts to inline emit |
| `FLEZHA_REAP_INTERVAL_MS` | `min(cell_idle_ttl / 4, 30 s)` (derived) | `flz_core::dynamic` and the Elixir SDK's Relay — idle-Cell reaper cadence |
| `FLEZHA_RECONNECT_RING_CAPACITY` | `1024` | `flz_core::state_sink` and `Flezha.SDK.Hold` — frames held while no stream will take them, replayed when one binds, oldest dropped on overflow and counted as a drop-oldest shed. Only a frame whose guarantee class is held enters it: a durable record or a plugin/steps output, never a control frame and never the heartbeat. In `flz_core` one ring holds them all; the same knob bounds four holders in the Elixir SDK — the Relay's plugin-bound outputs, the frames a stopping Cell hands to it, each Cell's own change records, and the steps driver's writes the runner forwards — a Relay reads it once and hands it to its own two holds and to the Cells it starts, so those three hold at one number, while the runner's own hold and a steps stage's Cells read it for themselves; and a rebind replays a stopped Cell's frames first, then a Cell's records, then the outputs of the same dispatch |
| `FLEZHA_BROKEN_PIPE_RECONNECT_THRESHOLD` / `FLEZHA_BROKEN_PIPE_WINDOW_MS` | `10` / `5000` | `flz_core::state_sink` and `Flezha.SDK.Hold` — write failures inside the window that request a reconnect. The Elixir SDK reads them for its `data_heartbeat`, which is owed no delivery and whose misses are the only signal an idle runner gives |
| `FLEZHA_RECONNECT_REQUEST_RETRY_MS` | `30000` | `flz_core::state_sink` and `Flezha.SDK.Hold` — an unanswered reconnect request re-arms after this, so it cannot disarm the detector for good |
| `FLEZHA_STEPS_ACK_TIMEOUT_MS` | `30000` | `flz_core::steps` and the controller's commit worker: how long one barrier send waits for its ack before it is re-sent (three sends in all), a deadline on the pass sheet; the controller reads the same value as its deadline for the effect records a barrier names to become durable, and fails the barrier with `record_not_durable` past it |
| `FLEZHA_FIXTURE_SLEEP_AFTER_MS` | unset | Test apps only: the `sleep_after` the reference runners' sleeping fixtures declare, so a drill or `mix bench.steps` can watch an instance sleep. A relay's sleep interval is its class's `durable` setting (`sleep_after`; by default a flow class never sleeps and a steps class sleeps after six hours), never the environment |
| `FLEZHA_NATIVE_LIB_DIR` | `java.io.tmpdir` | Java `NativeLibraryLoader` — where the bundled `flz_core_ffi` library is extracted |

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
