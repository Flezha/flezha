# Runner Identity & Session

For contributors working on runner connect, reconnect and takeover. A runner's identity is two
layers with distinct lifetimes: a durable **stable identity** the SDK owns and a per-connection
**session token** the controller mints. Separating them gives a restarted runner binding
continuity — its operatives follow it onto the new connection — and lets a takeover be ordered so
that no two sessions ever hold a writable binding to the same Entity. This document covers the two
layers, the reconnect binding swap, the takeover guard, the WAL write-path fence, degradation
without persistence, and data-stream routing. The sequence view is in
[`../architecture/kernel.md`](../architecture/kernel.md).

## The two layers

| | Stable identity | Session token |
|---|---|---|
| Owned by | the SDK | the controller |
| Lifetime | durable across process restarts | one per accepted connection |
| Role | the operative-binding key the controller trusts | the Registry / `StreamChannel` naming key and the runner's session id |
| On the wire | `stable_id` in the `connect_control_channel` handshake payload, on **every** connect | returned in the `AssignRunnerId` command; never persisted by the SDK |

**Stable identity.** Created once on first boot as `"<namespace>-<profile_hash8>-<rand6>"`
(`profile_hash8` is an order-independent FNV-1a hash of the declared factory set; the hostname is
deliberately not load-bearing — a container hostname churns on recreate), persisted atomically at
`FLEZHA_RUNNER_ID_PATH` (default `/var/lib/flezha-runner/id`) and cached per process so reconnects
present the same id. Rust and Java share `flz_core/src/persistent_id.rs` through the `flz_core`
handshake; the Elixir SDK's `Flezha.SDK.PersistentId` (`lib/flezha/sdk/persistent_id.ex`) is
byte-compatible.

**Session token.** Minted by the controller on every accept (`Identificator.unique_id/1`). It
names the per-connection supervision tree (control channel, control unit, stream channels) and the
per-runner data sockets. Nothing durable is bound to it: bindings are keyed through the stable
identity → session token map (`FleetManager.stable_sessions`).

Runner ownership is **node-local**: `ControlUnit` and the `StreamChannel` registry live in the
per-node `Identificator` Registry and `FleetManager` is a per-node GenServer, so an Overwatch can
bind only to a runner connected to its own node. Moving an operative to a runner on another node is
a re-placement (`replace_operative` in [`convergence_loop.md`](convergence_loop.md)), not a rebind.

## Reconnect = binding swap

On any connect the controller mints a fresh session token and brings up its supervision tree, then
`FleetManager.resolve_stable_identity/4`:

1. looks up the presented `stable_id`'s prior session token in `stable_sessions`; a first connect just records the mapping;
2. if found (a reconnect or a live-session takeover), `fence_superseded_session/1` unbinds the old session from every Entity write path **before** the new session is bound (below);
3. `swap_bindings/3` re-points that session's operatives onto the new token: `HubManager.operative_set_runner/3` flips the registry tag (propagated cross-node) and `HubManager.hook_insert_local_pid/3` seeds the new session's pre-spawned `StreamChannel`s — the same primitives the convergence `rebind_operative` uses — so in-flight frames land without waiting for a tick;
4. closes the prior, now operative-less session (`do_evict_runner/2`);
5. records `stable_sessions[stable_id] = new_token`.

The swap is the synchronous primary path; the `:runner_connected_event` dispatched afterwards makes
the convergence Loop run an immediate tick as a safety net. A binding is committed only after the
runner answers: the Overwatch pings the runner and starts the operative there before
`bind_to_runner`, so a failed migrate never leaves an operative naming a live runner it cannot reach.

## Takeover guard (newest wins)

Two live sessions claiming one stable identity — a fresh process racing the old one's
not-yet-closed session, or a misconfiguration sharing one persistence path — resolve
deterministically through the same `resolve_stable_identity/4` path: the newest session swaps the
bindings and the older one is closed through the standard disconnect path. The controller never
serves two simultaneous live sessions for one stable identity.

## The WAL binds to the owner (write-path fence)

Routing by owning connection closes the door, but a superseded session's frames can still be in
flight — queued in its `StreamChannel`s or in a `WalWriter` mailbox — when the takeover lands.
Without a fence such a frame presenting a divergent seq base would realign the WAL away from the
rightful owner's lineage. The fence is ordered unbind-old **before** bind-new, in two halves:

- **Superseded mark** — `FleetManager.mark_session_superseded/1` records the old token in a public ETS set (`superseded_session?/1`) first. `WalWriter` consults the mark only when a frame's session differs from the epoch owner (the owning session's append path gains no per-record check) and refuses with `{:error, :stale_writer}` instead of realigning or appending — this covers frames a wedged channel flushes only after the swap. Marks are pruned after 10 minutes on the periodic runner-cleanup tick.
- **Channel flip** — `fence_superseded_session/1` then calls `:fence_write_path` (bounded, 1 s) on each of the old session's `StreamChannel`s; a fenced channel refuses every durable-write forward (`entity_state_change`, `entity_state_snapshot`, `delete_entity_state`, compensation markers) at the binding.

Refusals are loud but per episode: the first refusal per (Entity, superseded session) emits
`[:flezha, :state, :stale_writer_refused]` (bridged onto the observability bus); the rest only
increment `stale_writer_refused` (per-channel liveness counter) and `stale_writer_refused_total`
(per-Entity state metric). A legitimate rapid restart racing its own eviction may be refused
briefly — never a durability loss, since the new session's restore/resync handshake re-covers those
frames.

## Graceful degradation without persistence

If `FLEZHA_RUNNER_ID_PATH` is unwritable (an ephemeral dev runner, no mounted volume), the SDK
derives a fresh stable id for the current boot, logs a single warning, caches it for the process
lifetime, and runs normally. A restart then presents a **new** stable id; the old session's binding
ages out through the runner-cleanup tick after `FLEZHA_RUNNER_KEEPALIVE` (default 30 000 ms). The
runner never refuses to start. On the controller side a missing or empty `stable_id` connects fresh
with no continuity.

## Data-stream routing is by connection

Each runner uses exactly one transport connection, so data streams route by the **owning
connection**, never by an SDK-echoed token — no SDK-side stream tagging, no wire-format change:

- **UDS** — inherent: data streams arrive on per-session socket paths keyed by the session token (`SocketSupport.stream_socket_path/2`).
- **QUIC** — the NIF driver tags each `{:flz_stream, …, conn_id}` with the connection's id (the control stream's NIF id); `Flezha.Transport.Server` keeps a `conn_id → session` map and a `Process.monitor` on the `ControlChannel` drops the entry when the session ends.
- **native** (in-node) — binds through the same `Transport.Server.bind_data_stream/3` path as QUIC.

## See also

- [`transport_liveness.md`](transport_liveness.md) — the watchdog that closes a session so a reconnect/swap can happen
- [`convergence_loop.md`](convergence_loop.md) — `Heal.rebind/2` and `replace_operative`, the binding primitives this path shares
- [`state_persistence.md`](state_persistence.md) — the WAL epoch and owner the fence protects
- [`../sdk/protocol_spec.md`](../sdk/protocol_spec.md) — the `connect_control_channel` handshake and `AssignRunnerId`
