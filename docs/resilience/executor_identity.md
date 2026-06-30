# Executor Identity & Session

Executor identity is **two layers** with distinct lifetimes (capability
`executor-identity-and-session`). Separating them removes the mint-evict-reissue
dance the old single `executor_id` forced on every restart, and is what gives a
restarted executor **binding continuity** — its operatives follow it onto the new
connection instead of being lost or re-issued.

## The two layers

| | Stable identity | Session token |
|---|---|---|
| Owned by | the SDK | the controller |
| Lifetime | durable across process restarts | one per accepted connection |
| Role | the **operative-binding key** the controller trusts | the **Registry/StreamChannel naming key** and the executor's session id |
| On the wire | `stable_id` in the `connect_control_channel` handshake (every connect) | returned in the `AssignExecutorId` command; not persisted by the SDK |

**Stable identity.** The SDK creates it once on first boot, derived for
readability as `"<namespace>-<profile_hash8>-<rand6>"` (`profile_hash8` is a
stable FNV-1a hash of the declared factory set; the boot-time hostname is
deliberately **not** load-bearing — a container hostname churns on recreate),
and persists it atomically at `FLEZHA_EXECUTOR_ID_PATH` (default
`/var/lib/flezha-executor/id`). It is cached per process (so reconnects present
the same id) and presented in the handshake on **every** connect. Rust:
`sdk/rust/src/executor/persistent_id.rs`. Java: `ExecutorIdStore` (parity).

**Session token.** Minted fresh by the controller on every accept
(`Identificator.unique_id/1`, what `executor_id` already was). It names the
per-connection supervision tree (control channel, control unit, stream channels)
and the per-executor data sockets. Nothing durable is bound to it — bindings are
keyed by the stable identity → session-token mapping (`FleetManager.stable_sessions`).

## Reconnect = binding swap (not mint-evict-reissue)

On any connect the controller mints a fresh session token and brings up its
supervision tree, then `FleetManager.resolve_stable_identity/4`:

1. looks up the presented `stable_id`'s prior session token (`stable_sessions`);
2. if found (reconnect **or** a live-session takeover), `swap_bindings/3`
   re-points that session's operatives onto the new token — flip the Registry
   tag via `HubManager.operative_set_executor/3` (propagates cross-node) and
   re-seed the new session's pre-spawned `StreamChannel`s via
   `HubManager.hook_insert_pid/2` (the same primitives the convergence
   `Heal.rebind_operative` uses), so in-flight frames land without waiting for a
   tick;
3. closes the prior session (`do_evict_executor/2`, now operative-less);
4. records `stable_sessions[stable_id] = new_token`.

There is no `previous_executor_id` pre-eviction, no fresh-id mint for the SDK,
and no `ExecutorReconnected` / `ReassignExecutorId` round-trip — all removed. The
convergence `Loop` still runs an immediate tick on `:executor_connected_event` as
a safety net; the swap above is the synchronous primary path.

## Takeover guard (newest-wins, D5)

Two live sessions claiming the same stable identity (a fresh process racing the
old one's not-yet-closed session, or a misconfiguration sharing one persistence
path) resolve **deterministically**: the newest session swaps the bindings and
the older is closed via the standard disconnect path. The controller never
serves two simultaneous live sessions for one stable identity. This is the same
`resolve_stable_identity` path as a reconnect.

## Graceful degradation without persistence

If `FLEZHA_EXECUTOR_ID_PATH` is unwritable/unset (an ephemeral dev executor, no
mounted volume), the SDK generates a fresh stable id for the current boot, logs a
single `warn`, caches it for the process lifetime, and runs normally. A restart
then presents a **new** stable id whose old binding ages out via
`FLEZHA_EXECUTOR_KEEPALIVE` (today's reconnect-as-new behaviour). The executor
never refuses to start. Persistence is therefore the mechanism for *binding
continuity across restart*; its absence degrades cleanly. (Controller side: a
missing/empty `stable_id` connects fresh with no continuity.)

## Data-stream routing is by connection

Because each executor uses exactly one transport connection, data streams route
by the **owning connection**, not by an SDK-echoed token:

- **UDS** — inherent: data streams arrive on per-session socket paths keyed by
  the session token.
- **QUIC** — the NIF driver tags each `{:flz_stream, …, conn_id}` with the
  connection's id (the control stream's nif id); `Flezha.Transport.Server` keeps
  a bounded `conn_id → session` map (a `Process.monitor` on the ControlChannel
  drops the entry when the session ends), replacing the `quic_current_ce_id`
  last-writer-wins scalar that misrouted on concurrent connect.

This fixes the QUIC concurrent-connect race with no SDK-side stream tagging and
no wire-format change.

## Related

- Control-plane session liveness (the watchdog that closes a connection so a
  reconnect/swap can happen): [`transport_liveness.md`](transport_liveness.md).
- The reusable binding primitives: `Flezha.Convergence.Heal.rebind_operative`.
