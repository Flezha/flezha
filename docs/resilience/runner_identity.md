# Runner Identity & Session

Runner identity is **two layers** with distinct lifetimes (capability
`runner-identity-and-session`). Separating them removes the mint-evict-reissue
dance the old single `runner_id` forced on every restart, and is what gives a
restarted runner **binding continuity** — its operatives follow it onto the new
connection instead of being lost or re-issued.

## The two layers

| | Stable identity | Session token |
|---|---|---|
| Owned by | the SDK | the controller |
| Lifetime | durable across process restarts | one per accepted connection |
| Role | the **operative-binding key** the controller trusts | the **Registry/StreamChannel naming key** and the runner's session id |
| On the wire | `stable_id` in the `connect_control_channel` handshake (every connect) | returned in the `AssignRunnerId` command; not persisted by the SDK |

**Stable identity.** The SDK creates it once on first boot, derived for
readability as `"<namespace>-<profile_hash8>-<rand6>"` (`profile_hash8` is a
stable FNV-1a hash of the declared factory set; the boot-time hostname is
deliberately **not** load-bearing — a container hostname churns on recreate),
and persists it atomically at `FLEZHA_RUNNER_ID_PATH` (default
`/var/lib/flezha-runner/id`). It is cached per process (so reconnects present
the same id) and presented in the handshake on **every** connect. Rust:
`sdk/rust/src/runner/persistent_id.rs`. Java: `RunnerIdStore` (parity).

**Session token.** Minted fresh by the controller on every accept
(`Identificator.unique_id/1`, what `runner_id` already was). It names the
per-connection supervision tree (control channel, control unit, stream channels)
and the per-runner data sockets. Nothing durable is bound to it — bindings are
keyed by the stable identity → session-token mapping (`FleetManager.stable_sessions`).

## Reconnect = binding swap (not mint-evict-reissue)

On any connect the controller mints a fresh session token and brings up its
supervision tree, then `FleetManager.resolve_stable_identity/4`:

1. looks up the presented `stable_id`'s prior session token (`stable_sessions`);
2. if found (reconnect **or** a live-session takeover),
   `fence_superseded_session/1` unbinds the old session from every Entity
   write path **before** the new session is bound (see "The WAL binds to the
   owner" below);
3. `swap_bindings/3` re-points that session's operatives onto the new token —
   flip the Registry tag via `HubManager.operative_set_runner/3` (propagates
   cross-node) and re-seed the new session's pre-spawned `StreamChannel`s via
   `HubManager.hook_insert_pid/2` (the same primitives the convergence
   `Heal.rebind_operative` uses), so in-flight frames land without waiting for a
   tick;
4. closes the prior session (`do_evict_runner/2`, now operative-less);
5. records `stable_sessions[stable_id] = new_token`.

There is no `previous_runner_id` pre-eviction, no fresh-id mint for the SDK,
and no `RunnerReconnected` / `ReassignRunnerId` round-trip — all removed. The
convergence `Loop` still runs an immediate tick on `:runner_connected_event` as
a safety net; the swap above is the synchronous primary path.

## Takeover guard (newest-wins, D5)

Two live sessions claiming the same stable identity (a fresh process racing the
old one's not-yet-closed session, or a misconfiguration sharing one persistence
path) resolve **deterministically**: the newest session swaps the bindings and
the older is closed via the standard disconnect path. The controller never
serves two simultaneous live sessions for one stable identity. This is the same
`resolve_stable_identity` path as a reconnect.

## The WAL binds to the owner (write-path fence)

Routing-by-owning-connection closes the door, but a superseded session's frames
can still be in flight — queued in its `StreamChannel`s or in a `WalWriter`
mailbox — when the takeover lands. Without a fence, such a frame presenting a
divergent seq base would trigger a WAL realign away from the rightful owner's
lineage. The fence is a single state flip inside the takeover transaction,
ordered unbind-old **before** bind-new, in two halves:

- **Channel flip** — `FleetManager.fence_superseded_session/1` synchronously
  calls `:fence_write_path` on each of the old session's `StreamChannel`s;
  a fenced channel refuses every durable-write forward (`entity_state_change`,
  `entity_state_snapshot`, `delete_entity_state`, compensation markers) at
  the binding. The owning session's append path gains **no** per-record check.
- **Superseded mark** — `FleetManager.mark_session_superseded/1` records the
  old token in a public ETS set (`superseded_session?/1`), covering the frames
  a wedged channel flushes only after the swap: `WalWriter` consults the mark
  only when a frame's session differs from the epoch owner, refusing with
  `{:error, :stale_writer}` instead of realigning or appending. Marks are
  pruned after 10 minutes on the periodic runner-cleanup tick.

Refusals are loud but episode-bounded: the first refusal per (Entity,
superseded session) emits `[:flezha, :state, :stale_writer_refused]`
(bridged onto the observability bus); the rest only increment
`stale_writer_refused` (per-channel liveness counter) /
`stale_writer_refused_total` (per-Entity state metric). A legitimate rapid
restart racing its own eviction may be refused briefly — never a durability
loss, since the new session's restore/resync handshake re-covers those frames.

## Graceful degradation without persistence

If `FLEZHA_RUNNER_ID_PATH` is unwritable/unset (an ephemeral dev runner, no
mounted volume), the SDK generates a fresh stable id for the current boot, logs a
single `warn`, caches it for the process lifetime, and runs normally. A restart
then presents a **new** stable id whose old binding ages out via
`FLEZHA_RUNNER_KEEPALIVE` (today's reconnect-as-new behaviour). The runner
never refuses to start. Persistence is therefore the mechanism for *binding
continuity across restart*; its absence degrades cleanly. (Controller side: a
missing/empty `stable_id` connects fresh with no continuity.)

## Data-stream routing is by connection

Because each runner uses exactly one transport connection, data streams route
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
