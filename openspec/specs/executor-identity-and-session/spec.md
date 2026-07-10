# executor-identity-and-session Specification

## Purpose
TBD - created by archiving change unify-executor-liveness-and-identity. Update Purpose after archive.
## Requirements
### Requirement: Executor identity is two layers — durable stable identity and ephemeral session token

Executor identity SHALL be split into two values with distinct lifetimes:

1. **Stable identity** — owned by the SDK, durable across process restarts, and used by the controller as the **operative-binding key**. It SHALL be created once on the executor's first boot, derived for readability as `"<namespace>-<profile_hash>-<random_suffix>"` (where `profile_hash` is a stable hash of the executor's declared factory set), and persisted at `FLEZHA_EXECUTOR_ID_PATH` via an atomic write. The SDK SHALL present it on **every** connect (initial and reconnect) in the control init message. The controller SHALL trust the presented stable identity as the binding key.
2. **Session token** — minted by the controller on every accepted connection, unique by construction, ephemeral. It SHALL be the controller-side Registry-naming key and the data-stream routing key. The controller SHALL return it to the SDK in the assign command, and nothing durable SHALL be bound to it.

The boot-time hostname MAY be stamped into the stable identity's metadata for debuggability but SHALL NOT be load-bearing for identity (a container hostname is not stable across recreation). Identity SHALL NOT depend on any Kubernetes-specific node-name source; the same mechanism SHALL work identically under Kubernetes, plain Docker, and bare-host runs.

#### Scenario: stable identity persists across a process restart

- **GIVEN** an executor that booted, derived and persisted a stable identity at `FLEZHA_EXECUTOR_ID_PATH`
- **WHEN** the process restarts with the persistence path intact
- **THEN** it SHALL present the same stable identity on reconnect
- **AND** the controller SHALL re-point that stable identity's bindings onto the new session token

#### Scenario: session token is fresh per connection and is the routing key

- **GIVEN** an executor connecting to the controller
- **WHEN** the controller accepts the connection
- **THEN** it SHALL mint a fresh, unique session token and return it via the assign command
- **AND** controller-side Registry names and data-stream routing SHALL key on the session token, not on the stable identity

### Requirement: Data streams route by the owning connection, eliminating the concurrent-connect race

The controller SHALL route each incoming data stream to the `StreamChannel`s of the connection (executor session) that owns it, and SHALL NOT route data streams via a single shared "current executor" variable. On UDS this holds by construction — data streams arrive on per-session socket paths keyed by the session token. On QUIC, where each executor uses exactly one connection (so the connection maps 1:1 to its session token), the controller SHALL route by the connection identity the transport driver tags each data stream with; it SHALL maintain a per-connection binding (not a shared scalar) and SHALL drop the binding when the session ends so it stays bounded. No SDK-side data-stream tagging or wire-format change is required.

#### Scenario: two executors connecting concurrently over QUIC route correctly

- **GIVEN** two executors completing their control handshakes over the same QUIC server in close succession, each receiving its own session token
- **WHEN** each opens its data streams on its own connection
- **THEN** each executor's data streams SHALL bind to that executor's own `StreamChannel`s (routed by the owning connection identity)
- **AND** neither executor's streams SHALL be misrouted to the other (the previous last-writer-wins behaviour SHALL NOT occur)

### Requirement: Same stable identity on two live sessions resolves newest-wins

When a new session presents a stable identity that already has a currently-live session, the controller SHALL treat this as a takeover: it SHALL close the older session through the standard `{:executor_disconnected}` path and bind the stable identity's operatives to the newer session. The resolution SHALL be deterministic (newest wins), not dependent on an eviction race.

#### Scenario: a fresh process takes over from a not-yet-closed prior session

- **GIVEN** stable identity `S` has a live session `sess_old` whose connection has not yet been observed as closed
- **WHEN** a new connection presents stable identity `S` and receives `sess_new`
- **THEN** the controller SHALL close `sess_old` via the standard disconnect path
- **AND** SHALL bind `S`'s operatives to `sess_new`

#### Scenario: a misconfiguration sharing one persistence path is contained

- **GIVEN** two distinct executor processes wrongly configured to share one `FLEZHA_EXECUTOR_ID_PATH`, so both present stable identity `S`
- **WHEN** they connect such that both would be live under `S`
- **THEN** only the newest session SHALL hold `S`'s bindings; the older SHALL be closed
- **AND** the controller SHALL NOT serve two simultaneous live sessions for the same stable identity

### Requirement: The durable write path's owner binding follows takeover

Newest-wins takeover SHALL update each affected Segment's durable write-path
binding in the same transaction that re-binds the stable identity to the new
session, ordered unbind-old-session before bind-new-session, so no window exists
in which both sessions can forward Changes to the same Segment's WAL. A superseded
session's forwards SHALL stop at the binding (single state flip at takeover), with no
per-record ownership check added to the owning session's append path. The first
refused forward per (Segment, superseded session) episode SHALL emit
`[:flezha, :state, :stale_writer_refused]`; subsequent refusals in the episode
SHALL only increment a counter.

#### Scenario: Takeover leaves exactly one writable binding

- **GIVEN** stable identity `S` bound to session `sess_old` with Changes in flight
- **WHEN** a new session `sess_new` takes over `S`
- **THEN** `sess_old`'s channels are unbound from every affected Segment's write path before `sess_new`'s are bound, and no Change from `sess_old` lands in any Segment WAL after the unbind

#### Scenario: Refusal is episode-bounded telemetry

- **WHEN** a superseded session forwards many Changes after losing its binding
- **THEN** one `stale_writer_refused` event is emitted for the episode and a counter tracks the rest

### Requirement: Missing persistence degrades to fresh-identity-per-boot, never a hard failure

When the persistence path is unavailable (unset, unwritable, or not mounted), the SDK SHALL generate a fresh stable identity for the current boot, log the condition once at `warn`, and operate normally for the lifetime of the process. A subsequent restart SHALL present a new stable identity; the prior binding SHALL age out via `FLEZHA_EXECUTOR_KEEPALIVE`. The executor SHALL NOT refuse to start, and SHALL NOT crash, due to missing persistence.

#### Scenario: ephemeral executor with no mounted volume still runs

- **GIVEN** an executor started with no writable `FLEZHA_EXECUTOR_ID_PATH` (e.g. a dev container with no volume)
- **WHEN** it boots
- **THEN** it SHALL generate a fresh stable identity, log a single `warn`, and connect and run normally
- **AND** on restart it SHALL present a new stable identity and its prior binding SHALL age out via the keepalive window

### Requirement: Rust and Java SDKs present identity identically

Both the Rust and Java SDKs SHALL implement the stable-identity derivation, persistence, presentation, and session-token storage identically, so an executor of either language is indistinguishable to the controller. The cross-language test matrix SHALL exercise both over UDS and QUIC.

#### Scenario: a Java executor and a Rust executor are handled identically

- **GIVEN** one Rust executor and one Java executor, each with a persisted stable identity
- **WHEN** each connects, restarts, and reconnects
- **THEN** the controller SHALL apply the same binding-swap and takeover behaviour to both
- **AND** the cross-language matrix SHALL pass for both transports

