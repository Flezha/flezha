# runner-identity-and-session Specification

## Purpose
Splits Runner identity into two values with distinct lifetimes: a **stable identity** owned and persisted by the SDK, presented on every connect and used by the Controller as the operative-binding key; and an ephemeral **session token** minted per accepted connection, used as the Registry-naming and data-stream routing key with nothing durable bound to it. This separation is what lets a reconnect re-point existing bindings instead of minting-evicting-reissuing, and it removes the concurrent-connect race by routing data streams per owning connection rather than through a shared "current runner" scalar.
## Requirements
### Requirement: Runner identity is two layers — durable stable identity and ephemeral session token

Runner identity SHALL be split into two values with distinct lifetimes:

1. **Stable identity** — owned by the SDK, durable across process restarts, and used by the controller as the **operative-binding key**. It SHALL be created once on the runner's first boot, derived for readability as `"<namespace>-<profile_hash>-<random_suffix>"` (where `profile_hash` is a stable hash of the runner's declared factory set), and persisted at `FLEZHA_RUNNER_ID_PATH` via an atomic write. The SDK SHALL present it on **every** connect (initial and reconnect) in the control init message. The controller SHALL trust the presented stable identity as the binding key.
2. **Session token** — minted by the controller on every accepted connection, unique by construction, ephemeral. It SHALL be the controller-side Registry-naming key and the data-stream routing key. The controller SHALL return it to the SDK in the assign command, and nothing durable SHALL be bound to it.

The boot-time hostname MAY be stamped into the stable identity's metadata for debuggability but SHALL NOT be load-bearing for identity (a container hostname is not stable across recreation). Identity SHALL NOT depend on any Kubernetes-specific node-name source; the same mechanism SHALL work identically under Kubernetes, plain Docker, and bare-host runs.

#### Scenario: stable identity persists across a process restart

- **GIVEN** a runner that booted, derived and persisted a stable identity at `FLEZHA_RUNNER_ID_PATH`
- **WHEN** the process restarts with the persistence path intact
- **THEN** it SHALL present the same stable identity on reconnect
- **AND** the controller SHALL re-point that stable identity's bindings onto the new session token

#### Scenario: session token is fresh per connection and is the routing key

- **GIVEN** a runner connecting to the controller
- **WHEN** the controller accepts the connection
- **THEN** it SHALL mint a fresh, unique session token and return it via the assign command
- **AND** controller-side Registry names and data-stream routing SHALL key on the session token, not on the stable identity

### Requirement: Data streams route by the owning connection, eliminating the concurrent-connect race

The controller SHALL route each incoming data stream to the `StreamChannel`s of the connection (runner session) that owns it, and SHALL NOT route data streams via a single shared "current runner" variable. On UDS this holds by construction — data streams arrive on per-session socket paths keyed by the session token. On QUIC, where each runner uses exactly one connection (so the connection maps 1:1 to its session token), the controller SHALL route by the connection identity the transport driver tags each data stream with; it SHALL maintain a per-connection binding (not a shared scalar) and SHALL drop the binding when the session ends so it stays bounded. No SDK-side data-stream tagging or wire-format change is required.

#### Scenario: two runners connecting concurrently over QUIC route correctly

- **GIVEN** two runners completing their control handshakes over the same QUIC server in close succession, each receiving its own session token
- **WHEN** each opens its data streams on its own connection
- **THEN** each runner's data streams SHALL bind to that runner's own `StreamChannel`s (routed by the owning connection identity)
- **AND** neither runner's streams SHALL be misrouted to the other (the previous last-writer-wins behaviour SHALL NOT occur)

### Requirement: Same stable identity on two live sessions resolves newest-wins

When a new session presents a stable identity that already has a currently-live session, the controller SHALL treat this as a takeover: it SHALL close the older session through the standard `{:runner_disconnected}` path and bind the stable identity's operatives to the newer session. The resolution SHALL be deterministic (newest wins), not dependent on an eviction race.

#### Scenario: a fresh process takes over from a not-yet-closed prior session

- **GIVEN** stable identity `S` has a live session `sess_old` whose connection has not yet been observed as closed
- **WHEN** a new connection presents stable identity `S` and receives `sess_new`
- **THEN** the controller SHALL close `sess_old` via the standard disconnect path
- **AND** SHALL bind `S`'s operatives to `sess_new`

#### Scenario: a misconfiguration sharing one persistence path is contained

- **GIVEN** two distinct runner processes wrongly configured to share one `FLEZHA_RUNNER_ID_PATH`, so both present stable identity `S`
- **WHEN** they connect such that both would be live under `S`
- **THEN** only the newest session SHALL hold `S`'s bindings; the older SHALL be closed
- **AND** the controller SHALL NOT serve two simultaneous live sessions for the same stable identity

### Requirement: The durable write path's owner binding follows takeover

Newest-wins takeover SHALL update each affected Entity's durable write-path
binding in the same transaction that re-binds the stable identity to the new
session, ordered unbind-old-session before bind-new-session, so no window exists
in which both sessions can forward Changes to the same Entity's WAL. A superseded
session's forwards SHALL stop at the binding (single state flip at takeover), with no
per-record ownership check added to the owning session's append path. The first
refused forward per (Entity, superseded session) episode SHALL emit
`[:flezha, :state, :stale_writer_refused]`; subsequent refusals in the episode
SHALL only increment a counter.

#### Scenario: Takeover leaves exactly one writable binding

- **GIVEN** stable identity `S` bound to session `sess_old` with Changes in flight
- **WHEN** a new session `sess_new` takes over `S`
- **THEN** `sess_old`'s channels are unbound from every affected Entity's write path before `sess_new`'s are bound, and no Change from `sess_old` lands in any Entity WAL after the unbind

#### Scenario: Refusal is episode-workflow telemetry

- **WHEN** a superseded session forwards many Changes after losing its binding
- **THEN** one `stale_writer_refused` event is emitted for the episode and a counter tracks the rest

### Requirement: Missing persistence degrades to fresh-identity-per-boot, never a hard failure

When the persistence path is unavailable (unset, unwritable, or not mounted), the SDK SHALL generate a fresh stable identity for the current boot, log the condition once at `warn`, and operate normally for the lifetime of the process. A subsequent restart SHALL present a new stable identity; the prior binding SHALL age out via `FLEZHA_RUNNER_KEEPALIVE`. The runner SHALL NOT refuse to start, and SHALL NOT crash, due to missing persistence.

#### Scenario: ephemeral runner with no mounted volume still runs

- **GIVEN** a runner started with no writable `FLEZHA_RUNNER_ID_PATH` (e.g. a dev container with no volume)
- **WHEN** it boots
- **THEN** it SHALL generate a fresh stable identity, log a single `warn`, and connect and run normally
- **AND** on restart it SHALL present a new stable identity and its prior binding SHALL age out via the keepalive window

### Requirement: Rust and Java SDKs present identity identically

Both the Rust and Java SDKs SHALL implement the stable-identity derivation, persistence, presentation, and session-token storage identically, so a runner of either language is indistinguishable to the controller. The cross-language test matrix SHALL exercise both over UDS and QUIC.

#### Scenario: a Java runner and a Rust runner are handled identically

- **GIVEN** one Rust runner and one Java runner, each with a persisted stable identity
- **WHEN** each connects, restarts, and reconnects
- **THEN** the controller SHALL apply the same binding-swap and takeover behaviour to both
- **AND** the cross-language matrix SHALL pass for both transports

### Requirement: A namespace hub's failure SHALL be refused to the connecting runner, never raised into a node-wide process

A call into one namespace's hub SHALL NOT terminate a process that serves the whole node.
`Flezha.Runner.FleetManager` and each `Flezha.Transport.Server` instance serve every
namespace on the node, and a hub that is gone, terminating, or mid-restart belongs to one
namespace only, so its failure SHALL reach them as a value and not as an exit.

Hub liveness SHALL therefore be guarded at the call and not only before it. A check that a
hub is alive is not a guarantee that it will still be alive when the call lands — a
terminating coordinator is still a registered name — so the seam that runs a hub
interaction SHALL answer its caller's dead-hub value both when the hub was already gone and
when the hub goes away during the call, for a coordinator exit and for a registry read that
raises alike.

A refusal earned this way SHALL be loud and SHALL name the namespace: the connect fails
with a structured error carrying the namespace, the transport counts it on
`[:flezha, :transport, :connect_rejected]` and closes the stream, and the containment
itself emits a signal carrying the hub and the reason. The runner SHALL be left able to
reconnect on its existing backoff. A hub failure SHALL NOT be turned into a connect that
succeeds without the runner's operative-insertion hook registered, because such a runner
never has an operative pid seeded into its stream channels.

This is a different rule from the node-wide quiesce refusal in
`controller-graceful-shutdown`, and does not replace it: that one reads a node-wide flag
before the connect begins, and is silent about a single namespace whose hub is being drained
while the node stays up.

#### Scenario: A connect landing on a terminating hub is refused, and the node keeps serving

- **GIVEN** a namespace whose hub coordinator has a runner-connect call queued on it
- **WHEN** that hub is torn down before the call is answered
- **THEN** `Flezha.Runner.FleetManager` SHALL remain the same running process, the transport
  server that dispatched the connect SHALL remain running, and the connect SHALL be answered
  with an error naming the namespace

#### Scenario: The refused runner is told, and reconnects

- **WHEN** a runner connect is refused because its namespace's hub is unavailable
- **THEN** the transport SHALL emit `[:flezha, :transport, :connect_rejected]` carrying the
  refusal reason and close the runner's stream, and the runner SHALL treat the close as a
  broken control channel and redial on its jittered backoff

#### Scenario: A hub that vanishes mid-call answers the same as one already gone

- **GIVEN** a caller reading or calling a hub through the guarded hub seam
- **WHEN** the hub's coordinator exits, or its registry table is removed, while the call is
  in flight
- **THEN** the caller SHALL receive the same value it receives for a hub that was already
  down, and the containment SHALL be reported rather than swallowed

### Requirement: Recovery-hook registration SHALL refuse a hub failure, never raise into a node-wide process

Registering a namespace's recovery hooks SHALL treat an unavailable, terminating or
raising hub as a refusal returned to its caller. It SHALL NOT allow the failure to
escape as an exit inside a process the whole node shares, because a hub that dies
inside the start-to-hooks window would otherwise take the node's runner acceptor
with it.

#### Scenario: The hub dies between start and hook registration

- **WHEN** `Boot.RecoveryHooks.register/1` calls a hub that is unavailable or
  terminating
- **THEN** the call SHALL answer an error for that namespace and every other
  namespace on the node SHALL keep running

#### Scenario: The refusal names the namespace it belongs to

- **WHEN** recovery-hook registration is refused
- **THEN** the refusal SHALL identify the namespace, so an operator can tell one
  namespace's failure from a node-wide one

