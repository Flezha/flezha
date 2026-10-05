# runner-identity-and-session Specification

## Purpose
Splits Runner identity into two values with distinct lifetimes: a **stable identity** owned and persisted by the SDK, presented on every connect and used by the Controller as the relay-binding key; and an ephemeral **session token** minted per accepted connection, used as the Registry-naming and data-stream routing key with nothing durable bound to it. This separation is what lets a reconnect re-point existing bindings instead of minting-evicting-reissuing, and it removes the concurrent-connect race by routing data streams per owning connection rather than through a shared "current runner" scalar.
## Requirements
### Requirement: Runner identity is two layers — durable stable identity and ephemeral session token

Runner identity SHALL be split into two values with distinct lifetimes:

1. **Stable identity** — owned by the SDK, durable across process restarts, and used by the controller as the **relay-binding key**. It SHALL be created once on the runner's first boot, derived for readability as `"<namespace>-<profile_hash>-<random_suffix>"` (where `profile_hash` is a stable hash of the runner's declared factory set), and persisted at `FLEZHA_RUNNER_ID_PATH` via an atomic write. The SDK SHALL present it on **every** connect (initial and reconnect) in the control init message. The controller SHALL trust the presented stable identity as the binding key: the record of which runner a relay is bound to SHALL name the stable identity, SHALL be written by the relay's Overwatch alone, and SHALL be what every door that reads a frame naming the relay checks against.
2. **Session token** — minted by the controller on every accepted connection, unique by construction, ephemeral. It SHALL be the controller-side Registry-naming key and the data-stream routing key. The controller SHALL return it to the SDK in the assign command, and nothing durable SHALL be bound to it. The controller SHALL record which stable identity each session token belongs to for the session's lifetime, so a channel and an Overwatch can read a session's identity without calling the process that admitted it.

Beside the stable identity, the SDK SHALL present an **incarnation** in the same control init message: a random 128-bit value as lowercase hex, minted once per runner runtime start and never persisted, so a process that restarts, or a runner runtime whose supervisor restarts it, presents a new one while a process that only reconnects presents the one it presented before. The controller SHALL keep the incarnation with the stable identity's held session, and SHALL read a connect that presents a held identity with the held incarnation as the same process and any other incarnation, or none, as a new process. Every SDK runtime SHALL present the incarnation the same way: the shared runner core for the runtimes built on it, and the Elixir SDK on its own.

The boot-time hostname MAY be stamped into the stable identity's metadata for debuggability but SHALL NOT be load-bearing for identity (a container hostname is not stable across recreation). Identity SHALL NOT depend on any Kubernetes-specific node-name source; the same mechanism SHALL work identically under Kubernetes, plain Docker, and bare-host runs.

#### Scenario: stable identity persists across a process restart

- **GIVEN** a runner that booted, derived and persisted a stable identity at `FLEZHA_RUNNER_ID_PATH`
- **WHEN** the process restarts with the persistence path intact
- **THEN** it SHALL present the same stable identity and a new incarnation on reconnect
- **AND** the controller SHALL bind each of that stable identity's relays onto the new session through the one bind path, so the new process is told `AddRelay` with the relay's resume state and receives its persisted Cells and its undelivered inbox tail; the ownership record SHALL be unchanged, because the identity is

#### Scenario: a reconnecting process presents the incarnation it holds

- **GIVEN** a runner whose control connection dropped while its process, still running its relays, survived
- **WHEN** it reconnects presenting the same stable identity and the same incarnation
- **THEN** the controller SHALL re-point the identity's bindings onto the new session token by telling each relay's Overwatch its new session, which SHALL publish its own tag and route, and SHALL NOT send `AddRelay`, `RestoreInitState` or the inbox tail to the process for a relay it already runs

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

When a new session presents a stable identity that already has a currently-live session, the controller SHALL treat this as a takeover: it SHALL close the older session through the standard `{:runner_disconnected}` path and bind the stable identity's relays to the newer session. The resolution SHALL be deterministic (newest wins), not dependent on an eviction race. How the relays are bound SHALL follow the incarnation the new session presents: the held incarnation means the same process, and each relay's Overwatch is told its new session and publishes its tag and route; a different incarnation, or none, means a new process, and each relay's Overwatch binds it onto the new session through the one bind path, the same supervised bind task an un-park runs, carrying the relay's resume state. A takeover SHALL be refused when the identity's live or keepalive-preserved session was admitted under a credential and the new connect presents a different one: the connect SHALL end as a handshake refusal naming the identity, counted on `[:flezha, :transport, :connect_rejected]`, and the pin SHALL be released only when the identity's last session leaves the roster. A connect that carries no per-runner credential (a certificate-proven or posture-off connect) SHALL NOT be pinned.

#### Scenario: a fresh process takes over from a not-yet-closed prior session

- **GIVEN** stable identity `S` has a live session `sess_old` whose connection has not yet been observed as closed
- **WHEN** a new connection presents stable identity `S` with a new incarnation and receives `sess_new`
- **THEN** the controller SHALL close `sess_old` via the standard disconnect path
- **AND** SHALL bind each of `S`'s relays onto `sess_new` through the one bind path, so `sess_new` is told `AddRelay` for each with its resume state

#### Scenario: a durable steps relay mid-run comes back on the restarted process

- **GIVEN** a durable steps instance bound to session `sess_old` of identity `S`, with tasklet A committed and tasklet B in flight
- **WHEN** the runner process is killed and its replacement presents `S` with a new incarnation before the controller has evicted `sess_old`
- **THEN** the instance SHALL be bound onto the replacement with `task_journal`, `resume_cell_count` and `inbox_tail_count` set, tasklet A SHALL NOT run again on the replacement, and the instance SHALL reach its terminal completion

#### Scenario: a flow relay's inputs reach the restarted process

- **GIVEN** a flow relay bound to session `sess_old` of identity `S`
- **WHEN** the runner process is killed and its replacement presents `S` with a new incarnation before `sess_old` is evicted, and an input is sent after the replacement connected
- **THEN** the relay SHALL have been bound onto the replacement through the one bind path and the input SHALL produce its sink output

#### Scenario: a misconfiguration sharing one persistence path is contained

- **GIVEN** two distinct runner processes wrongly configured to share one `FLEZHA_RUNNER_ID_PATH`, so both present stable identity `S`
- **WHEN** they connect such that both would be live under `S`
- **THEN** only the newest session SHALL hold `S`'s bindings; the older SHALL be closed
- **AND** the controller SHALL NOT serve two simultaneous live sessions for the same stable identity

#### Scenario: a stable identity presented under another credential is refused while it is held

- **GIVEN** stable identity `S` admitted under credential `C1`, with a live or keepalive-preserved session
- **WHEN** a connect presents `S` under credential `C2`
- **THEN** the connect SHALL be refused naming `S`, `[:flezha, :transport, :connect_rejected]` SHALL count it, `S`'s relays SHALL stay bound to the `C1` session, and the same connect after `S`'s last session has left the roster SHALL be admitted and pinned to `C2`

### Requirement: The durable write path's owner binding follows takeover

Newest-wins takeover SHALL update each affected Cell's durable write-path
binding in the same transaction that re-binds the stable identity to the new
session, ordered unbind-old-session before bind-new-session, so no window exists
in which both sessions can forward Changes to the same Cell's WAL. A superseded
session's forwards SHALL stop at the binding (single state flip at takeover), with no
per-record ownership check added to the owning session's append path. The first
refused forward per (Cell, superseded session) episode SHALL emit
`[:flezha, :state, :stale_writer_refused]`; subsequent refusals in the episode
SHALL only increment a counter.

#### Scenario: Takeover leaves exactly one writable binding

- **GIVEN** stable identity `S` bound to session `sess_old` with Changes in flight
- **WHEN** a new session `sess_new` takes over `S`
- **THEN** `sess_old`'s channels are unbound from every affected Cell's write path before `sess_new`'s are bound, and no Change from `sess_old` lands in any Cell WAL after the unbind

#### Scenario: Refusal is episode-steps telemetry

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
reconnect on its existing backoff.

A runner's connect SHALL NOT call a running hub's coordinator: it names its namespace's
hub only to start one that is not running, so a hub torn down while a connect is in
flight cannot fail that connect.

This is a different rule from the node-wide quiesce refusal in
`controller-graceful-shutdown`, and does not replace it: that one reads a node-wide flag
before the connect begins, and is silent about a single namespace whose hub is being drained
while the node stays up.

#### Scenario: A connect whose hub cannot start is refused, and the node keeps serving

- **GIVEN** a namespace with no running hub, whose hub start fails
- **WHEN** a runner connects to it
- **THEN** `Flezha.Runner.FleetManager` SHALL remain the same running process, the transport
  server that dispatched the connect SHALL remain running, and the connect SHALL be answered
  with an error naming the namespace and the start's own error

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
- **THEN** the refusal SHALL identify the namespace, so a user can tell one
  namespace's failure from a node-wide one

### Requirement: A runner SHALL act only on the relays bound to its stable identity

The controller SHALL record, for every running relay, the stable identity of the
runner it is bound to, written by the relay's own Overwatch before the
`AddRelay` that starts it on that runner, and removed when the relay ends; a
suspended or unbound relay SHALL keep the identity it was last bound to until a
bind names another, so what that runner sent before the relay left it still
lands. A rebind SHALL record the identity of the runner it is about to tell to start
the relay as the owner before that attempt, as every bind does; the record SHALL
hold no pending identity, because the runner a rebind moves away from is dead, holds
no copy, or was proved not producing, and its copy is refused and removed when it
speaks. Every frame the controller reads that names
a relay (an output frame, the seven cell-write payloads, `correction_request`,
`RelayCompleted`, `IterPark`, `RunnerShutdownNotification`, `RelayExited`,
`TaskCommit` and `IterCommit`) SHALL be accepted only when the record names the sending session's
identity as the owner, or when no record exists for the relay. A
frame the record refuses SHALL NOT act on the relay: it SHALL be counted on the
sending channel, reported on `[:flezha, :runner, :foreign_frame_refused]` with the
runner, its identity, the recorded owner, the relay and the frame kind, and the
sending runner SHALL be told `RemRelay` for that relay once per session. A
refused durability barrier SHALL be acked with a non-empty error; a refused shutdown
notification SHALL still be acked; a refused completion SHALL write no record and
drive no teardown; a refused park SHALL journal nothing; a refused exit report SHALL park and rebind nothing. A runner presenting no stable
identity SHALL be its own session token for this rule. A route from a relay to
its Overwatch SHALL exist only in the channels of the session that owns it, written
by the same publish that writes the registry tag and removed from the previously
bound session's channels by it; no other mechanism SHALL seed a route.

#### Scenario: A runner evicted while alive keeps its copy, and the copy is refused when it speaks

- **GIVEN** relay `X` bound to runner A, which has armed a timer, and a second
  runner B advertising the same factory
- **WHEN** A is frozen, the controller is told A disconnected and moves `X` to B, B
  takes two more inputs, and A resumes and reconnects under the same stable identity
- **THEN** the first frame A's copy of `X` sends SHALL be refused and reported with
  A's new session and `X`, A SHALL be sent `RemRelay` for `X` once, no
  `[:flezha, :state, :wal_realign]` SHALL name A's new session, no output of A's copy
  SHALL reach `X`'s sinks, and `X`'s WAL SHALL keep the lineage B wrote

#### Scenario: A rebind names its target as the owner before the start

- **GIVEN** relay `X` bound to runner A, which the controller has declared disconnected, and a live runner B advertising the factory
- **WHEN** the convergence loop rebinds `X` onto B
- **THEN** the ownership record SHALL name B's identity before B is told `AddRelay`, and a frame A's late channels deliver for `X` after that SHALL be refused

#### Scenario: A control frame naming another runner's relay does nothing to it

- **GIVEN** relay `X` bound to runner A and a second session B in the namespace
- **WHEN** B sends `RelayCompleted`, `IterPark` or `RunnerShutdownNotification`
  naming `X`
- **THEN** `X` SHALL NOT be stopped, no completion record SHALL be written, no park
  SHALL be journaled for `X` or for an instance another namespace claims, `X` SHALL
  NOT be suspended, its stored snapshot SHALL be unchanged, and the shutdown
  notification SHALL still be acked to B

#### Scenario: A barrier from a runner that does not own the instance is answered with an error

- **WHEN** session B sends `TaskCommit` or `IterCommit` naming an instance bound to
  runner A
- **THEN** the response SHALL carry a non-empty error, the journal SHALL hold no entry
  for it, no inbox SHALL be truncated and no journal cut, and B SHALL be sent
  `RemRelay` for the instance once

#### Scenario: A data frame from a runner that does not own the relay is refused

- **WHEN** session B's stream channel, holding no route for `X`, reads an output frame
  or a cell-write frame naming `X` while the record names runner A
- **THEN** the frame SHALL NOT reach `X`'s Overwatch or its Cell's writer, the
  channel's `foreign_frame_refused` count SHALL rise, the event SHALL name B, A and
  `X`, and a second frame SHALL raise the count without a second `RemRelay`

#### Scenario: The owner's first output warms its route

- **WHEN** the owning session's channel reads an output frame for `X` before its
  route was seeded
- **THEN** the channel SHALL route it through the record's Overwatch and cache the
  route, and the frame SHALL NOT be refused or dropped

#### Scenario: A relay nobody records is handled as before

- **WHEN** a frame names a relay for which no record exists on this node
- **THEN** an output SHALL be the counted `:unrouted` drop it is today, a cell
  write SHALL reach its writer under the session fence alone, and a control frame
  SHALL be handled as one whose Overwatch is absent

#### Scenario: An exit report from a runner that does not own the instance parks nothing

- **GIVEN** relay `X` bound to runner A and a second session B in the namespace
- **WHEN** B sends `RelayExited` naming `X`
- **THEN** `X` SHALL NOT be parked or rebound, its Overwatch SHALL receive nothing,
  the refusal SHALL be counted and reported with B, A and `X`, and B SHALL be sent
  `RemRelay` for `X` once

### Requirement: A refused runner connect SHALL be answered with its error

When the controller refuses a runner's connect, whatever the cause (an unproven namespace, an unknown credential, a hub failure, an identity held by a live session), it SHALL first send the runner a `ConnectRefused` frame carrying the error, and only then close the stream. The error carries the catalogue code, class, message, details and hint. The runner SHALL surface that error to the author as its SDK's one error type, and SHALL log its code and message once per refused attempt. A runner SHALL NOT report a refused connect as connected.

#### Scenario: An unproven namespace is named

- **WHEN** a runner without a valid credential connects to a namespace that requires one
- **THEN** the runner SHALL receive `ConnectRefused` whose code is `runner.namespace_not_proven`, and its SDK SHALL surface that code and not report "Connected"

#### Scenario: The stream closes after the answer

- **WHEN** a connect is refused
- **THEN** the controller SHALL write the `ConnectRefused` frame before it closes the stream, and a runner that reads the stream to its end SHALL find the frame

