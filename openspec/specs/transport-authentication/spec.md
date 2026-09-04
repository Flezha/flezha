# transport-authentication Specification

## Purpose

The transport-authentication capability makes every peer-facing surface of the
Flezha Controller authenticate its peer by default: the runner transport (QUIC
client-certificate verification against a configured trust anchor; owner-only
UDS socket and directory), the Erlang distribution (operator-provisioned
cookie, optional TLS distribution), and the CLI (verifiable token identity
threaded into the service wrappers). Running without authentication requires an
explicit opt-out that is warned on every boot.
## Requirements
### Requirement: Runner transport authenticates its peer

The runner transport (QUIC and UDS) SHALL authenticate the connecting peer before
accepting any control or data frame. The QUIC server SHALL verify a client
certificate against a configured trust anchor; the UDS control socket SHALL be
created with owner-only permissions on a directory not world-writable. A connection
that fails authentication SHALL be refused and the refusal SHALL be observable.

The UDS access boundary MAY be widened to a single named group, and only so, by setting
`FLEZHA_SOCKET_DIR_GROUP` to a group the controller's user belongs to: the socket directory is
then created group-readable and group-executable (`0750`) and owned by that group, and the
controller SHALL log a warning naming the group on every boot while the variable is set. Unset,
the directory remains owner-only. No setting SHALL make the directory world-readable or
world-writable.

#### Scenario: Unauthenticated QUIC peer is refused

- **WHEN** a peer opens a QUIC session presenting no client certificate or one not
  signed by the configured trust anchor
- **THEN** the handshake is rejected, no operative-control frame is processed, and a
  refusal is recorded

#### Scenario: UDS socket is not world-accessible

- **WHEN** the controller creates the runner control socket
- **THEN** the socket and its parent directory permit access only to the owning user, unless the
  group mode is configured, and in no case to any other user

#### Scenario: The group mode is explicit and warned

- **WHEN** the controller boots with `FLEZHA_SOCKET_DIR_GROUP` naming a group
- **THEN** the socket directory is created `0750` owned by that group, a warning naming the group
  is logged, and a runner whose user is in that group connects while a user outside it is refused

#### Scenario: An unusable group is refused at boot

- **WHEN** `FLEZHA_SOCKET_DIR_GROUP` names a group that does not exist or that the controller's
  user does not belong to
- **THEN** the controller does not boot, and the refusal names the group

### Requirement: Erlang distribution is not trust-by-constant-cookie

The release SHALL NOT ship a hard-coded default distribution cookie, and the cluster
SHALL support TLS distribution. A deployment that has not provisioned a cookie
(and, where enabled, distribution TLS) SHALL refuse to form a cluster rather than
fall back to a well-known constant.

#### Scenario: Missing cookie refuses to cluster

- **WHEN** a controller boots without a provisioned `RELEASE_COOKIE`
- **THEN** it does not fall back to a source-committed constant and does not accept
  distribution connections under a default cookie

### Requirement: CLI callers present a verifiable identity

The `flz` CLI SHALL accept a token (`--token` flag or `FLEZHA_TOKEN` environment
variable) and pass it through to the controller in one place: the trailing options
list every verb's request ends with, appended once by the one function that issues
every RPC. No verb SHALL carry the credential another way, and no verb SHALL omit it,
so that a verb cannot be gated on the controller and token-less on the wire. The
controller SHALL resolve it via the token registry into the `caller_context` principal
at one place. While authorization is enforced, a gated verb invoked without a
resolvable principal SHALL be refused with a message naming the token requirement;
`flz auth bootstrap` SHALL remain callable without a prior token.

#### Scenario: Token resolves to the CLI principal

- **WHEN** `FLEZHA_TOKEN` carries a valid token bound to `operator@team-orders` and
  a gated verb targeting `team-orders` is invoked
- **THEN** the service call runs with `caller_context.principal =
  operator@team-orders` and is authorized against that principal

#### Scenario: Gated verb without a token is refused

- **WHEN** authorization is enforced and a destructive verb is invoked with no
  token supplied
- **THEN** the verb is refused naming the missing credential, and the denial is
  audited

#### Scenario: Every verb's request carries the token it was given

- **WHEN** each verb `flz` builds is invoked with `--token <t>` against a shim that
  echoes the request
- **THEN** every echoed request SHALL end with an options list carrying `token: "<t>"`,
  and a verb whose request does not SHALL fail the CLI's own test

#### Scenario: A verb that carries a JSON document still carries the token beside it

- **WHEN** `flz plugin install`, `plugin upgrade` or `operative scale` is invoked with a
  token
- **THEN** the JSON document SHALL travel unchanged and the token SHALL ride the
  trailing options list, never inside the document

### Requirement: Secure default posture

The runner transport SHALL default to requiring peer authentication. Running without
authentication SHALL require an explicit opt-out that logs a warning on every boot.

#### Scenario: Wide-open start is explicit and loud

- **WHEN** an operator disables transport authentication
- **THEN** the disable requires an explicit setting and a warning is emitted on each
  boot for as long as it remains disabled

