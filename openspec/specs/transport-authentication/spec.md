# transport-authentication Specification

## Purpose

The transport-authentication capability makes every peer-facing surface of the
Flezha Controller authenticate its peer by default: the runner transport (QUIC
client-certificate verification against a configured trust anchor; owner-only
UDS socket and directory), the Erlang distribution (user-provisioned
cookie, optional TLS distribution), and the CLI (verifiable token identity
threaded into the service wrappers). Running without authentication requires an
explicit opt-out that is warned on every boot.
## Requirements
### Requirement: Runner transport authenticates its peer

The runner transport (QUIC and UDS) SHALL authenticate the connecting peer before accepting any control or data frame, and the authentication SHALL establish which namespace the peer is entitled to rather than only that the peer is admissible. The QUIC server SHALL verify a client certificate against a configured trust anchor and SHALL make that certificate available to the connect path, so the namespace it names can be read. The UDS control socket SHALL be created with owner-only permissions on a directory not world-writable, and because one socket serves every namespace, a UDS peer's namespace entitlement SHALL come from a presented credential rather than from the socket. A connection that fails authentication SHALL be refused and the refusal SHALL be observable.

The UDS access boundary MAY be widened to a single named group, and only so, by setting
`FLEZHA_SOCKET_DIR_GROUP` to a group the controller's user belongs to: the socket directory is
then created group-readable and group-executable (`0750`) and owned by that group, and the
controller SHALL log a warning naming the group on every boot while the variable is set. Unset,
the directory remains owner-only. No setting SHALL make the directory world-readable or
world-writable.

#### Scenario: Unauthenticated QUIC peer is refused

- **WHEN** a peer opens a QUIC session presenting no client certificate or one not
  signed by the configured trust anchor
- **THEN** the handshake is rejected, no relay-control frame is processed, and a
  refusal is recorded

#### Scenario: The peer certificate reaches the connect path

- **WHEN** a QUIC peer completes a handshake presenting a client certificate
- **THEN** the connect path SHALL be able to read that certificate, so that the
  namespace it names is available before the connection is admitted

#### Scenario: UDS socket is not world-accessible

- **WHEN** the controller creates the runner control socket
- **THEN** the socket and its parent directory permit access only to the owning user, unless the
  group mode is configured, and in no case to any other user

#### Scenario: A UDS peer's namespace is not implied by the socket

- **WHEN** two peers entitled to different namespaces connect over the same UDS
  control socket
- **THEN** each SHALL be admitted only to the namespace its presented credential
  entitles, and reaching the socket SHALL entitle a peer to no namespace by
  itself

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
SHALL support TLS distribution. A deployment that has not provisioned a cookie SHALL
NOT fall back to a well-known constant and SHALL NOT accept distribution connections
under a value any other installation could hold. Where no cookie is provisioned, the
release MAY mint a node-local cookie from a cryptographically random source, persist
it readable only by its owner, and reuse it on later boots; such a value SHALL be
unique to the node that minted it, SHALL NOT appear in the source tree, and SHALL
leave the node unable to cluster with any node that did not receive it explicitly.
Only a release command that starts a node SHALL mint and persist that value. Every
other release command, including `flzvm remote` and the `flzvm eval` behind the CLI's local
verbs, SHALL only read it and SHALL NOT mint or change it;
with nothing to read it SHALL persist nothing, and its call SHALL reach no node and
report the node unreachable. A node start that can neither be given nor mint a cookie
SHALL refuse to start distribution rather than proceed without one, and SHALL say so. The cookie SHALL NOT appear on the command line of any process the release
starts, the node itself or `flzvm remote`, because any local user can read a
command line; each VM SHALL be given it through an owner-only cookie file of its own, written from
`RELEASE_COOKIE` or the minted value, which the runtime reads before distribution starts,
while the processes the node spawns see the user's own home directory.

#### Scenario: Missing cookie refuses to cluster

- **WHEN** a controller boots without a provisioned `RELEASE_COOKIE`
- **THEN** it does not fall back to a source-committed constant and does not accept
  distribution connections under a default cookie

#### Scenario: A minted cookie is unique to its node

- **WHEN** two controllers each boot with no provisioned `RELEASE_COOKIE` and each mint one
- **THEN** the two values SHALL differ, and neither node SHALL be able to form a cluster with the other

#### Scenario: A provisioned cookie is never overridden

- **WHEN** a controller boots with `RELEASE_COOKIE` set
- **THEN** that value SHALL be used, no cookie SHALL be minted, and the minted cookie file
  SHALL NOT be read

#### Scenario: A minted cookie survives a restart

- **WHEN** a controller that minted a cookie is restarted with its state directory intact
- **THEN** it SHALL reuse the same value rather than mint a new one

#### Scenario: A cookie that cannot be minted refuses distribution

- **WHEN** a node starts with no cookie provisioned and the release cannot generate or persist one
- **THEN** it SHALL start with distribution disabled, SHALL log the reason, and SHALL NOT fail the boot

#### Scenario: A CLI verb run before the first start creates no cookie

- **GIVEN** no cookie is provisioned and no node has started with this cookie file
- **WHEN** a CLI verb runs
- **THEN** no cookie file SHALL be written, and a verb that reaches a controller SHALL report it
  unreachable

#### Scenario: A remote shell racing a first start leaves the node's cookie in the file

- **WHEN** `flzvm remote` has found no cookie file and a node's first start writes one before it finishes
- **THEN** the file SHALL hold the value the node started with, and a later `flzvm remote` SHALL read that value

#### Scenario: No command line carries the cookie

- **WHEN** a controller is running and `flzvm remote` is connected to it
- **THEN** neither `/proc/<pid>/cmdline` of the controller nor that of the `flzvm remote` process SHALL hold the cookie, and the remote shell SHALL still reach the controller

### Requirement: CLI callers present a verifiable identity

The `flz` CLI SHALL accept a token (`--token` flag or `FLEZHA_TOKEN` environment
variable) and pass it through to the controller in one place: the `opts` object of the
`cli_request` frame every controller request sends, filled once by the one function that sends
every request. The token SHALL travel only inside that frame, over the owner-only control
socket, and SHALL NOT appear on any command line or in any child process's environment or
stdin. No verb SHALL carry the credential another way, and no verb that sends a request SHALL
omit it, so that a verb cannot be gated on the controller and token-less on the wire. The
verbs that run in a VM of their own and reach no controller (`flz plugin install`,
`flz plugin uninstall`, `flz plugin sync`, and `flz mcp`'s knowledge tools while no controller
can be reached) send no request and carry no token; write access to the plugin root is the
plugin verbs' gate. The controller SHALL resolve the token via the token registry into the
`caller_context` principal at one place. While authorization is enforced, a gated verb invoked
without a resolvable principal SHALL be refused with a message naming the token requirement;
`flz auth bootstrap` SHALL remain callable without a prior token, and SHALL read the one-time
bootstrap token from its own standard input, never from an argument.

#### Scenario: Token resolves to the CLI principal

- **WHEN** `FLEZHA_TOKEN` carries a valid token bound to `ops@team-orders` and
  a gated verb targeting `team-orders` is invoked
- **THEN** the service call runs with `caller_context.principal =
  ops@team-orders` and is authorized against that principal

#### Scenario: Gated verb without a token is refused

- **WHEN** authorization is enforced and a destructive verb is invoked with no
  token supplied
- **THEN** the verb is refused naming the missing credential, and the denial is
  audited

#### Scenario: Every verb's request carries the token it was given

- **WHEN** each verb `flz` builds a request for is invoked with `--token <t>` against a
  stand-in socket server that records the request
- **THEN** every recorded `cli_request` SHALL carry `<t>` as `opts.token`, and a verb whose
  request does not SHALL fail the CLI's own test

#### Scenario: The token never rides a command line

- **WHEN** a gated verb, or `flz mcp` answering a line, runs with `FLEZHA_TOKEN` set while a
  controller runs
- **THEN** `flz` SHALL start no child process for the request, and the token SHALL appear only
  in the `cli_request` frame

#### Scenario: A verb that carries a JSON document still carries the token beside it

- **WHEN** `flz plugin enable` or `relay scale` is invoked with a token
- **THEN** the JSON document SHALL travel unchanged in `args` and the token SHALL ride
  `opts`, never inside the document

#### Scenario: A verb that reaches no controller starts no RPC

- **WHEN** `flz plugin install ./p.fpz --token <t>` is invoked against a shim that echoes
  every `flzvm` invocation, with a stand-in socket server listening
- **THEN** the shim SHALL see one `eval`, the server SHALL receive no request, and the token
  SHALL appear in neither

#### Scenario: The bootstrap token is read from stdin

- **WHEN** `flz auth bootstrap < bootstrap.token` runs against a controller with an unclaimed bootstrap
- **THEN** the claim succeeds, `flz`'s command line does not hold the token, and no child
  process is started

### Requirement: Secure default posture

The runner transport SHALL default to requiring peer authentication. Running without
authentication SHALL require an explicit opt-out that logs a warning on every boot.

#### Scenario: Wide-open start is explicit and loud

- **WHEN** a user disables transport authentication
- **THEN** the disable requires an explicit setting and a warning is emitted on each
  boot for as long as it remains disabled

