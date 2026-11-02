# security-transport Specification

## Purpose

How the controller authenticates its peers. By default the runner transport verifies its peer
(client certificates over QUIC, an owner-only socket over UDS), the Erlang distribution uses a
provisioned cookie with an optional tested mutual-TLS setup and a warning on every boot when
unprotected, and CLI callers present a verifiable identity; running without authentication needs an
explicit opt-out. Where enforcement is configured, a runner proves its namespace with a credential
that never expires, can be rotated with more than one live at a time, and closes its sessions when
revoked, all managed through the operations surface.

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

### Requirement: An unprotected distribution link SHALL be warned on every boot

A controller SHALL log one warning on every boot when its node host is not a loopback address
(`127.0.0.0/8`, `::1`) or `localhost` and its distribution is not TLS, saying that traffic between
controllers is neither encrypted nor authenticated beyond the cookie, and naming
`FLEZHA_DIST_TLS_OPTFILE`. When distribution is TLS, the controller SHALL read the options file at
boot and SHALL log one warning on every boot for each of these that holds:

- the server section does not set `{verify, verify_peer}`, so peers are admitted without a
  certificate check;
- neither section sets `versions`, so only TLS 1.2 is offered;
- a section's `cacertfile` is the same file as the runner transport's trust anchor
  (`FLEZHA_QUIC_CLIENT_CA` or `FLEZHA_CA_CERT_PATH`).

An options file that cannot be read SHALL be warned naming its path. None of these conditions
SHALL stop the boot or refuse distribution. A loopback node with plain distribution SHALL NOT be
warned.

#### Scenario: Plain distribution off loopback is warned

- **WHEN** a controller boots as `flezha@10.0.0.5` without `FLEZHA_DIST_TLS_OPTFILE`
- **THEN** one warning naming `FLEZHA_DIST_TLS_OPTFILE` SHALL be logged, and the node SHALL start
  and accept distribution as before

#### Scenario: A loopback node is not warned

- **WHEN** a controller boots as `flezha@127.0.0.1` without `FLEZHA_DIST_TLS_OPTFILE`
- **THEN** no distribution warning SHALL be logged

#### Scenario: A TLS file that does not check peers is warned

- **WHEN** a controller boots with an options file whose server section has a certificate and
  key but no `{verify, verify_peer}`
- **THEN** one warning saying peers are admitted without a certificate check SHALL be logged

#### Scenario: A TLS file without versions is warned

- **WHEN** a controller boots with an options file that sets no `versions`
- **THEN** one warning saying only TLS 1.2 is offered SHALL be logged

#### Scenario: Reusing the runner trust anchor is warned

- **WHEN** a controller boots with an options file whose `cacertfile` is the file
  `FLEZHA_QUIC_CLIENT_CA` names
- **THEN** one warning saying distribution trusts the runner authority SHALL be logged

#### Scenario: The shipped example file is not warned

- **WHEN** a controller boots with the shipped example options file and certificates made by the
  shipped script
- **THEN** no distribution warning SHALL be logged

### Requirement: The repository SHALL ship a tested mutual-TLS setup for distribution

The repository SHALL ship an example distribution options file that sets, for both sections, a
node certificate, its key, a CA file, `{verify, verify_peer}` and `{versions, ['tlsv1.3']}`, and
for the server section `{fail_if_no_peer_cert, true}`. It SHALL ship a script that creates a
distribution certificate authority and node certificates carrying both the `serverAuth` and
`clientAuth` extended key usages and the given DNS names and IP addresses in the subject
alternative name, writing every private key readable by its owner only. A multi-node test SHALL
start controllers' distribution with the example file and certificates from the script and SHALL
prove each scenario below.

#### Scenario: A valid peer joins

- **WHEN** two nodes use the example file with certificates from one distribution authority and
  share the cookie
- **THEN** they SHALL connect, and a remote call between them SHALL succeed

#### Scenario: A peer from an unknown authority is refused

- **WHEN** a node presents a certificate from a different authority
- **THEN** the connection SHALL be refused during the TLS handshake

#### Scenario: A peer without a certificate is refused

- **WHEN** a node connects without a certificate
- **THEN** the connection SHALL be refused during the TLS handshake

#### Scenario: A certificate without the client usage is refused

- **WHEN** a node connects presenting a certificate from the distribution authority that carries
  only the `serverAuth` usage
- **THEN** the connection SHALL be refused

#### Scenario: A server certificate for another host is refused

- **WHEN** a node dials `flezha@a.example` and the answering node presents a certificate that
  does not name `a.example`
- **THEN** the dialling node SHALL refuse the connection

#### Scenario: A wrong cookie is refused over TLS

- **WHEN** a node with a valid certificate connects with a different cookie
- **THEN** the connection SHALL be refused after the TLS handshake

#### Scenario: A TLS 1.2 only peer is refused

- **WHEN** a node offering only TLS 1.2 connects to a node using the example file
- **THEN** the connection SHALL be refused

#### Scenario: A node cut off by rotating the authority is refused

- **GIVEN** three nodes connected under authority A
- **WHEN** two of them are restarted with a new authority B, new certificates and a new cookie,
  and the third keeps its certificate from A
- **THEN** the third SHALL NOT be able to connect to either of the other two

### Requirement: The distribution port SHALL be pinnable for controller starts

`FLEZHA_DIST_PORT`, when set to a port number, SHALL make a command that starts a controller node
listen for distribution on exactly that port, over plain TCP or TLS. It SHALL NOT apply to the
virtual machines behind `flzvm rpc`, `flzvm remote` or the CLI's local verbs. A value that is not
a port number SHALL stop the start naming the variable. Unset, the port SHALL be chosen as
before.

#### Scenario: A pinned port is used

- **WHEN** a controller starts with `FLEZHA_DIST_PORT=9100`
- **THEN** EPMD SHALL list the node at port 9100

#### Scenario: A remote shell does not take the pinned port

- **WHEN** `flzvm remote` runs on the same machine as a controller started with
  `FLEZHA_DIST_PORT=9100`
- **THEN** the remote shell SHALL reach the controller, and its own virtual machine SHALL NOT be
  given that port

#### Scenario: An invalid port is refused

- **WHEN** a controller starts with `FLEZHA_DIST_PORT=ninety`
- **THEN** the start SHALL fail with a message naming `FLEZHA_DIST_PORT`

### Requirement: Generated runner-transport keys SHALL be owner-only from creation

When the controller generates the runner transport's key files, each key file SHALL be readable
and writable by its owner only before any key material is written to it.

#### Scenario: A key is never readable by others

- **WHEN** the controller generates the runner transport certificate set
- **THEN** each key file SHALL have mode `0600` at the moment its first byte is written, and
  afterwards

### Requirement: A runner credential entitles a namespace and never expires

The controller SHALL hold, per namespace, a set of runner credentials, and a runner presenting one SHALL be entitled to that namespace and no other. A runner credential SHALL carry no expiry, and the registry SHALL refuse to issue one with an expiry, because the process holding it is unattended and a credential that lapses fails at a restart rather than at a convenient moment. No runner credential SHALL be issued for the `_system` namespace. The store SHALL keep only a hash of each credential, so that a copied store is not a credential, and the credential SHALL live in the token registry the operations surface already issues, lists and revokes through rather than in a store of its own.

#### Scenario: A credential entitles exactly its namespace

- **WHEN** a runner connects presenting a credential issued for `team-orders` and
  names `team-payments`
- **THEN** the connection SHALL be refused

#### Scenario: A credential does not lapse

- **WHEN** a runner reconnects with a credential issued arbitrarily long ago that
  has not been revoked
- **THEN** the connection SHALL be admitted, and no elapsed time SHALL by itself
  have made the credential invalid

#### Scenario: An expiring runner credential cannot be issued

- **WHEN** a user asks for a runner credential with any expiry
- **THEN** the request SHALL be refused, so that no deployment can put an
  unattended process behind a credential that lapses

#### Scenario: The store holds no credential in the clear

- **WHEN** the credential store is read from disk
- **THEN** no stored value SHALL be usable as a credential

### Requirement: A namespace holds more than one live credential so rotation is rolling

The controller SHALL admit a runner presenting any live credential for its namespace, so that a namespace may hold more than one at a time and a rotation is issue, roll, retire rather than a cutover. Rotating SHALL NOT require any running runner to be disconnected.

#### Scenario: Two live credentials are both admitted

- **WHEN** a namespace holds two live credentials and runners present one each
- **THEN** both SHALL be admitted to that namespace

#### Scenario: A rolling rotation drops no connection

- **WHEN** a user issues a second credential, restarts each runner onto it,
  and retires the first
- **THEN** no runner SHALL have been refused at any point in the sequence, and
  the retired credential SHALL afterwards be refused

### Requirement: Revoking a credential closes the sessions it authenticated

The controller SHALL close the control channels authenticated with a runner credential when that credential is revoked. Refusing future connections alone SHALL NOT be sufficient, because a credential that never expires leaves nothing that would otherwise evict a holder.

#### Scenario: A live session is closed by revocation

- **WHEN** a runner is connected on a credential and a user revokes that
  credential
- **THEN** the runner's control channel SHALL be closed without waiting for it to
  reconnect, and the closure SHALL be observable

#### Scenario: Sessions on other credentials are untouched

- **WHEN** one of a namespace's two live credentials is revoked
- **THEN** runners authenticated with the other SHALL remain connected

### Requirement: The operations surface issues, lists, rotates and revokes a credential

Every verb in a runner credential's lifecycle SHALL bind a row of the user-verb declaration and be judged by the one seam, like every other administration verb, so that the grant, the scope and the audit of that lifecycle are declared once. Issuance SHALL return the credential exactly once and store only its hash. A rotation SHALL be expressible as an issue followed by a revoke, because a single atomic rotation verb could not leave both credentials live and would therefore disconnect a running fleet.

#### Scenario: A credential verb is judged like every other administration verb

- **WHEN** a caller without the granting role invokes a verb in the credential's
  lifecycle
- **THEN** the call SHALL be refused at the same policy point as any other
  administration verb, and the refusal SHALL be audited

#### Scenario: The raw credential is shown once

- **WHEN** a user issues a runner credential
- **THEN** the raw value SHALL be returned exactly once and SHALL NOT be
  retrievable afterwards

### Requirement: A runner's namespace SHALL be proven whenever enforcement is configured

The namespace a connecting runner claims SHALL be proven by its certificate or its credential
whenever authorization is configured on, including while the bootstrap is not yet claimed. Only
under the open posture (authorization configured off) SHALL the claimed namespace be taken as
given.

#### Scenario: Before the claim, an unproven namespace is refused

- **GIVEN** authorization is configured on and the bootstrap is not claimed
- **WHEN** a runner connects claiming namespace `b` with a credential for namespace `a`
- **THEN** the connection SHALL be refused, and no session SHALL exist in `b`

#### Scenario: The open posture takes the claim

- **GIVEN** authorization is configured off
- **WHEN** a runner connects claiming namespace `b` with no credential
- **THEN** it SHALL be admitted to `b`
