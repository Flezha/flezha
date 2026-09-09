# runner-namespace-proof Specification

## Purpose
TBD - created by archiving change prove-the-namespace-and-recover-a-lost-admin. Update Purpose after archive.
## Requirements
### Requirement: A runner credential entitles a namespace and never expires

The controller SHALL hold, per namespace, a set of runner credentials, and a runner presenting one SHALL be entitled to that namespace and no other. A runner credential SHALL carry no expiry, and the registry SHALL refuse to issue one with an expiry, because the process holding it is unattended and a credential that lapses fails at a restart rather than at a convenient moment. No runner credential SHALL be issued for the `_system` namespace. The store SHALL keep only a hash of each credential, so that a copied store is not a credential, and the credential SHALL live in the token registry the operator surface already issues, lists and revokes through rather than in a store of its own.

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

- **WHEN** an operator asks for a runner credential with any expiry
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

- **WHEN** an operator issues a second credential, restarts each runner onto it,
  and retires the first
- **THEN** no runner SHALL have been refused at any point in the sequence, and
  the retired credential SHALL afterwards be refused

### Requirement: Revoking a credential closes the sessions it authenticated

The controller SHALL close the control channels authenticated with a runner credential when that credential is revoked. Refusing future connections alone SHALL NOT be sufficient, because a credential that never expires leaves nothing that would otherwise evict a holder.

#### Scenario: A live session is closed by revocation

- **WHEN** a runner is connected on a credential and an operator revokes that
  credential
- **THEN** the runner's control channel SHALL be closed without waiting for it to
  reconnect, and the closure SHALL be observable

#### Scenario: Sessions on other credentials are untouched

- **WHEN** one of a namespace's two live credentials is revoked
- **THEN** runners authenticated with the other SHALL remain connected

### Requirement: The operator surface issues, lists, rotates and revokes a credential

Every verb in a runner credential's lifecycle SHALL bind a row of the operator-verb declaration and be judged by the one seam, like every other operator verb, so that the grant, the scope and the audit of that lifecycle are declared once. Issuance SHALL return the credential exactly once and store only its hash. A rotation SHALL be expressible as an issue followed by a revoke, because a single atomic rotation verb could not leave both credentials live and would therefore disconnect a running fleet.

#### Scenario: A credential verb is judged like every other operator verb

- **WHEN** a caller without the granting role invokes a verb in the credential's
  lifecycle
- **THEN** the call SHALL be refused at the same policy point as any other
  operator verb, and the refusal SHALL be audited

#### Scenario: The raw credential is shown once

- **WHEN** an operator issues a runner credential
- **THEN** the raw value SHALL be returned exactly once and SHALL NOT be
  retrievable afterwards

### Requirement: An in-node runner proves its namespace by co-residence

A runner connected over the in-node transport SHALL be admitted to the namespace its frame names without presenting a credential, because it is code inside the controller's own release: it holds the distribution cookie and every file the credential lives in, and the in-node connect SHALL refuse a handle owned by another BEAM. This is the transport reading of the `:internal` caller source, and like that source it SHALL NOT extend to `_system`.

#### Scenario: An in-node runner needs no credential

- **WHEN** an in-BEAM SDK runner connects over the in-node transport naming
  `team-orders` while enforcement is on
- **THEN** the connection SHALL be admitted, and no credential SHALL have been
  required

#### Scenario: An in-node runner is still refused `_system`

- **WHEN** an in-BEAM SDK runner's connect frame names `_system`
- **THEN** the connection SHALL be refused

