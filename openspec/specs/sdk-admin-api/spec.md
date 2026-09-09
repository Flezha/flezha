# sdk-admin-api Specification

## Purpose
TBD - created by archiving change let-an-application-administer-its-namespace. Update Purpose after archive.
## Requirements
### Requirement: An application administers its own namespace from runner code

The SDK SHALL offer, on the runner object rather than on a procedure context, a surface by which ordinary application code administers the namespace its connection proved. The surface SHALL cover starting, stopping and sending to an operative, enabling and disabling a plugin, and reading operatives, plugins and entity state. It SHALL require no procedure to be running and SHALL require the application to hold no credential of its own beyond what its connection already presented.

#### Scenario: An application starts an operative it implements

- **WHEN** application code on a connected runner asks to start an operative,
  giving the same specification `flz operative start` takes
- **THEN** the operative SHALL start in the namespace the connection proved

#### Scenario: The call needs no procedure

- **WHEN** the surface is called from application code with no procedure running
- **THEN** the call SHALL succeed on the same terms as any other

#### Scenario: The SDK presents no expiring credential

- **WHEN** an application uses the surface after running unattended for an
  arbitrarily long time without operator intervention
- **THEN** no credential held by the application SHALL have lapsed, and the call
  SHALL be judged exactly as it would have been on the first day

### Requirement: The act is judged and recorded like any other operator act

An SDK-issued act SHALL be judged at the same policy point, against the same declared row, in the same scope, and SHALL produce the same audit record as the operator-issued equivalent, naming the requesting principal. The controller SHALL reach the effect through the published consumer surface rather than through a path of its own, so that no second authority exists to keep in step.

#### Scenario: The same policy point judges it

- **WHEN** the same act is issued from the terminal and from application code,
  both authorized
- **THEN** exactly one authorization decision SHALL be made in each case, against
  the same row

#### Scenario: The audit names the requester

- **WHEN** an application starts an operative
- **THEN** the audit record SHALL name the requesting runner's principal and the
  namespace, and SHALL be indistinguishable in shape from an operator-issued one

#### Scenario: Quotas are enforced unchanged

- **WHEN** an application asks to start an operative that would exceed the
  namespace's `max_operatives`
- **THEN** the call SHALL be refused with the same quota reason an operator would
  receive

#### Scenario: A read writes no audit record

- **WHEN** an application reads operatives, plugins or entity state
- **THEN** the read SHALL be authorized and SHALL write no audit record, matching
  the declaration its row already carries

### Requirement: The request names no namespace and no principal

The request the SDK sends SHALL carry no namespace field and no principal field, and the controller SHALL fill both from what the connection proved. The unsafe case SHALL be unexpressible rather than validated.

#### Scenario: A foreign namespace cannot be expressed

- **WHEN** the wire form of an SDK administration request is inspected
- **THEN** it SHALL contain no field by which a caller could name a namespace or
  a principal

#### Scenario: The controller attributes the act to the connection

- **WHEN** the controller receives an SDK administration request
- **THEN** the namespace and principal it judges SHALL come from the connection's
  own established identity

### Requirement: Node-wide and auth-state administration is out of reach

An SDK caller SHALL NOT be able to install, upgrade or uninstall a plugin, stop the controller, delete or retune the retention of persisted state, or mutate any auth state. The refusal SHALL follow from the role's grants and the verb's scope, and SHALL NOT depend on a list of forbidden verbs maintained beside them.

#### Scenario: A node-wide verb is refused

- **WHEN** application code asks to install a plugin or stop the controller
- **THEN** the call SHALL be refused, and the refusal SHALL follow from the
  declared grant and scope rather than from an SDK-side check

#### Scenario: No allow-list stands between the caller and the policy

- **WHEN** the SDK administration path is inspected for a per-verb list of
  permitted or forbidden names
- **THEN** none SHALL exist, because reachability follows the role's grant set

### Requirement: A started operative outlives its requester

An operative started through this surface SHALL be owned by the cluster and not by the connection that asked for it. It SHALL survive the requesting runner's disconnection and SHALL be visible and stoppable by an operator.

#### Scenario: The requesting runner disconnects

- **WHEN** the runner whose application started an operative disconnects
- **THEN** the operative SHALL continue, subject to the same rebinding rules as
  one an operator started

#### Scenario: An operator can see and stop it

- **WHEN** an operator lists and stops operatives in that namespace
- **THEN** one started from application code SHALL appear and SHALL stop

### Requirement: Every SDK carries the surface over one shared entry

Every SDK SHALL expose the surface, and the hosts that share `flz_core` SHALL reach it through a single verb-keyed entry point rather than one entry per verb, so that adding a verb costs no change to the shared bindings. A deferred SDK SHALL be named, together with the follow-up change that owes it, in the manifest the conformance gate reads.

#### Scenario: A further verb costs no shared-binding change

- **WHEN** a verb is added to the surface
- **THEN** no new entry point, veneer wrapper or language glue SHALL be required
  in the shared bindings

#### Scenario: The declared surface matches the shipped one

- **WHEN** the conformance suite enumerates each SDK's administration surface
- **THEN** it SHALL match the declared set, and any deferral SHALL be named with
  the change that owes it

