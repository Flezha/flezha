# sdk-admin-api Specification

## Purpose
TBD - created by archiving change let-an-application-administer-its-namespace. Update Purpose after archive.
## Requirements
### Requirement: An application administers its own namespace from runner code

The SDK SHALL offer, on the runner object rather than on a tasklet context, a surface reached as `control()` (`control` in Ruby, `$runner->control()` in PHP, `Flezha.SDK.Control` in Elixir) by which ordinary application code administers the namespace its connection proved. The surface SHALL cover starting and stopping an operator, enabling and disabling a plugin by name, binding a plugin to an operator and unbinding it, and reading operators, plugins and cell state. It SHALL not carry sending to an operator, which is on the runner object itself, and no SDK SHALL offer the surface under its former name `admin`. It SHALL require no tasklet to be running and SHALL require the application to hold no credential of its own beyond what its connection already presented. Starting SHALL take the same specification `flz operator start` takes: the factory, and an id only when the application chooses one. Enabling SHALL take the same specification `flz plugin enable` takes: the name, the provider and its options, and an operator only when the application binds in the same call.

#### Scenario: An application starts an operator it implements

- **WHEN** application code on a connected runner asks to start an operator,
  giving the same specification `flz operator start` takes
- **THEN** the operator SHALL start in the namespace the connection proved

#### Scenario: An application starts an operator without naming it

- **WHEN** application code asks to start an operator giving only its factory
- **THEN** the operator SHALL start in the namespace the connection proved under a
  generated id

#### Scenario: The call needs no tasklet

- **WHEN** the surface is called from application code with no tasklet running
- **THEN** the call SHALL succeed on the same terms as any other

#### Scenario: The SDK presents no expiring credential

- **WHEN** an application uses the surface after running unattended for an
  arbitrarily long time without user intervention
- **THEN** no credential held by the application SHALL have lapsed, and the call
  SHALL be judged exactly as it would have been on the first day

#### Scenario: An application shares a plugin between two operators

- **WHEN** application code enables a plugin by name and binds it to two operators
  it started
- **THEN** both operators SHALL be bound to the one plugin, and unbinding one SHALL
  leave the other bound and the plugin enabled

#### Scenario: The surface is reached as control

- **WHEN** application code asks a started runner for `control()` and starts an operator
  through it
- **THEN** the operator SHALL start, and the runner SHALL offer no `admin()`

### Requirement: The act is judged and recorded like any other user act

An SDK-issued act SHALL be judged at the same policy point, against the same declared row, in the same scope, and SHALL produce the same audit record as the user-issued equivalent, naming the requesting principal. The controller SHALL reach the effect through the published consumer surface rather than through a path of its own, so that no second authority exists to keep in step.

#### Scenario: The same policy point judges it

- **WHEN** the same act is issued from the terminal and from application code,
  both authorized
- **THEN** exactly one authorization decision SHALL be made in each case, against
  the same row

#### Scenario: The audit names the requester

- **WHEN** an application starts an operator
- **THEN** the audit record SHALL name the requesting runner's principal and the
  namespace, and SHALL be indistinguishable in shape from a user-issued one

#### Scenario: Quotas are enforced unchanged

- **WHEN** an application asks to start an operator that would exceed the
  namespace's `max_operators`
- **THEN** the call SHALL be refused with the code `quota.exceeded` and the same details
  (`kind`, `current`, `limit`) a user would receive from `flz`

#### Scenario: A read writes no audit record

- **WHEN** an application reads operators, plugins or cell state
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

### Requirement: A started operator outlives its requester

An operator started through this surface SHALL be owned by the cluster and not by the connection that asked for it. It SHALL survive the requesting runner's disconnection and SHALL be visible and stoppable by a user.

#### Scenario: The requesting runner disconnects

- **WHEN** the runner whose application started an operator disconnects
- **THEN** the operator SHALL continue, subject to the same rebinding rules as
  one a user started

#### Scenario: A user can see and stop it

- **WHEN** a user lists and stops operators in that namespace
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

### Requirement: Starting from application code SHALL answer the id and the outcome

Every SDK's start call SHALL take the factory first and the id as an optional named argument, in
the form its language has: `OperatorSpec::new(factory).id(..)` in Rust,
`OperatorSpec.of(factory).id(..)` in Java, a keyword list in Elixir, keyword arguments in Python
and Ruby, named arguments in PHP and an options object in Node. A successful start SHALL answer one
result carrying two fields, the same in every SDK: `id`, the id the operator runs under whether
the application gave it or the controller generated it, and `outcome`, one of `started` and
`already_running`. `already_running` SHALL be answered only for an id the application gave, when a
live instance already holds it. The answer SHALL carry nothing else; where an operator runs is read
through `flz operator info` or the read surface. A refused start SHALL be reported as the SDK's one error type, carrying the `code`,
`class`, `message`, `details` and `hint` the controller answered.

#### Scenario: A start without an id answers the generated id

- **WHEN** application code starts factory `echo` with no id
- **THEN** the call SHALL answer `id` set to the generated id and `outcome` set to `started`, and
  that id SHALL be the one `flz operator list` shows and a stop through the same surface accepts

#### Scenario: A start of a running id answers already running

- **WHEN** application code starts factory `echo` with id `my_echo` while a live instance holds
  `my_echo`
- **THEN** the call SHALL succeed answering `id` set to `my_echo` and `outcome` set to
  `already_running`, and the running instance SHALL observe no stop, restart or rebind

#### Scenario: The answer has one shape in every SDK

- **WHEN** each SDK's own suite starts an operator with and without an id through its wrapper
- **THEN** every SDK SHALL expose the same two fields, `id` and `outcome`, with the same outcome
  words, and no `node`, `runner_id` or `pid`

#### Scenario: A refused start carries its code

- **WHEN** application code asks to start an operator that would exceed the namespace's
  `max_operators`
- **THEN** the call SHALL be refused with the SDK's one error type, its `code` SHALL be
  `quota.exceeded` in every SDK, and its details SHALL name the quota kind, the current count
  and the limit

