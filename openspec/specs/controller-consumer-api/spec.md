# controller-consumer-api Specification

## Purpose
TBD - created by archiving change publish-a-controller-consumer-api. Update Purpose after archive.
## Requirements
### Requirement: A consumer's controller calls SHALL resolve against this tree at build time

Every controller function a declared consumer calls SHALL be recorded in a manifest in
this repository, and the test suite SHALL resolve every manifest entry against this
tree's real exports. A consumer that reaches the controller by runtime name resolution
gets no compiler to tell it a name has moved, so the repository that moves the name is
where the break SHALL be reported.

An unresolved entry SHALL be reported in one of three classes, because the remedy
differs: **moved** when some module here defines a public function of that name and
arity, and the candidates SHALL be named; **privatised** when the name survives only as
a private function; **withdrawn** when the name appears nowhere.

The manifest SHALL record call sites and SHALL NOT record module names a consumer uses
as values, since a module named in data is not a call and reporting it is a false break.

Where a consumer checkout is available to the run, the manifest SHALL be regenerated
from it and any difference SHALL fail, so that a manifest cannot describe a consumer
that has moved on.

#### Scenario: A renamed module fails the run that renamed it

- **WHEN** a module a manifest entry names is renamed and the manifest is not updated
- **THEN** the suite SHALL fail, naming the entry, reporting it as **moved**, and naming
  every module here that defines a function of the same name and arity

#### Scenario: A function made private is not reported as present

- **WHEN** a manifest entry names a function that now exists only as a private function
- **THEN** the suite SHALL fail and report the entry as **privatised**, naming the module
  that holds the private definition

#### Scenario: A name with no successor is reported as withdrawn

- **WHEN** a manifest entry names a function that appears nowhere in this tree
- **THEN** the suite SHALL fail and report the entry as **withdrawn**, offering no
  successor

#### Scenario: A module named only as a value is not a call site

- **WHEN** a consumer names a controller module as a data value, such as a plugin
  provider in a test fixture
- **THEN** that name SHALL NOT appear in the manifest and SHALL NOT be reported

#### Scenario: A stale manifest is refused when the consumer is present

- **WHEN** the run is given a consumer checkout whose call sites differ from the manifest
- **THEN** the run SHALL fail, naming the entries added and removed

### Requirement: The controller SHALL expose a data service API that a consumer uses instead of internals

The controller SHALL expose, for each domain a declared consumer reads or mutates, a
service-API module whose every public function takes a `Flezha.Service.CallerContext` as
its last argument, authorizes through it before any effect, and answers **data** rather
than rendered text. `Flezha.Service.AuditLog` and `Flezha.Service.Observability` are the
existing instances of this shape and SHALL remain its reference.

A function that renders text for a terminal SHALL NOT be the only way to reach a fact,
because a consumer that is not a terminal then reads the fact from an internal module or
copies the derivation.

A function on this surface that establishes a subscription SHALL have its inverse on the
same surface, so that a consumer never has to leave the surface to undo what it did on
it.

The identity reads a consumer makes before it holds a context — verifying a token and
reading the enforcement posture — SHALL be the only published functions callable without
one, and SHALL be read-only.

A partial surface satisfies this requirement for the domains it covers; a domain with no
service-API module is unfinished, not conformant.

#### Scenario: A published mutation is authorized

- **WHEN** a consumer invokes a published mutating function with a context whose
  principal lacks the required role, while authorization is enforced
- **THEN** the mutation SHALL NOT take effect and the refusal SHALL be audited,
  identically to the same verb invoked from the CLI

#### Scenario: A published read answers data

- **WHEN** a consumer invokes a published read
- **THEN** the answer SHALL be a term the caller can navigate, and SHALL NOT be text
  formatted for a terminal

#### Scenario: A subscription can be undone on the surface

- **WHEN** a consumer subscribes through a published function
- **THEN** a published function SHALL undo that subscription, taking the same caller
  context

#### Scenario: Only the identity reads are context-free

- **WHEN** the published surface is enumerated
- **THEN** every function except token verification and the enforcement-posture read
  SHALL take a caller context as its last argument

### Requirement: A published surface name SHALL be retired only together with its consumers

A published surface name SHALL be retired only together with the manifest edit that stops
naming it — a rename, an arity change or a withdrawal lands in one change with its
consumers. Internal module and function names remain free to move; that freedom is what
the published surface exists to protect.

A rename that leaves a manifest entry unresolved SHALL fail the run that made it, rather
than being discovered by a consumer at runtime.

#### Scenario: A surface rename lands with its manifest

- **WHEN** a published function is renamed and every manifest naming it is updated in the
  same change
- **THEN** the suite SHALL pass

#### Scenario: An internal rename is not gated

- **WHEN** a module that no manifest names is renamed
- **THEN** the consumer gate SHALL report nothing

### Requirement: A published function SHALL bind a declared verb

Every published service function that takes a caller context SHALL bind one row of
the user-verb declaration and apply it through the one seam, so that the console
and the terminal are judged by the same action, in the same scope, with the same audit.
A published function SHALL NOT decide its own action, scope or audit inline, and a
terminal verb body SHALL reach an effect the published function wraps rather than the
published function itself, so that one request is judged once.

#### Scenario: The console and the terminal read one declaration

- **WHEN** the grant, scope rule or audit of a verb both surfaces carry is changed in
  its row
- **THEN** the next invocation from either surface SHALL reflect the change, with no
  edit to either surface

#### Scenario: A published function that bypasses the seam fails the suite

- **WHEN** a public function on a published module reaches an effect without binding a
  row
- **THEN** the declaration test SHALL fail naming that function, before any consumer can
  call it

#### Scenario: A request is judged once

- **WHEN** a terminal verb whose effect the published surface also carries is invoked
  with a valid credential
- **THEN** exactly one authorization decision and at most one audit record SHALL result

### Requirement: The published surface SHALL serve every caller that proves an identity

The published consumer surface SHALL be reachable by any caller the controller can attribute an identity to, and the way an identity is proven SHALL be the only thing that differs between them. A terminal proves it with a verified token, a console with a verified token over its own transport, and a runner connection with what its connection proved. Each SHALL resolve to a `Flezha.Service.CallerContext` before the surface is called, and no caller SHALL supply its own principal, namespace or role.

A caller SHALL NOT be given a separate dispatch table, a separate policy point or a hand-maintained list of the functions it may call. What a caller may reach SHALL follow from the role its identity resolves to, so that changing a row's grant changes every caller at once.

#### Scenario: One declaration judges every caller

- **WHEN** the grant, scope rule or audit of a verb is changed in its row
- **THEN** the next invocation from the terminal, the console or a runner
  connection SHALL reflect the change, with no edit to any of them

#### Scenario: A runner connection reaches the surface as itself

- **WHEN** a runner connection invokes a published function
- **THEN** the call SHALL be judged against the role its principal resolves to,
  in the namespace its connection proved, and SHALL produce the audit record its
  row declares

#### Scenario: A caller cannot name its own identity

- **WHEN** a request from any non-internal caller carries a principal, namespace
  or role in its payload
- **THEN** those values SHALL NOT reach the caller context, which SHALL be built
  only from what the caller proved

#### Scenario: Reachability follows the grant, not a list

- **WHEN** a caller names a published function its role does not grant
- **THEN** the call SHALL be refused at the policy point, and the refusal SHALL
  NOT depend on any per-caller list of permitted functions

