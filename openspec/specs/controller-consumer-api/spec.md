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
than rendered text. `Flezha.Service.Logs` and `Flezha.Service.Observability` are the
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
consumers. A consumer kept in another repository whose change is its maintainer's SHALL instead
have the retired call recorded in the consumer gate's record, with the name that replaces it, in
the change that retires it. Internal module and function names remain free to move; that freedom is what
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
the user-verb declaration and apply it through the one seam, so that the console, the
terminal and every other surface are judged by the same action, in the same scope, with the
same audit. A published function SHALL NOT decide its own action, scope or audit inline. No
surface SHALL bind a row of its own: every surface SHALL call the published function, so
that one request is judged once, by the published function's binding.

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

- **WHEN** a terminal verb is invoked with a valid credential
- **THEN** exactly one authorization decision and at most one audit record SHALL result

### Requirement: The published surface SHALL serve every caller that proves an identity

The published consumer surface SHALL be reachable by any caller the controller can attribute an identity to, and the way an identity is proven SHALL be the only thing that differs between them. A terminal proves it with a verified token, a console with a verified token over its own transport, an HTTP caller with a verified bearer token, and a runner connection with what its connection proved. Each SHALL resolve to a `Flezha.Service.CallerContext` before the surface is called, and no caller SHALL supply its own principal, namespace or role.

Every remote caller SHALL enter the surface through one door, `Flezha.Service.Admin`, which resolves an operation by its name, converts its arguments by the catalogue and applies the published function. A caller SHALL NOT be given a separate dispatch table, a separate policy point or a hand-maintained list of the functions it may call. What a caller may reach SHALL follow from the role its identity resolves to, so that changing a row's grant changes every caller at once.

#### Scenario: One declaration judges every caller

- **WHEN** the grant, scope rule or audit of a verb is changed in its row
- **THEN** the next invocation from the terminal, the console, HTTP or a runner
  connection SHALL reflect the change, with no edit to any of them

#### Scenario: A runner connection reaches the surface as itself

- **WHEN** a runner connection invokes a published function
- **THEN** the call SHALL be judged against the role its principal resolves to,
  in the namespace its connection proved, and SHALL produce the audit record its
  row declares

#### Scenario: The terminal enters through the door

- **WHEN** `flz dlq list` is run with a valid token
- **THEN** the controller SHALL serve it by `Flezha.Service.Admin` resolving `Dlq.list`,
  and by no CLI-specific binding

#### Scenario: A caller cannot name its own identity

- **WHEN** a request from any non-internal caller carries a principal, namespace
  or role in its payload
- **THEN** those values SHALL NOT reach the caller context, which SHALL be built
  only from what the caller proved

#### Scenario: Reachability follows the grant, not a list

- **WHEN** a caller names a published function its role does not grant
- **THEN** the call SHALL be refused at the policy point, and the refusal SHALL
  NOT depend on any per-caller list of permitted functions

### Requirement: A published function SHALL answer a failure as `%Flezha.Error{}`

Every function on the published consumer surface SHALL answer a failure as `{:error, %Flezha.Error{}}`, carrying the catalogue code, the class, the message, the details and the hint. It SHALL never answer a raw reason term or a text reason. This includes the two context-free functions: `Flezha.AuthZ.TokenStore.verify/1` answers `authz.token_*` codes. The conversion SHALL be made once, at `Flezha.AuthZ.Verb.run/5` for every bound function. Internal modules keep their reason terms.

#### Scenario: A consumer matches on the code

- **WHEN** a consumer calls `Flezha.Service.Dlq.drop/2` with an entry that does not exist
- **THEN** it SHALL receive `{:error, %Flezha.Error{code: "dlq.not_found", class: :not_found}}`

#### Scenario: No raw term reaches a consumer

- **WHEN** the consumer-surface test drives each published module's refusals
- **THEN** every `{:error, _}` answer SHALL carry a `%Flezha.Error{}` whose code is in the catalogue

### Requirement: Every CLI verb SHALL be a published function

Every `flz` verb that reaches a controller SHALL be served by a function on the published
consumer surface. No fact or effect the CLI offers SHALL be reachable only through the CLI.

#### Scenario: A former CLI-only verb is on the surface

- **WHEN** the published surface is enumerated
- **THEN** it SHALL carry a function for each of token issue, list and revoke, role bind,
  list and unbind, `whoami`, schedule run, state get, list, delete, rotate, retention and
  outbox, relay listen and replay, plugin update and info, boot status and await,
  controller status, memory top and stop, and cluster status

### Requirement: A consumer on another node SHALL hold a stream operation with data alone

A published stream operation SHALL accept a pid as the caller context's `:emit` — in
`Relay.listen/5`, `Observability.tail/2`, `AuditLog.tail/2` and `Controller.stop/2` — and SHALL
then send each event to it as `{:flezha_emit, event}`, so a consumer on another node needs no code of its own
on the controller. The operation SHALL watch that pid and end when it exits, including when its
node disconnects, releasing whatever it subscribed. A function as `:emit` SHALL keep working as
before, watching the caller's group leader. One function, `Flezha.Service.CallerContext.emitter/1`,
SHALL resolve `:emit` for every operation.

#### Scenario: A remote pid receives a tail

- **WHEN** a process on another node calls `Observability.tail/2` over `:erpc` with its own pid
  as `:emit` and a signal of that source is emitted
- **THEN** the pid receives `{:flezha_emit, signal}`

#### Scenario: The subscriber exits

- **WHEN** the pid named as `:emit` exits while a tail or a listen it asked for is running
- **THEN** the operation ends, answers `{:ok, :caller_down}`, and no subscription or listener
  plugin it took is left behind

#### Scenario: A window bounds a tail

- **WHEN** a tail's context carries `window_ms`
- **THEN** the tail ends with `{:ok, :window_closed}` once that window has run out

#### Scenario: A caller without `:emit`

- **WHEN** in-node code calls `Observability.tail/2` or `AuditLog.tail/2` with no `:emit`
- **THEN** the calling process is subscribed, as before

### Requirement: A forwarded tail SHALL hand on only what the caller may read

A tail that forwards to an `:emit` SHALL drop every signal `Observability.query/2` would not
answer the same caller, and every audit record `AuditLog.read/2` would not, applying the same
rule rather than a copy of it, so data a caller may not read never leaves the controller.

#### Scenario: Another namespace's signal

- **WHEN** a caller bound to namespace `a` forwards a tail and a signal whose metadata names
  namespace `b` is emitted
- **THEN** the signal is not sent to its `:emit`

#### Scenario: Another namespace's audit record

- **WHEN** a caller bound to namespace `a` forwards the audit tail and a record of namespace `b`
  is written
- **THEN** the record is not sent to its `:emit`

### Requirement: Every call the console makes SHALL be on the published surface

Every entry of `docs/internal/api/consumers/flezha_console.txt` SHALL be a function listed in
`docs/internal/api/consumers/published.txt` or one of the context-free reads (`Flezha.Service.Admin.context/2`, and, until the console
calls it, `Flezha.AuthZ.TokenStore.verify/1`, plus `Flezha.AuthZ.enforcement_posture/0`), and the
consumer-surface test SHALL assert it. Its record of console calls known not to resolve SHALL hold
only calls this tree retired while the console still names them, each with the operation that
replaces it, and SHALL be emptied by the manifest regenerated from the console that no longer
names them.

#### Scenario: The record is checked against the surface

- **WHEN** `test/flezha/api/consumer_surface_test.exs` runs
- **THEN** it fails naming any console call that is neither published nor context-free

#### Scenario: A retired operation the console still calls is recorded with its survivor

- **WHEN** `Relay.list_relays` is withdrawn while `docs/internal/api/consumers/flezha_console.txt` still names <!-- docs-citation-record:begin -->`Flezha.Service.Relay.list_relays/2`<!-- docs-citation-record:end -->
- **THEN** the record SHALL name that call and `Relay.list`, the test SHALL pass, and a regenerated manifest that no longer names it SHALL fail the test until the record entry is removed

### Requirement: A caller SHALL be able to read which operations its role grants

`Flezha.Service.Namespace.whoami/1` SHALL answer, beside the principal, namespace and role,
`operations`: the names of the operations `Flezha.Service.Admin.catalogue/1` answers for the
same caller context. The answer SHALL NOT change how any call is judged. `flz auth whoami`
SHALL print the same text as before.

#### Scenario: A viewer's operations

- **WHEN** a caller bound as `viewer` calls `whoami/1` while authorization is enforced
- **THEN** `operations` lists exactly the names `Admin.catalogue/1` answers for that context
- **AND** it includes `Relay.list` and excludes `Relay.start`

#### Scenario: An unbound caller under the open posture

- **WHEN** a caller with no role calls `whoami/1` while authorization is open
- **THEN** `operations` lists every operation that caller reaches

#### Scenario: The CLI text

- **WHEN** an operator runs `flz auth whoami`
- **THEN** it prints the principal, namespace and role lines and nothing more

### Requirement: A relay class's history retention SHALL be readable

`Flezha.Service.Steps.cells/2` SHALL answer `history_retention_hours`, the write-ahead history
retention the relay class runs with, resolved as `Flezha.State.PersistenceResolver.retention_hours/1`
resolves it, so a value set through `Steps.set_retention/3` is read back.

#### Scenario: The default

- **WHEN** no retention was set for a relay class
- **THEN** `cells/2` answers the configured default

#### Scenario: A value set since boot

- **WHEN** `set_retention/3` set a relay class's retention to 7 hours
- **THEN** `cells/2` for that class answers `history_retention_hours: 7`

### Requirement: An enabled plugin SHALL name the provider id it was enabled with

Each row `Flezha.Service.PluginManager.enabled/2` answers SHALL carry `provider_id`, the id
its provider is registered under in `Flezha.Plugin.Providers`, which `Fleet.provider/2` and
`PluginManager.enable/3` take, beside `provider`, the provider's name. A provider whose
package this controller did not load SHALL answer `provider_id: nil`.

#### Scenario: A plugin of a first-party provider

- **WHEN** a plugin enabled with the provider `relay_websocket` is listed
- **THEN** its row answers `provider_id: "relay_websocket"`

#### Scenario: A provider this controller did not load

- **WHEN** a plugin's provider module is registered under no id on this controller
- **THEN** its row answers `provider_id: nil`

### Requirement: A deleted Cell SHALL no longer be listed

After `Flezha.Service.Steps.delete/2` deletes a Cell, `Flezha.Service.Steps.cells/2` SHALL not
list it, until a writer for that Cell is started again.

#### Scenario: Deleting a Cell

- **WHEN** a Cell with persisted state is deleted
- **THEN** `cells/2` for its relay class answers no row for it

### Requirement: A provider's options SHALL be readable as typed rows

`Flezha.Service.Fleet.provider/2` SHALL answer, beside `provider`, `help`, `type` and `hooks`,
`options`: one map per row of the provider's declaration that is not internal, in declaration
order, as `Flezha.Plugin.ProviderBehaviour.Options.describe_row/1` describes it. Each map SHALL
carry `name`, the option's name as a string; `type`, the row's type, `:any` when the row names
none; `required`, a boolean; and `doc`. It SHALL carry `default`, `values`, `min`, `max`,
`live` and `transient` when the row declares them. A transient row's default SHALL NOT be
answered.

The answer SHALL be data: over `:erpc` a caller receives the declared terms, and through the
door every remote surface receives its JSON, in which an atom is its name. Every default a
first-party provider declares SHALL be one JSON carries: `nil`, a boolean, a number, a string,
an atom, or a list or map of those.

The keys the answer carried before SHALL be unchanged, and `flz plugin help <provider>` SHALL
print the same text; with `--json` it SHALL print the whole answer, `options` included.

#### Scenario: Every row type

- **GIVEN** a provider whose public rows declare `int` with `min: 1` and `max: 10`, `bool`,
  `string`, `names`, `enum` with `values: [:none, :gzip]` and `default: :none`, `pid`, `json`,
  `map`, `list` and `any`, and one row that names no type
- **WHEN** `Fleet.provider/2` is read for it
- **THEN** `options` SHALL list those rows in declaration order with those types, the untyped
  one as `:any`, the `int` row's bounds and the `enum` row's values and default

#### Scenario: An internal row is not answered

- **GIVEN** a provider with a row declared `internal: true`
- **WHEN** `Fleet.provider/2` is read for it
- **THEN** `options` SHALL carry no entry of that name

#### Scenario: A transient row keeps its default

- **GIVEN** a public row declared `transient: true, default: "s3cret"`
- **WHEN** `Fleet.provider/2` is read
- **THEN** its entry SHALL carry `transient: true` and no `default`

#### Scenario: The JSON form

- **WHEN** `flz plugin help relay_http_poll --json` is run
- **THEN** the object SHALL carry `options`, whose `url` entry carries `"type": "string"` and
  `"required": true`, beside `provider`, `help`, `type` and `hooks`

#### Scenario: The text is unchanged

- **WHEN** `flz plugin help relay_websocket` is run
- **THEN** it SHALL print the screen, the type and the supported hooks, and nothing of the
  `options` key

#### Scenario: Every first-party default is JSON

- **WHEN** `Fleet.provider/2` is read for every provider in `Flezha.Plugin.Providers.all/0`
- **THEN** every `default` in every `options` SHALL be `nil`, a boolean, a number, a string, an
  atom, or a list or map of those

### Requirement: The provider listing SHALL say which providers feed a relay

`Flezha.Service.Fleet.providers/1` SHALL list, for each provider, `feeds`: `true` when the provider's declaration says it injects records into a relay's entry channel, `false` otherwise. `Flezha.Plugin.Providers.help/1` SHALL carry the same flag. The flag SHALL be read from the provider's declaration, never inferred from its id.

#### Scenario: A source provider is marked

- **WHEN** a caller holding `plugin_list` lists the providers
- **THEN** `relay_websocket`, `relay_http_poll` and `relay_postgres_poll` carry `feeds: true` and `relay_stdout` carries `feeds: false`

### Requirement: The console SHALL enter through the door as a networked caller that may hold a stream

A caller whose source is `:ui` SHALL build its context with `Flezha.Service.Admin.context(:ui,
token)` and call operations through `Flezha.Service.Admin.call/4` or `run/4` with `:given`
placement. The door SHALL hold it to what a networked caller reaches: an operation whose row is
`remote: false` SHALL be refused to it as `control.unknown_function`, and its failures SHALL be
withheld as a networked caller's are; unlike the other networked sources it MAY call a `remote`
operation that streams, since it holds the stream's process on the controller's distribution as
`flz` does.

#### Scenario: The console cannot stop the controller

- **WHEN** a `:ui` caller calls `Controller.stop` through the door
- **THEN** the door SHALL answer `control.unknown_function` and the controller SHALL keep running

#### Scenario: The console may tail a stream

- **WHEN** a `:ui` caller with a role granting `audit.read` calls `AuditLog.tail` through the door with an `:emit` pid
- **THEN** the stream SHALL run and hand its events to that pid

#### Scenario: A console caller reads a withheld reason

- **WHEN** a `:ui` caller's call fails with an internal code carrying a reason
- **THEN** the reason SHALL read as naming the controller's log, as it does for an HTTP caller

### Requirement: The connected controllers SHALL be readable

`Flezha.Service.Controller.controllers/1` and `flz controller list` SHALL list every controller the cluster directory lists. Each row SHALL give its node, its host address, the machine it runs on when `FLEZHA_HOST_NODE` names one, its version, how long it has been up, how many runners are connected to it, and whether it is the controller that answered. A controller that does not answer SHALL still be listed, marked unreachable, with only its node, and SHALL also be named in the answer's `unreachable`. A node connected to the cluster that is not a controller SHALL NOT be listed.

#### Scenario: Two controllers

- **GIVEN** two connected controllers, each with `FLEZHA_HOST_NODE` set
- **WHEN** a caller lists the controllers
- **THEN** both are listed with their machine names, and the one that answered is marked as such

#### Scenario: A peer does not answer

- **WHEN** a connected controller does not answer within the read's timeout
- **THEN** it is listed with its node and marked unreachable, it is named in `unreachable`, and the other rows are complete

#### Scenario: A console node is connected

- **GIVEN** two controllers and a console connected to them as a visible node
- **WHEN** a caller lists the controllers
- **THEN** only the two controllers are listed

