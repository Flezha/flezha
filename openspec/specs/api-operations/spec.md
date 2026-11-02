# api-operations Specification

## Purpose

The controller's published operations and how they are read. Every published operation is described
once, on the function that binds its authorization row; the catalogue built from those declarations
is what the CLI, the console, MCP and JSON surfaces resolve names through, and every caller that
proves an identity is served by it with failures answered as `%Flezha.Error{}`. Consumers use the
data service API instead of internals and resolve their calls against it at build time. The reads
scale with the fleet (one subject's health costs the same at any size, a listing costs time linear
in its rows and answers by the page) and answer for the whole cluster from whichever controller
received them, naming any controller that did not answer.

## Requirements

### Requirement: Every published operation SHALL be described once, on its binding

Every published function SHALL declare, where it binds its row, the positional arguments it
takes before the caller context (each with its type, whether it is required, its default and
a one-line description, in the vocabulary plugin options use) and whether it answers a
subscription. Its description SHALL be the first sentence of its `@doc`. The catalogue SHALL
be built from these declarations and from the rows, and SHALL NOT be written anywhere else by
hand.

Each catalogue entry SHALL carry the operation's name in the door's wire form
(`"<Module>.<function>"`), its arguments, its description, its action, whether it streams,
and whether it may be reached over the network.

#### Scenario: A binding whose arguments do not match its arity fails the suite

- **WHEN** a published function takes three arguments before its context and its binding
  declares two
- **THEN** the declaration test SHALL fail naming the module, the function and both counts

#### Scenario: A published function without a description fails the suite

- **WHEN** a published function has no `@doc`
- **THEN** the declaration test SHALL fail naming it

#### Scenario: The catalogue answers at runtime in a release

- **WHEN** `Flezha.Service.catalogue/0` is called on a controller started from a release
- **THEN** it SHALL answer every published operation with its description, although the
  release carries no documentation chunks

### Requirement: A row SHALL say whether its verb may be reached over the network

Every row of the verb declaration SHALL carry `remote`, true unless declared otherwise. The
rows of stopping the controller, issuing and revoking tokens, binding and unbinding roles,
deleting a namespace and claiming the first admin SHALL declare `remote: false`. The flag
SHALL live in the row, so that no list of forbidden verbs is kept beside the declaration.

#### Scenario: A non-remote verb is marked in the catalogue

- **WHEN** the catalogue is read
- **THEN** the entry of `Controller.stop` SHALL carry `remote: false`, and the entry of
  `Dlq.list` SHALL carry `remote: true`

### Requirement: The CLI SHALL ship a generated copy of the catalogue

`mix flezha.catalogue` SHALL write the catalogue to `crates/cli/catalogue.json`, and the suite SHALL
fail when the committed file differs from a fresh generation. `flz` SHALL embed the file, and
`flz help --json` SHALL print it without reaching a controller.

#### Scenario: A stale catalogue file fails the suite

- **WHEN** a published function gains an argument and `crates/cli/catalogue.json` is not
  regenerated
- **THEN** the catalogue test SHALL fail naming the entry that differs

#### Scenario: Help as data works offline

- **WHEN** `flz help --json` is run on a machine where no controller runs
- **THEN** it SHALL print the catalogue as JSON and exit 0

### Requirement: The CLI SHALL reach the controller only through the door

The controller side of `flz` SHALL call nothing but the door, the catalogue, its own
renderers and the standard library. A test SHALL read the compiled imports of every CLI
module and fail on a call outside that set. Every door name `flz` sends SHALL be a catalogue
entry, and a Rust test SHALL fail on a verb row that sends a name, or a positional count, the
catalogue does not declare.

#### Scenario: A CLI module that reads a store fails the suite

- **WHEN** a module under `Flezha.Service.Cli` calls `Flezha.Dlq.Store.list/1`
- **THEN** the import test SHALL fail naming the module and the call

#### Scenario: A Rust verb that sends an unknown name fails the build's tests

- **WHEN** a verb row in `crates/cli/src` sends `"Dlq.lst"`
- **THEN** the Rust catalogue test SHALL fail naming the verb and the name

### Requirement: The catalogue SHALL publish one operation per concept, and only operations a surface calls

The catalogue SHALL hold one operation per concept: the relays of a namespace are `Relay.list`
(rows) and the plugins enabled in a namespace are `PluginManager.enabled`; `Relay.list_relays` and
`PluginManager.list` SHALL NOT be operations. Every catalogue operation SHALL be called by a `flz`
verb, by an SDK control method, or by the console's manifest; a function no surface calls SHALL be an
internal function without a `verb` binding, which `/v1/call`, MCP and the door do not reach.

#### Scenario: The SDK list reads reach the survivors

- **WHEN** a runner's `control().list_relays()` and `list_plugins()` are called in any SDK
- **THEN** the door SHALL resolve `Relay.list` and `PluginManager.enabled` with the namespace the connection proved

#### Scenario: An uncalled function is not an operation

- **WHEN** the catalogue is generated
- **THEN** it SHALL carry no entry for a function no `flz` verb, SDK method or console manifest line names, except the operation that undoes a stream a surface calls (`AuditLog.unsubscribe`, `Observability.unsubscribe`), and a `/v1/call` naming one SHALL be answered `control.unknown_function`

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
same caller context holding a stream open, so the remote streams its role grants are named, and
`posture`: the controller's enforcement posture (`open`, `bootstrap_pending` or `enforced`). The answer SHALL NOT change how any call is judged.
`flz auth whoami` SHALL print the same text as before.

#### Scenario: A viewer's operations

- **WHEN** a caller bound as `viewer` calls `whoami/1` while authorization is enforced
- **THEN** `operations` lists exactly the names `Admin.catalogue/1` answers for that context
- **AND** it includes `Relay.list` and excludes `Relay.start`

#### Scenario: A networked caller is told its streams

- **WHEN** an `:http` caller whose role grants `relay.listen` calls `whoami/1`
- **THEN** `operations` SHALL include `Relay.listen` and SHALL NOT include `Controller.stop`

#### Scenario: An unbound caller under the open posture

- **WHEN** a caller with no role calls `whoami/1` while authorization is open
- **THEN** `operations` lists every operation that caller reaches

#### Scenario: The CLI text

- **WHEN** an operator runs `flz auth whoami`
- **THEN** it prints the principal, namespace and role lines and nothing more

#### Scenario: The posture is answered

- **WHEN** a caller calls `whoami/1` while authorization is enforced
- **THEN** the answer's `posture` SHALL be `enforced`

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

### Requirement: A networked caller SHALL reach a streaming operation only through a surface that holds it open

The door SHALL hold every networked caller (`:sdk`, `:http`, `:mcp`) to what the network
reaches: an operation whose row is `remote: false` SHALL be refused as
`control.unknown_function`, and failures SHALL be withheld as networked failures are. A `remote`
operation that streams SHALL be reachable by a networked caller only when its caller context
carries an `:emit` holder, which only a surface that keeps the stream open sets.

#### Scenario: A networked caller cannot stop the controller

- **WHEN** an `:http` caller calls `Controller.stop` through the door
- **THEN** the door SHALL answer `control.unknown_function` and the controller SHALL keep running

#### Scenario: The HTTP stream route may tail

- **WHEN** an `:http` caller whose role grants `audit.read` calls `Observability.tail` through the
  door with an `:emit` holder
- **THEN** the stream SHALL run and hand its events to the holder

#### Scenario: A runner cannot open a stream

- **WHEN** an `:sdk` caller asks for `Observability.tail` through an admin request
- **THEN** the door SHALL answer `control.unknown_function`

#### Scenario: A networked caller reads a withheld reason

- **WHEN** an `:http` caller's call fails with an internal code carrying a reason
- **THEN** the reason SHALL read as naming the controller's log

### Requirement: A caller of source `:ui` SHALL be refused

The door SHALL refuse a caller context of source `:ui` with `control.surface_retired`, naming the
HTTP API as the console's channel. `:ui` SHALL remain a valid source in stored audit records.

#### Scenario: A console still on distribution is refused

- **WHEN** a caller with source `:ui` calls `Relay.list` through the door
- **THEN** the door SHALL answer `control.surface_retired` and run nothing

#### Scenario: Old audit records still read

- **WHEN** the audit log holds a record written with caller source `ui` and an admin reads it
- **THEN** the record SHALL be returned as written

### Requirement: Reading one subject's health SHALL NOT grow with the fleet

Reading the convergence verdict or the listed health of one subject (a relay, runner, plugin, channel or driver) SHALL copy only that subject's verdict and the small per-tick facts. Its cost SHALL NOT grow with the number of subjects the controller judges.

#### Scenario: One lookup in a large fleet

- **GIVEN** a controller that judges 8,000 subjects
- **WHEN** a caller reads one relay's health
- **THEN** the read SHALL take about the same time as with 1,000 subjects judged, within a factor of 2

#### Scenario: The answer is unchanged

- **GIVEN** a relay whose verdict is `:failed`, inside the unmet-intent grace
- **WHEN** a caller reads its listed health
- **THEN** the answer SHALL be `:recovering`, as before this requirement

### Requirement: A namespace's relay listing SHALL cost time linear in its relays

A read that lists a namespace's relays with their health (relay rows, the relay list, the relay info report, cluster status) SHALL read every relay's health in one pass over the namespace's verdicts. It SHALL NOT read the verdicts once per row. Its cost SHALL grow linearly with the namespace's relays.

#### Scenario: A thousand relays

- **GIVEN** a namespace running 1,000 relays on one controller
- **WHEN** a caller reads its relay rows
- **THEN** the answer SHALL arrive in well under the caller's default timeout, and each row's health SHALL equal what a single-relay read answers for that relay

#### Scenario: A relay not yet judged

- **GIVEN** a registry row for a relay the Loop has not judged yet
- **WHEN** a caller reads the relay rows
- **THEN** that row's health SHALL be `:unknown`

### Requirement: A namespace's plugin listing SHALL cost time linear in its plugins and bindings

The enabled-plugin listing SHALL group the namespace's bindings and running plugins by plugin once. It SHALL build each plugin's row from that plugin's group, without scanning every binding or every relay once per plugin.

#### Scenario: Plugins that grow with relays

- **GIVEN** a namespace where each of 2,000 relays has its own bound plugin
- **WHEN** a caller lists the enabled plugins
- **THEN** each row SHALL name exactly the relays bound to that plugin and the controllers it runs on
- **AND** the listing SHALL take no more than about 4 times as long as for 500 such relays and plugins

### Requirement: Every whole-fleet health reader SHALL read every verdict

A reader that folds over every verdict the controller judges, such as the health endpoint, SHALL read all of them in one read. It SHALL count the same subjects the Loop published on its last tick.

#### Scenario: The health endpoint counts every relay

- **GIVEN** the Loop has published verdicts for 3 relays, one of them failed
- **WHEN** the health endpoint is read
- **THEN** its relay health SHALL count 3 relays, one of them failed

### Requirement: A list read SHALL answer one page, its total and a cursor when asked

The relay list, the enabled-plugin list, the dead-letter list and the steps reads (journal, committed iterations, Cells, undelivered) SHALL take an optional `opts` keyword of `limit`, `after`, `sort` and `filter`. Without `opts` each SHALL answer exactly what it answered before.

With `opts` it SHALL answer:

- at most `limit` rows matching the filter, in the declared sort order;
- `total`, every matching row before the limit;
- `next`, a cursor for the following page, or `nil` on the last page;
- `unreachable`.

`limit` SHALL default to 50 and SHALL NOT exceed 500, and `0` SHALL answer only the totals. An unknown key, a limit outside its bounds, and a cursor the controller did not issue for that read and sort SHALL be refused as `invalid` before any work. No operation SHALL be added for paging: each collection keeps its one operation.

#### Scenario: Paging through a namespace

- **GIVEN** a namespace running 1,200 relays
- **WHEN** a caller reads the relay list with `limit: 500`, passing each answer's `next` as `after`
- **THEN** three reads SHALL answer 500, 500 and 200 rows, each SHALL carry `total: 1200`, and the third SHALL carry `next: nil`

#### Scenario: A relay stops between two pages

- **GIVEN** a caller has read the first page sorted by id
- **WHEN** a relay on that page stops and the caller reads the next page with the cursor
- **THEN** the next page SHALL start at the first relay after the last one the caller received, repeating none and skipping none of the relays still running

#### Scenario: A forged cursor

- **WHEN** a caller passes a cursor the controller did not issue, or one issued for another read or sort
- **THEN** the read SHALL be refused as `invalid` and SHALL answer no rows

#### Scenario: Existing callers are unchanged

- **WHEN** `flz relay list` or an SDK's `list_relays()` calls the relay list without `opts`
- **THEN** the answer SHALL be the one it received before this requirement

### Requirement: The relay list SHALL filter, sort and summarise a namespace in one read

A paged relay list SHALL filter by health, factory, run kind (`flow` or `steps`), runner, node, partition group, relay ids (at most 500) and a case-insensitive text search over relay id and factory id. It SHALL sort by id or by severity. A partition group SHALL count as one row, and each row SHALL carry its run kind and bound-plugin count. A run kind other than `flow` or `steps` SHALL be refused as `invalid`.

Each paged answer SHALL carry the namespace's `summary`, computed over every relay whatever the filter: counts by health, the faulted partition groups, and the factories relays wait on with how many relays wait on each. When the filter names only `ids`, the read SHALL look those relays up directly, its cost SHALL NOT grow with the namespace's other relays, and its `summary` SHALL be `nil`.

#### Scenario: Finding one relay among thousands

- **GIVEN** a namespace running 5,000 relays
- **WHEN** a caller reads the relay list with `filter: [text: "00500"]`
- **THEN** the rows SHALL be the relays whose id or factory contains `00500`, and `total` SHALL count them

#### Scenario: Only the steps relays

- **GIVEN** a namespace running 2,000 flow relays and 40 steps relays
- **WHEN** a caller reads the relay list with `filter: [run: "steps"]`
- **THEN** the rows SHALL be the 40 steps relays and `total` SHALL be 40

#### Scenario: A status line without rows

- **GIVEN** a namespace running 3,000 relays, 12 of them failed
- **WHEN** a caller reads the relay list with `limit: 0`
- **THEN** the answer SHALL hold no rows, and its summary's health counts SHALL sum to 3,000 with 12 failed

#### Scenario: A runner goes away

- **GIVEN** 1,000 relays of factory `orders` whose only runner disconnected
- **WHEN** a caller reads the relay list with `filter: [health: unhealthy]`, `sort: :severity` and `limit: 20`
- **THEN** the answer SHALL hold 20 rows with `total: 1000`, and the summary's `waiting` SHALL name `orders` with 1,000 relays

#### Scenario: One relay screen in a large namespace

- **GIVEN** a namespace running 5,000 relays
- **WHEN** a caller reads the relay list with `filter: [ids: ["orders_17"]]`
- **THEN** the one row SHALL equal that relay's row in the unpaged list, and the read SHALL not walk the namespace

### Requirement: A paged plugin list SHALL carry a sample of each plugin's relays

A paged enabled-plugin list SHALL filter by provider, by a bound relay and by a name search. Each row SHALL carry at most 10 bound relay ids, sorted, and `relay_count`. The unpaged list SHALL keep every bound relay.

#### Scenario: A plugin bound to a thousand relays

- **GIVEN** a plugin bound to 1,000 relays
- **WHEN** a caller reads the page holding it
- **THEN** its row SHALL carry 10 relay ids and `relay_count: 1000`

#### Scenario: The plugins of one relay

- **WHEN** a caller reads the plugin list with `filter: [relay: "orders_17"]`
- **THEN** the rows SHALL be exactly the plugins bound to `orders_17`

### Requirement: Liveness counters SHALL be readable for named relays

The liveness counters SHALL key every relay by its namespace and id, and every relay row SHALL carry its `namespace`. Two relays with the same id in different namespaces SHALL keep separate counters.

The liveness counters SHALL take an optional `opts` of `filter` with either `relays: [{namespace, relay_id}]` (at most 500 relays) or `namespace: ns`, and `limit`. Any other key, and `relays` together with `namespace`, SHALL be refused as `invalid`. They SHALL then answer, across the cluster and in the same `channels`, `relays` and `unreachable` shape:

- for `relays`: only those relays' counters, matched on namespace and id, and the channel counters of the runners those relays are bound to;
- for `namespace`: every relay of that namespace and the channel counters of the runners they are bound to;
- for `namespace` with `limit: 0`: no rows, and `totals`, each counted relay slot summed over the namespace's relays on every controller.

#### Scenario: Rates for one page

- **GIVEN** a cluster running 5,000 relays
- **WHEN** a caller asks for the counters of the 20 relays on its page
- **THEN** the answer SHALL hold those 20 relays' counters and their runners' channel counters, and no other relay's

#### Scenario: The same id in two namespaces

- **GIVEN** a relay `orders` in namespace `a` and another `orders` in namespace `b` on one controller, each having received a different number of records
- **WHEN** a caller reads the liveness counters
- **THEN** the answer SHALL hold two rows for `orders`, one per namespace, each with its own counts

#### Scenario: A namespace's flow without its rows

- **GIVEN** a namespace running 5,000 relays across two controllers
- **WHEN** a caller reads the liveness counters with `filter: [namespace: "trade"]` and `limit: 0`
- **THEN** the answer SHALL hold no relay or channel rows
- **AND** its `totals` SHALL equal, slot by slot, the sum of the counted slots of that namespace's relay rows read without `limit: 0` at the same moment

#### Scenario: One scope per read

- **WHEN** a caller passes both `relays` and `namespace`
- **THEN** the read SHALL be refused as `invalid`

### Requirement: The undelivered input-log records SHALL be countable without reading them

The undelivered read with `opts` SHALL answer `total` from the input log's offsets, without reading a record. It SHALL read only the records of the page it answers. Its `total` SHALL equal the length of the unpaged answer read at the same moment.

#### Scenario: A large backlog

- **GIVEN** an instance with 50,000 undelivered records
- **WHEN** a caller reads undelivered with `limit: 0`
- **THEN** the answer SHALL carry `total: 50000` and no rows, and no record SHALL be read from disk

### Requirement: A steps instance's journal, iterations and Cells SHALL be readable by the page

Journal and committed iterations SHALL page newest first. Cells SHALL page by key, with a key search.

#### Scenario: Picking a rewind point in a long history

- **GIVEN** an instance with 20,000 committed iterations
- **WHEN** a caller reads committed iterations with `limit: 50`
- **THEN** the answer SHALL hold the 50 newest, `total: 20000` and a cursor

### Requirement: Dead-letter statistics SHALL be read without decoding entries

The dead-letter Store SHALL keep entry and byte counts per relay class and tasklet in memory, updated after an entry is parked, dropped, resolved or swept and after a relay's parked sets are hydrated, and rebuilt from disk at start. Stats SHALL answer from those counts without reading an entry index. A paged dead-letter list SHALL answer newest first across the cluster and SHALL decode only the entries of the page it answers.

#### Scenario: Stats with a large queue

- **GIVEN** 100,000 parked entries across 40 tasklets
- **WHEN** a caller reads dead-letter stats
- **THEN** the answer SHALL give each tasklet's entry and byte counts, and no entry index SHALL be read

#### Scenario: Counts survive a crash

- **GIVEN** the Store is killed during a sweep
- **WHEN** it restarts and a caller reads stats
- **THEN** the counts SHALL equal the entries on disk

#### Scenario: A drop between two pages

- **GIVEN** a caller has read the first page of the dead-letter list
- **WHEN** an entry on that page is dropped and the caller reads the next page with the cursor
- **THEN** the next page SHALL repeat none and skip none of the entries still parked

### Requirement: A cluster read SHALL name the controllers that did not answer

A read that gathers its answer from several controllers SHALL report, in a field named
`unreachable`, the node of every controller it asked that did not answer within the read's
bound. The field SHALL be an empty list when every controller answered. A list read SHALL
answer `%{rows: rows, unreachable: nodes}`. A read that answers a map SHALL carry
`unreachable` as one more key of that map. Every row that describes something living on one
controller SHALL carry that controller's node in a field named `node`.

A controller that does not answer SHALL cost the read one bound, not one bound per controller,
and SHALL NOT fail the read: the rows of the controllers that answered are still returned.

#### Scenario: Every controller answers

- **GIVEN** a cluster of two controllers that both answer
- **WHEN** a caller reads the relay list
- **THEN** the answer carries `unreachable: []` and every row carries the `node` that hosts it

#### Scenario: A controller does not answer in time

- **GIVEN** a cluster of three controllers where one is wedged
- **WHEN** a caller reads the factory list
- **THEN** the answer arrives within one bound, holds the rows of the two answering
  controllers, and names the wedged controller in `unreachable`

### Requirement: The controllers of a cluster read SHALL come from the cluster directory

A cluster read SHALL ask the controllers that the cluster directory lists, and SHALL NOT take
its peers from `Node.list()`. Every controller SHALL advertise itself in the directory while it
runs, and stop advertising when it stops. A node connected to the cluster that is not a
controller, such as a console, SHALL therefore never be asked, never be listed as a controller
and never be reported unreachable.

#### Scenario: A console is connected

- **GIVEN** two controllers and a console node connected to both as a visible node
- **WHEN** a caller lists the connected controllers
- **THEN** exactly the two controllers are listed and `unreachable` is empty

### Requirement: Relay health SHALL be the hosting controller's verdict

Every surface that answers a relay's health SHALL answer the verdict of the controller that
hosts that relay. This covers the relay list, relay rows, partition-group rows, `Relay.info`,
`Controller.cluster_status` and every health count derived from them. A relay whose hosting
controller does not answer SHALL read `unknown`, and that controller SHALL be named in the
read's `unreachable`. A relay that no controller hosts SHALL keep the health its registry row
gives it today.

#### Scenario: A healthy relay on another controller

- **GIVEN** controllers A and B, and a healthy relay `orders` hosted on B
- **WHEN** a caller connected to A lists the namespace's relays
- **THEN** `orders` reads `healthy` with `node` B, not `unknown`

#### Scenario: The hosting controller does not answer

- **GIVEN** a relay hosted on controller B, which does not answer the health read
- **WHEN** a caller connected to A lists the relays
- **THEN** the relay reads `unknown` and B is named in `unreachable`

### Requirement: A registry read SHALL answer on a controller outside the namespace's hub

Every read of a namespace's relay registry SHALL answer the same rows on every controller. The
registry reads are the relay rows, the relay bindings, the registry dump and the relay ids. A controller that is not a live
member of the namespace's hub SHALL route the read to a member instead of answering an empty
list or a `hub_down` refusal. Only when no controller runs the hub SHALL the read answer as it
does today when the hub is down.

#### Scenario: The answering controller left the hub

- **GIVEN** controller A left namespace `trader`'s hub and controller B is its member with three
  relays
- **WHEN** a caller connected to A lists `trader`'s relays
- **THEN** the three relays are listed

### Requirement: The factory list SHALL hold every factory the cluster's runners advertise

`Fleet.factories/2` SHALL answer one row per factory that any controller's runners advertise in
the namespace. The row SHALL list the `nodes` whose runners advertise it and the union of the
`runners` that can place it. `Fleet.runners_for_factory/3` SHALL answer the runners of every
controller that advertises the factory. A factory advertised only on a controller that does not
answer SHALL be missing, and that controller SHALL be named in `unreachable`.

#### Scenario: A factory served only through another controller

- **GIVEN** factory `fan_trade` whose only runner is connected to controller B
- **WHEN** a caller connected to controller A lists the factories
- **THEN** `fan_trade` is listed with `nodes: [B]` and its runner

#### Scenario: Two controllers serve one factory

- **GIVEN** factory `ma_fan` advertised by one runner on A and one on B
- **WHEN** a caller lists the factories
- **THEN** `ma_fan` is listed once, with both nodes and both runners

### Requirement: The cluster status SHALL not name a leader the cluster does not elect

`Controller.cluster_status/2` SHALL NOT answer a `leader` or `is_leader`, because the cluster
elects none. It SHALL answer the controller that answered as `answered_by`, the hub's nodes with
their relay counts and recovery state, the unplaced relays, the cluster-wide relay health, and
`unreachable`.

#### Scenario: Two controllers host relays

- **GIVEN** controllers A and B, each hosting two healthy relays of the namespace
- **WHEN** a caller connected to A reads the cluster status
- **THEN** the relay health counts four healthy relays, `answered_by` is A, and no leader is
  named

### Requirement: An instance's steps reads SHALL run where its state is

Every steps read of one instance SHALL run on the controller that holds the instance's state:
the controller hosting the instance while it runs, otherwise the controller whose state root
holds its directory. The steps reads are its completion, journal, committed iterations, consumed
offset, undelivered input, inbox, last input offset, Cells, a Cell's outbox and a Cell's state at
an earlier point. A caller connected to any controller SHALL get the same answer. An instance whose
state no answering controller holds SHALL be refused as not found, and the controllers that did
not answer SHALL be named in the refusal.

#### Scenario: A running instance on another controller

- **GIVEN** a steps instance running on controller B
- **WHEN** a caller connected to controller A reads its journal
- **THEN** the journal B holds is answered

#### Scenario: A retained finished instance on another controller

- **GIVEN** a finished steps instance whose state its class keeps, retained on controller B
- **WHEN** a caller connected to controller A reads its completion
- **THEN** the completion record B holds is answered

### Requirement: A relay's diagnosis SHALL be made on the controller that hosts it

`Relay.diagnose/3` SHALL build its report on the controller that hosts the relay, so the
verdict, the runner and plugin records, the dead letters and the input-log findings are that
controller's. A relay that no controller hosts SHALL be diagnosed where its state is, as the
steps reads are.

#### Scenario: A relay with dead letters on another controller

- **GIVEN** a relay hosted on controller B with two dead letters parked on B
- **WHEN** a caller connected to controller A diagnoses it
- **THEN** the report names the two dead letters and B's verdict for the relay

### Requirement: Relay info SHALL list the finished relays of every controller

`Relay.info/2` SHALL list the retained finished relays of every controller in the namespace,
each with its `node`, beside the live relays. A read of one relay SHALL find a retained
finished relay on any controller.

#### Scenario: A relay finished on another controller

- **GIVEN** a steps relay that finished on controller B and is still inside its retention window
- **WHEN** a caller connected to controller A reads the namespace's relay info
- **THEN** the finished relay is listed with `node` B

### Requirement: Dead letters SHALL be read and acted on across the cluster

`Dlq.list/2` and `Dlq.stats/1` SHALL answer the parked entries of every controller in the
cluster answer shape, each row carrying the `node` whose store holds it.
`Dlq.list/2` SHALL keep its order, nearest-to-expiry first, across controllers. `Dlq.get/2`,
`Dlq.drop/2` and `Dlq.replay/2` SHALL find the entry on whichever controller holds it, judge the
caller against the entry's namespace as they do today, and act on that controller. An entry no
answering controller holds SHALL be refused as not found, naming the controllers that did not
answer.

#### Scenario: Dead letters parked on two controllers

- **GIVEN** one entry parked on controller A and two on controller B
- **WHEN** a caller connected to A lists the dead letters
- **THEN** all three are listed, each with its node

#### Scenario: Replaying an entry held by another controller

- **GIVEN** an entry parked on controller B
- **WHEN** a caller connected to A replays it
- **THEN** B re-dispatches it through the ordinary input path and the entry is removed from B

### Requirement: Schedules SHALL be read and acted on across the cluster

`Schedule.list/1` SHALL answer the schedule definitions of every controller in the cluster
answer shape, each row carrying the `node` that holds the definition and `fires_on`, the node
whose job fires it. `Schedule.run/2` and
`Schedule.remove/2` SHALL act on the controller that holds the named definition. `Schedule.add/2`
SHALL refuse a name that any answering controller already holds, and SHALL refuse while a
controller that might hold it does not answer, naming that controller.

#### Scenario: A schedule added through another controller

- **GIVEN** schedule `nightly` added through controller B
- **WHEN** a caller connected to A lists the schedules
- **THEN** `nightly` is listed with node B, the node firing it, its next fire and its last result

#### Scenario: Removing a schedule held by another controller

- **GIVEN** schedule `nightly` held by controller B
- **WHEN** a caller connected to A removes it
- **THEN** B forgets it and it stops firing

#### Scenario: A duplicate name is refused

- **GIVEN** schedule `nightly` held by controller B
- **WHEN** a caller connected to A adds another schedule named `nightly`
- **THEN** the add is refused naming the existing schedule and B, and nothing is stored on A

### Requirement: An observability signal SHALL say where and when it was recorded

Every signal answered by `Observability.query/2` or handed on by `Observability.tail/2` SHALL carry
`node`, the controller that recorded it, and `at_us`, the wall-clock time in microseconds at which
it was recorded, computed on that controller from its own clock. A consumer SHALL be able to order
signals from several controllers by `at_us` without knowing any controller's monotonic clock.

#### Scenario: Two controllers record signals

- **GIVEN** controller A records a signal at 12:00:01 and controller B one at 12:00:02, wall clock
- **WHEN** a caller queries the source
- **THEN** both signals are answered, A's first, each carrying its node and its `at_us`

### Requirement: Observability reads and switches SHALL cover every controller

`Observability.query/2` SHALL read the ring buffer of every controller, filter each by the
caller's namespace before the limit, merge the signals by `at_us` and answer the newest up to the
limit, in the cluster answer shape. It SHALL take its lower bound as `since_us`, a wall-clock time
each controller converts to its own clock. `Observability.list_sources/1` SHALL answer one row per
source with its status on each controller and its recent activity summed across controllers.
`Observability.list_sinks/1` SHALL answer each controller's sinks with their `node`.
`Observability.enable_source/2` and `Observability.disable_source/2` SHALL apply the change on
every controller and answer the controllers that applied it (`applied`) and `unreachable`.
`Observability.tail/2` SHALL hand on the signals of every controller.

#### Scenario: A source disabled across the cluster

- **GIVEN** two controllers with source `flezha_relay` enabled
- **WHEN** a caller connected to A disables it
- **THEN** both controllers stop emitting it and the answer names both as applied

#### Scenario: A tail sees another controller's signals

- **GIVEN** a tail opened through controller A on source `flezha_relay`
- **WHEN** a relay hosted on controller B starts
- **THEN** the tail hands on B's `relay_started` signal carrying node B

### Requirement: Quota usage SHALL be the cluster's total

`Namespace.quota/3` SHALL answer the namespace's usage of a quota kind across the cluster: the
`total`, the usage per controller in `by_node`, and `unreachable`. The relay count SHALL be read
once from the namespace's relay registry and not summed per controller. Connected runners, Cells,
mailbox bytes and disk bytes SHALL be summed across controllers.

#### Scenario: Runners on two controllers

- **GIVEN** namespace `trader` with two runners connected to A and one to B
- **WHEN** a caller reads `trader`'s runner quota usage
- **THEN** the total is 3, with A at 2 and B at 1

### Requirement: Per-controller facts SHALL be answered for every controller

`Controller.status/1`, `Controller.boot_status/1` and `Controller.mem_top/1` SHALL answer one row
per controller, each with its `node`, in the cluster answer shape. `Controller.durability_breaches/1`,
`Controller.durability_status/1` and `Fleet.installed_plugins/1` SHALL answer the rows of every
controller, each with the `node` that holds it, in the cluster answer shape.
`Fleet.providers/1` SHALL answer one row per provider with the `nodes` that can enable it, and the
count of refused packages per controller (`refused`). `Fleet.provider/2` SHALL answer from a
controller that holds the provider.

#### Scenario: Plugin packages differ between controllers

- **GIVEN** the `rabbitmq` package installed on controller A only
- **WHEN** a caller connected to B lists the providers
- **THEN** the `rabbitmq` providers are listed with `nodes: [A]`

#### Scenario: Status of a two-controller cluster

- **WHEN** a caller reads the controller status of a two-controller cluster
- **THEN** two rows are answered, one per controller, each with its node
