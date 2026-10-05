# controller-http-api Specification

## Purpose
The controller's published operations over HTTP, for AI tools and scripts: the `http_api` controller plugin serves them as MCP tools and as plain JSON, each call made through the one door as the principal of the bearer token it carries, with the network-only rules (no non-remote or streaming operation, TLS off loopback, an origin guard, caps) that keep it safe by default.
## Requirements
### Requirement: The HTTP API SHALL be a controller plugin enabled in `_system`

The `http_api` provider of the `flezha_http_plugin` package SHALL be a controller plugin that
needs no relay and no hook, and SHALL run one listener on every controller that holds
`_system`. An enable in any other namespace SHALL be refused, and disabling it SHALL stop
every listener before the disable returns.

#### Scenario: Enabled in `_system`

- **WHEN** an admin enables `http_api` in `_system` with no hook
- **THEN** every controller of the cluster SHALL answer `GET /v1/catalogue` on the
  configured port

#### Scenario: Enabled elsewhere

- **WHEN** `http_api` is enabled in namespace `n`
- **THEN** the enable SHALL be refused with a code naming the namespace, and no port SHALL be
  bound

### Requirement: Every HTTP request SHALL be made as a verified token's principal

Every request SHALL carry `Authorization: Bearer <token>`, verified by the token store, and
SHALL be served with a caller context of source `http` built only from that verification. A
request without a valid token SHALL be answered `401` with `WWW-Authenticate: Bearer` and the
token's error code before its body is read. No token SHALL appear in a log, a response or an
error's details.

#### Scenario: No token

- **WHEN** `POST /mcp` arrives without an `Authorization` header
- **THEN** the answer SHALL be `401` and no published function SHALL run

#### Scenario: A revoked token

- **WHEN** a request carries a revoked token
- **THEN** the answer SHALL be `401` with code `authz.token_revoked`

#### Scenario: The audit names the token's principal

- **WHEN** an authorized `tools/call` of `relay_start` succeeds
- **THEN** the audit record SHALL name the token's principal and the source `http`, in the
  shape a `flz` start writes

### Requirement: Every HTTP call SHALL go through the door and nothing else

Both the MCP endpoint and the JSON routes SHALL serve a call only by `Flezha.Service.Admin`
with the caller's context, so that the policy, the quotas, the audit and the error shape are
the published surface's own. The HTTP surface SHALL keep no list of functions, no policy and
no dispatch table of its own.

#### Scenario: A quota is enforced unchanged

- **WHEN** a start over HTTP would exceed the namespace's `max_relays`
- **THEN** the answer SHALL carry `quota.exceeded` with the details a `flz` start receives

#### Scenario: A role that does not grant the action

- **WHEN** a namespace `viewer` token calls `Relay.start`
- **THEN** the call SHALL be refused at the policy point with an `authz.*` code, and the
  refusal SHALL be audited as a `flz` refusal is

### Requirement: A verb that is not remote SHALL never be reachable over HTTP

An operation whose row declares `remote: false`, or whose binding streams, SHALL NOT be
listed and SHALL NOT be callable over HTTP, whatever the token's role.

#### Scenario: An admin token asks to stop the controller

- **WHEN** an `admin@_system` token calls `POST /v1/call/Controller.stop`
- **THEN** the call SHALL be refused as an unknown operation, and the controller SHALL keep
  running

#### Scenario: Not in the tool list

- **WHEN** an `admin@_system` token sends `tools/list`
- **THEN** no tool for stopping the controller, issuing or revoking a token, binding or
  unbinding a role, deleting a namespace, claiming the admin or any streaming operation
  SHALL be listed

### Requirement: The MCP endpoint SHALL speak Streamable HTTP, stateless and tools only

`POST /mcp` SHALL accept JSON-RPC 2.0 and answer `application/json`. It SHALL implement
`server/discover`, `ping`, `tools/list` and `tools/call` for the pinned revision, answer
`initialize` and accept notifications for a client of the earlier handshake revision,
declare only the `tools` capability, mint no session, and answer `405` to `GET /mcp` and
`DELETE /mcp`. A request naming a revision it does not support SHALL be answered `400`
listing the supported ones. An unknown method SHALL be answered with JSON-RPC error
`-32601`.

#### Scenario: A client discovers the server

- **WHEN** a client sends `server/discover` naming the pinned revision
- **THEN** the answer SHALL list the supported revisions, the `tools` capability and the
  server named `flezha`

#### Scenario: A client of the handshake revision initializes

- **WHEN** a client sends `initialize`
- **THEN** the answer SHALL carry the supported handshake revision, the `tools` capability
  and `serverInfo` named `flezha`, and no session id

#### Scenario: A server stream is asked for

- **WHEN** a client sends `GET /mcp`
- **THEN** the answer SHALL be `405`

### Requirement: Each tool SHALL be generated from its catalogue entry

Every remote, non-streaming catalogue entry the token's role grants SHALL be one tool:

- its name SHALL be the entry name in snake case with `.` replaced by `_`;
- its description SHALL be the entry's description;
- its input schema SHALL be generated from the entry's argument rows, with the required rows
  required and no other property allowed;
- its `readOnlyHint` SHALL be true only for an operation whose row writes no audit record.

A tool SHALL be listed only when the token's role grants its row's action. Beside the
operation tools, the four knowledge tools of the `ai-assistant-kit` capability SHALL be
listed for every token, and the server's `instructions` SHALL carry the working rules.

#### Scenario: The schema follows the rows

- **WHEN** `tools/list` is read and the entry `Dlq.list` declares one optional string argument
  `relay_class`
- **THEN** the tool `dlq_list` SHALL have an input schema with one optional string property
  `relay_class` and `additionalProperties: false`

#### Scenario: A role sees only what it may call

- **WHEN** a namespace `viewer` token sends `tools/list`
- **THEN** no tool whose action the viewer role does not grant SHALL be listed

#### Scenario: The knowledge tools are served over HTTP

- **WHEN** a namespace `viewer` token sends `tools/list` to `POST /mcp`
- **THEN** `flezha_guide`, `flezha_sdk_example`, `flezha_error` and `flezha_docs` SHALL be
  listed

### Requirement: A Flezha failure SHALL reach the model as a tool error with its code and hint

A `tools/call` whose operation fails SHALL answer a result with `isError: true`, whose text
content is the error object as JSON and whose structured content is `{"error": {...}}`
carrying `code`, `class`, `message`, `details` and `hint`. A protocol fault (an unknown tool,
arguments that are not an object) SHALL be answered as a JSON-RPC error instead. A success
SHALL answer the door's JSON as text content and as structured content, wrapped as
`{"result": ...}` when it is not a JSON object.

#### Scenario: A dead letter that does not exist

- **WHEN** `tools/call` of `dlq_get` names an entry that does not exist
- **THEN** the result SHALL carry `isError: true` and the code `dlq.not_found`

#### Scenario: An unknown tool

- **WHEN** `tools/call` names `controller_stop`
- **THEN** the answer SHALL be a JSON-RPC error, not a tool result

### Requirement: The plain JSON form SHALL be the same calls without MCP

`POST /v1/call/<Name>` SHALL take a JSON array of arguments and answer `200` with the door's
JSON, or the error object with an HTTP status chosen by its class. `GET /v1/catalogue` SHALL
answer the same entries `tools/list` would, in the catalogue's own form.

#### Scenario: A listing over plain JSON

- **WHEN** `POST /v1/call/Relay.list` is sent with `["default"]` and a token whose role
  grants listing
- **THEN** the answer SHALL be `200` with the relays of `default` as JSON, the same JSON
  `flz relay list --json` prints

#### Scenario: A failure's status follows its class

- **WHEN** `POST /v1/call/Dlq.get` names an entry that does not exist
- **THEN** the answer SHALL be `404` with the error object

### Requirement: The listener SHALL be safe by default

The listener SHALL bind `127.0.0.1` unless configured otherwise, and SHALL refuse to start on
any other address without TLS unless `plain_http: true` is set. A request carrying an `Origin`
header not listed in `allowed_origins` SHALL be refused with `403`. Request bodies SHALL be
capped, waiting calls SHALL be capped and answered `503` beyond the cap, and every call SHALL
have a deadline. Only the method, the path, the status, the duration and the tool name SHALL
be logged.

#### Scenario: A public address without TLS

- **WHEN** `http_api` is enabled with `bind=0.0.0.0` and no certificate
- **THEN** the enable SHALL be refused with a code naming the rule, and no port SHALL be bound

#### Scenario: A browser page on another origin

- **WHEN** a request arrives with `Origin: https://evil.test` and `allowed_origins` is empty
- **THEN** the answer SHALL be `403` and no published function SHALL run

#### Scenario: Nothing secret in the log

- **WHEN** the suite drives every route with debug logging on
- **THEN** no captured log line SHALL contain the token, a header value or a body

