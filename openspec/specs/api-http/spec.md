# api-http Specification

## Purpose
The controller's published operations over HTTP, for AI tools and scripts: the `http_api` controller plugin serves them at `/mcp` and as plain JSON, each call made through the one door as the principal of the bearer token it carries, with the network-only rules (no non-remote or streaming operation, TLS off loopback, an origin guard, caps) that keep it safe by default. What the `/mcp` endpoint speaks is `api-mcp`.
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

An operation whose row declares `remote: false` SHALL NOT be listed and SHALL NOT be callable
over HTTP, whatever the token's role. An operation whose binding streams SHALL NOT be listed as a
tool and SHALL NOT be callable at `POST /v1/call/<operation>` or through MCP; when it is `remote`,
it SHALL be reachable only at the stream route.

#### Scenario: An admin token asks to stop the controller

- **WHEN** an `admin@_system` token calls `POST /v1/call/Controller.stop`
- **THEN** the call SHALL be refused as an unknown operation, and the controller SHALL keep
  running

#### Scenario: Not in the tool list

- **WHEN** an `admin@_system` token sends `tools/list`
- **THEN** no tool for stopping the controller, issuing or revoking a token, binding or
  unbinding a role, deleting a namespace, claiming the admin or any streaming operation
  SHALL be listed

#### Scenario: A stream is not a call

- **WHEN** a token granting `audit.read` calls `POST /v1/call/Observability.tail`
- **THEN** the call SHALL be refused as an unknown operation, and no stream SHALL start

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

### Requirement: A relay's channel SHALL be callable at `POST /v1/relays/<id>/<channel>`

The HTTP API SHALL serve `POST /v1/relays/<id>/<channel>` through the door as
`Relay.call_channel`, taking the body as the value by the channel's format (a JSON value for
`json`, UTF-8 text for `text`, raw bytes for `bytes`, `msgpack` and `custom`), and SHALL answer the
decoded answer with the content type of its format (`application/json`,
`text/plain; charset=utf-8`, `application/octet-stream`), `202` with `{"accepted": true}` for a
channel that does not answer, and the error object with its class status on failure. An optional
`key` query parameter SHALL be passed as the call's `key`, so a partition group is answered by the
member owning the key; a group called without one is refused `call.target_unkeyed`. The call SHALL
carry a deadline below the listener's own `deadline_ms` by at least `call_grace_ms`, or half of
that deadline when it is shorter, so a relay that does not answer in time is answered
`call.timeout`, never `control.timeout`.

#### Scenario: A JSON call

- **WHEN** a client posts `{"pair":"EUR/USD","amount":1000}` to `/v1/relays/orders-eu/place`
  with a token allowed to send in the namespace
- **THEN** the response is `200`, `application/json`, carrying the relay's answer

#### Scenario: A send answers 202

- **WHEN** a client posts `checked` to `/v1/relays/orders-eu/audit`
- **THEN** the response is `202` with `{"accepted": true}`

#### Scenario: A wrong channel answers 404

- **WHEN** a client posts to `/v1/relays/orders-eu/plce`
- **THEN** the response carries `api.channel_unknown` with the status of its class

#### Scenario: A partition group is called by key

- **WHEN** a client posts `x` to `/v1/relays/prices/in?key=EURUSD` and `prices` is a partition group
- **THEN** the member owning `EURUSD` answers, and the same post without `?key=` is refused
  `call.target_unkeyed`

#### Scenario: A relay that does not answer times out as a call

- **WHEN** the listener's `deadline_ms` is 3000 and a client posts to a relay channel whose graph never answers
- **THEN** the response SHALL carry `call.timeout` with the status of its class, before the listener's own deadline, and no `control.timeout` SHALL be answered

### Requirement: A remote streaming operation SHALL be served as server-sent events at the stream route

`POST /v1/stream/<operation>` with a JSON argument array as the body SHALL run a `remote`
streaming operation through the door with the caller context built from the request's token,
and SHALL write each event the operation emits as one server-sent event as soon as it is
emitted. The event name SHALL be the event's tag and its data SHALL be the door's JSON encoding of
the event. When the operation returns, its answer SHALL be written as a final `end` event and the
response SHALL close. A refusal before the first event SHALL be answered as a plain error response
with the api-errors body. When the client disconnects, the operation SHALL end and anything it
attached SHALL be detached. Open streams SHALL be capped by `max_streams`, a stream beyond it
answered `503`. A stream whose unwritten events exceed `max_pending_events` SHALL be ended with an
`overrun` event carrying the count of events not written.

#### Scenario: A relay's output is pushed as it happens

- **GIVEN** a token granting `relay.listen` in `orders`, and relay `prices` running there
- **WHEN** the client posts `["orders", "prices", "output"]` to `/v1/stream/Relay.listen` and the
  relay emits an output
- **THEN** the client SHALL receive an `output` event carrying that payload while the response
  stays open

#### Scenario: A refused stream is a plain error

- **WHEN** a `viewer` token posts to `/v1/stream/Logs.tail` and its role does not grant the
  action
- **THEN** the response SHALL be the refusal's status with an `authz.*` error body and no event

#### Scenario: A client that leaves ends the stream

- **WHEN** a client streaming `Relay.listen` closes its connection
- **THEN** the listener attached for it SHALL be detached, and the relay SHALL keep running

#### Scenario: Too many streams

- **GIVEN** `max_streams` streams are open
- **WHEN** another stream is requested
- **THEN** it SHALL be answered `503` and no operation SHALL run for it

#### Scenario: A client that falls behind is cut off, not buffered

- **GIVEN** a client that stops reading while its stream keeps emitting
- **WHEN** the unwritten events exceed `max_pending_events`
- **THEN** the stream SHALL end with an `overrun` event naming the count, and the controller's
  memory for it SHALL be released

### Requirement: A listener off loopback SHALL require a client certificate

`http_api` SHALL accept `tls_client_cacertfile`. When it is set, the listener SHALL require every
client to present a certificate chaining to that authority and SHALL refuse a connection without
one during the TLS handshake. A TLS listener bound to a non-loopback address without
`tls_client_cacertfile` SHALL be refused at enable, unless `require_client_cert: false` is set,
which SHALL log a warning naming the listener on every enable and every boot. A loopback listener
SHALL need neither, nor SHALL a `plain_http` listener, whose TLS ends at a proxy in front of it.

#### Scenario: A public listener without a client authority is refused

- **WHEN** `http_api` is enabled with `bind=10.0.0.5`, a certificate and key, and no
  `tls_client_cacertfile`
- **THEN** the enable SHALL be refused with a code naming the rule, and no port SHALL be bound

#### Scenario: A client without a certificate is refused

- **GIVEN** `http_api` bound to `10.0.0.5` with `tls_client_cacertfile`
- **WHEN** a client connects without a certificate, or with one from another authority
- **THEN** the TLS handshake SHALL fail and no request SHALL be read

#### Scenario: A client with a certificate and a token is served

- **GIVEN** the same listener
- **WHEN** a client with a certificate from that authority calls `POST /v1/call/Relay.list` with a
  valid token
- **THEN** the call SHALL be answered as on a loopback listener

#### Scenario: The opt-out is warned

- **WHEN** `http_api` is enabled with `bind=10.0.0.5`, TLS, and `require_client_cert: false`
- **THEN** the listener SHALL start and a warning naming it SHALL be logged, and again on every
  boot while it stays enabled

### Requirement: A TLS listener SHALL speak HTTP/2

A TLS `http_api` listener SHALL offer HTTP/2 by ALPN beside HTTP/1.1, so a client MAY run calls
and several streams over one connection.

#### Scenario: Two streams on one connection

- **WHEN** an HTTP/2 client opens two streams and makes a call over one TLS connection
- **THEN** all three SHALL be served, each stream receiving its own events

#### Scenario: HTTP/1.1 still works

- **WHEN** an HTTP/1.1 client calls the same listener
- **THEN** it SHALL be served

