# http-push-plugin Specification

## Purpose
The first-party `relay_http_push` provider sends each payload a relay emits (or ingests) as one HTTP request, carrying the output's identity, holding a delivery-guaranteed output until a 2xx or parking it as a dead letter, from a bounded queue shared by the relays bound to it.
## Requirements
### Requirement: `relay_http_push` SHALL send each payload as one HTTP request

The first-party provider `relay_http_push` SHALL, for each payload dispatched to its
`on_output` or `on_input` hook on a channel it sends, issue one HTTP request with the
configured `method` (`POST` by default, or `PUT`) to its `url`, the payload as the body
unchanged, `content-type` set to `content_type`, and the configured `headers`. It SHALL keep
its connections open between requests and, with `max_in_flight` at its default of 1, SHALL
send payloads over one connection in the order they were dispatched.

#### Scenario: An output reaches the endpoint

- **GIVEN** a local HTTP test server and a relay bound to a `relay_http_push` plugin
  on its output
- **WHEN** the relay emits three outputs
- **THEN** the server SHALL receive three `POST` requests in emit order, each body equal to
  the output's payload, over one connection

#### Scenario: Another channel is not sent

- **GIVEN** the plugin set to `channel: alerts`
- **WHEN** the relay emits on `alerts` and on `prices`
- **THEN** the server SHALL receive only the output on `alerts`

### Requirement: `relay_http_push` SHALL validate its options without I/O

`url` SHALL be required, `http` or `https`, with a host and no credentials, refused otherwise
with `plugin.url_scheme_unsupported`, `plugin.url_without_host` or
`plugin.url_has_credentials`. `method` SHALL be `POST` or `PUT`. `headers` SHALL be a JSON
object of string or number values; a name that is not an HTTP token, a value outside visible
ASCII, space and tab, or a name `content-type`, `content-length`, `host`,
`transfer-encoding`, `connection`, `idempotency-key` or starting `flz-` SHALL be refused with
`plugin.headers_invalid` naming the header. `max_in_flight`, `retries`, `timeout_ms`, the
backoff bounds, `delivery_timeout_ms`, `failure_threshold` and the queue bounds SHALL be
bounded integers, and `queue_overflow` one of `drop_nothing`, `drop_oldest`, `drop_newest`.
Validation SHALL open no connection.

#### Scenario: A spoofed identity header

- **WHEN** the plugin is enabled with `headers` `{"flz-key": "x"}`
- **THEN** the enable SHALL be refused with `plugin.headers_invalid` naming `flz-key`

#### Scenario: Credentials in the URL

- **WHEN** the plugin is enabled with `url` `https://u:p@api.test/in`
- **THEN** the enable SHALL be refused with `plugin.url_has_credentials`

### Requirement: Every request SHALL carry the output's identity

A request SHALL carry the headers `flz-key`, `flz-cell`, `flz-seq` and `flz-epoch` for each of
`flz.key`, `flz.cell`, `flz.seq` and `flz.epoch` the payload's metadata holds. An output whose
metadata holds `flz.cell`, `flz.seq` and `flz.epoch`, as every delivery-guaranteed output's
does, SHALL also carry `idempotency-key` with the value `<cell>/<seq>/<epoch>`, the same on
every retry and every redrive of that output. Each value SHALL be sent percent-encoded outside
visible ASCII and at its `%` characters, and unchanged otherwise.

#### Scenario: A retried guaranteed output keeps its key

- **GIVEN** a relay with `effectively_once` and a test server answering 503 once and then
  200
- **WHEN** the relay emits one output
- **THEN** the server SHALL receive two requests with the same `idempotency-key`

### Requirement: The dispatch SHALL wait only on a full queue

Every payload the plugin sends SHALL be handed to the plugin's queue, and the dispatch SHALL
return without waiting on the endpoint. Under `queue_overflow` `drop_nothing` the dispatch
SHALL wait only while the queue is full, until there is room or `queue_block_timeout_ms`
passes. A plain output SHALL be retried on 408, 429, 5xx, a timeout or a transport error along
the backoff curve while its attempts after the first stay within `retries` and the time since
its first attempt within `delivery_timeout_ms`, and SHALL then be dropped, counted and logged
once per reason per plugin. Any other status SHALL drop it at once.

#### Scenario: A failing endpoint does not stop the relay

- **GIVEN** a relay bound to the plugin and a test server answering 500
- **WHEN** 100 plain outputs are dispatched
- **THEN** every dispatch SHALL return without waiting on the server, and the plugin's drop
  count SHALL reach 100 once every retry is spent

### Requirement: A guaranteed output SHALL be held until a 2xx or become a dead letter

For a delivery-guaranteed output the plugin SHALL take the outbox hold before queueing it,
SHALL confirm it on a 2xx answer, and SHALL retry it on 408, 429, 5xx, a timeout or a
transport error along the backoff curve (waiting at least a `retry-after` given in seconds,
at most `retry_max_backoff_ms`) while its attempts after the first stay within `retries` and
the time since its first attempt within `delivery_timeout_ms`. It SHALL park the output with
the error `http_push.exhausted` naming the last reason when that budget runs out, and with
`http_push.rejected` naming the status on any other status, including a 3xx. A redirect SHALL
NOT be followed. A guaranteed output still queued when its plugin stops SHALL be parked with
`http_push.exhausted`. While the plugin's worker is not running on the controller, a
guaranteed output SHALL be held and left in the outbox, and a plain one dropped and counted.

#### Scenario: A receiver that recovers

- **GIVEN** a relay with `effectively_once` and a test server that fails every request for
  2 seconds
- **WHEN** the relay emits outputs during the outage
- **THEN** the server SHALL receive every one of them after it comes back, in order, and the
  outbox SHALL hold none of them afterwards

#### Scenario: A refused output becomes a dead letter

- **GIVEN** a relay with `effectively_once` and a test server answering 422
- **WHEN** the relay emits one output
- **THEN** the server SHALL receive it once, and the relay's DLQ SHALL hold it with the
  error `http_push.rejected` and the status 422

#### Scenario: A redirect is refused

- **GIVEN** a test server answering 307 with a `location` on another host
- **WHEN** a guaranteed output is sent
- **THEN** no request SHALL reach the other host and the output SHALL be parked with
  `http_push.rejected`

#### Scenario: Retries run out

- **GIVEN** a test server answering 503 and `retries` 2
- **WHEN** a guaranteed output is sent
- **THEN** the server SHALL receive it three times and the output SHALL be parked with
  `http_push.exhausted`

### Requirement: The queue SHALL be bounded and its overflow named

The plugin's queue SHALL hold at most `queue_max` payloads and `queue_max_bytes` payload
bytes, except for payloads already on their way to it when it filled under `drop_nothing`,
which SHALL be admitted. Past the bound, `drop_nothing` (the default) SHALL make the dispatch
wait as above and overflow the payload when `queue_block_timeout_ms` passes, `drop_oldest`
SHALL drop the oldest waiting plain payloads and then the oldest guaranteed one, and
`drop_newest` SHALL overflow the new one. Each overflow SHALL be counted and emitted as
`overflowed`. A guaranteed output that overflows SHALL be parked with `http_push.exhausted`
and the reason `queue_full`, never lost.

#### Scenario: Drop-oldest under an outage

- **GIVEN** `queue_max` 10, `queue_overflow` `drop_oldest`, plain outputs and a server that is
  down
- **WHEN** 30 outputs are queued while one is in flight and the server comes back
- **THEN** the server SHALL receive the one in flight and the last 10 in order, and the plugin
  SHALL report 19 overflowed

#### Scenario: Drop-newest

- **GIVEN** `queue_max` 10, `queue_overflow` `drop_newest` and a server that is down
- **WHEN** 30 plain outputs are queued while one is in flight and the server comes back
- **THEN** the server SHALL receive the one in flight and the first 10 queued after it, and
  the plugin SHALL report 19 overflowed

#### Scenario: Drop-nothing waits and then overflows

- **GIVEN** `queue_max` 10, `queue_overflow` `drop_nothing` and a server that is down
- **WHEN** a payload is dispatched to the full queue
- **THEN** the dispatch SHALL wait about `queue_block_timeout_ms`, the payload SHALL be counted
  as overflowed, and a guaranteed one SHALL be parked with `http_push.exhausted`

### Requirement: The plugin SHALL report a stalled endpoint and never show secrets

After `failure_threshold` (default 3) consecutive failed requests the plugin SHALL report
`{:unhealthy, :send_stalled}`, and SHALL report healthy after the next 2xx. No log line or
telemetry event SHALL contain the URL's path or query or any header value, and neither SHALL
the status of the plugin's worker or of its shared copy.

#### Scenario: Health follows the endpoint

- **GIVEN** a test server answering 500 and then, after a while, 200
- **THEN** the plugin SHALL be unhealthy after three failed requests and healthy after the
  first 2xx, and no captured log line SHALL contain the URL's path

### Requirement: Relays bound to one plugin SHALL share its worker

`relay_http_push` SHALL declare itself shareable; every relay bound to one such plugin on
a controller SHALL send through the same worker, connection and queue.

#### Scenario: Two relays, one connection

- **GIVEN** two relays on one controller bound to one `relay_http_push` plugin
- **WHEN** each emits an output
- **THEN** the test server SHALL see both requests on one connection

