# plugin-http Specification

## Purpose

The HTTP plugins. The `http_poll` source feeds a relay from a slowly changing HTTP resource,
injecting only on a content change plus one seed per start. The `relay_http_push` output sends each
payload as one HTTP request carrying the output's identity, from a bounded queue shared by the
relays bound to it, holding a guaranteed output until a 2xx or parking it as a dead letter. The HTTP
server plugin, a package built outside this repository, puts a relay behind an HTTP/1.1 port: every
request becomes one outside call, its answer becomes the response or a server-sent event stream,
every resource it holds is bounded, and every failure is an HTTP status with the error model's body.

## Requirements

### Requirement: An `http_poll` source provider SHALL inject hash-gated entry frames

A built-in provider `http_poll` SHALL be attachable as a plugin with config `{url, interval_ms ≥ 1000 (default 60000), channel | channel_in, headers?, timeout_ms?, max_body_bytes? (default 1 MiB)}`. It SHALL poll `url` each interval and inject the response body as an entry frame on the channel named by either spelling, only when the body's SHA-256 differs from the last-injected hash. On provider start and on handover/recovery re-application, the first successful poll SHALL inject unconditionally (seed frame). Attach-time validation SHALL reject a scheme other than http/https, a URL with no host, an interval below the minimum, a non-positive tunable, a body cap above the ceiling, an empty or entirely missing entry channel, malformed headers, and any unrecognised option.

The provider SHALL declare that it feeds the relay, so its `channel` resolves as the entry channel by the rule in `plugin-providers`. `channel_in` SHALL be accepted as the explicit spelling of the same name, one of the two SHALL be required, and supplying both SHALL be refused. `channel_out` SHALL NOT be a declared option of this provider: it never acts on records passing through the relay, so the option could not take effect, and it is refused as an option the provider does not offer.

The `channel` name SHALL NOT be checked against the relay's declared entry channels: channel names are an SDK-side topology fact that never reaches the controller (`AddRelayResponse.metadata` carries capability declarations only), and attach runs before the relay binds to a runner. A name that is not one of the relay's entry channels SHALL surface on the runner as a dead-letter park under that name, as a mistyped `flz relay send --channel` does, rather than as a dropped frame.

#### Scenario: Unchanged content injects nothing

- **WHEN** three consecutive polls return byte-identical bodies after a seed injection
- **THEN** exactly the one seed frame SHALL have been injected

#### Scenario: Changed content injects once

- **WHEN** the polled body changes between two polls
- **THEN** exactly one new frame with the new body SHALL be injected on `channel`

#### Scenario: Rejoining relay is seeded

- **WHEN** a relay recovers/hands over and the plugin is re-applied
- **THEN** the first successful poll SHALL inject the current body even if its hash matches the pre-handover value

#### Scenario: Invalid attach is rejected

- **WHEN** a plugin declares a non-http scheme, a hostless URL, an interval below the minimum, a non-positive tunable, a body cap above the ceiling, neither `channel` nor `channel_in`, or an unrecognised option
- **THEN** the attach SHALL fail with an error naming the offending option

#### Scenario: A channel no entry channel matches parks rather than vanishing

- **GIVEN** an attached `http_poll` plugin whose `channel` names no entry channel of the relay it feeds
- **WHEN** the first poll injects a body
- **THEN** the frame SHALL be parked in the dead-letter store under that channel name, and SHALL be replayable onto the channel once the relay declares one of that name

#### Scenario: The explicit spelling names the same channel

- **WHEN** a plugin is attached with `channel_in` and no `channel`
- **THEN** the attach SHALL succeed and frames SHALL be injected on that entry channel

#### Scenario: The chain-side key is refused

- **WHEN** a plugin is attached with `channel_out`
- **THEN** the attach SHALL fail, naming `channel_out` as an option this provider does not offer

### Requirement: Polling SHALL be worker-based, non-blocking, and health-conformant

`init_provider/1` SHALL perform no upstream I/O (first poll deferred to the worker). All HTTP waiting SHALL occur off the host's dispatch path, and off the poller process itself, so a hanging endpoint blocks neither dispatch nor the health probe. Failures (timeout, connect, status outside 2xx, oversized body, non-UTF-8 body, refused ingest) SHALL inject nothing, retain the last-delivered hash AND validator, and count toward the standard provider health contract (consecutive-failure threshold → unhealthy; success → healthy, and inject if the content changed during the outage).

The health verdict SHALL additionally carry the idle window defined in "A poller
that has stopped settling polls SHALL self-report unhealthy": the consecutive-failure
threshold catches a poller that keeps trying and keeps failing, the idle window
catches one that has stopped trying at all. Both verdicts are unhealthy; the
failure run takes precedence when both apply, because it names the actual error.

Injection SHALL use the relay's confirming ingest, and the recorded hash SHALL advance only on a delivered verdict, so a frame the relay did not take is re-offered on the next poll rather than treated as delivered. Redirects SHALL NOT be followed, and the response-size cap SHALL be enforced as the body arrives so it bounds controller memory.

#### Scenario: A refused ingest is re-offered

- **WHEN** the relay refuses an injected frame and the endpoint content does not change
- **THEN** the poll SHALL count as a failure and the next poll SHALL offer the same content again

#### Scenario: A rejected body does not commit its validator

- **WHEN** a body is rejected (oversized or non-UTF-8) and the endpoint would answer a conditional request with `304`
- **THEN** every subsequent poll SHALL keep failing until the content is acceptable, rather than reading as unchanged and clearing the failure run

#### Scenario: Endpoint outage flips health and recovery converges

- **WHEN** the endpoint fails for the threshold count and then recovers with changed content
- **THEN** health SHALL have flipped unhealthy then healthy, and exactly one frame with the new content SHALL be injected

#### Scenario: Oversized body is a failure, not a frame

- **WHEN** a response exceeds `max_body_bytes`
- **THEN** no frame SHALL be injected and the failure reason SHALL name the size cap

#### Scenario: Non-UTF-8 body is a failure when the entry payload is String

- **WHEN** the target entry channel carries `String` payloads (the Rust SDK seals entry types to `String`) and a response body is not valid UTF-8
- **THEN** no frame SHALL be injected and the failure SHALL name the encoding, counting toward the health threshold

#### Scenario: A failure run outranks the idle window

- **WHEN** a poller has both exceeded its consecutive-failure threshold and gone idle past the window
- **THEN** the verdict SHALL name the failure and its last error, not the idle age

### Requirement: The URL SHALL support partition placeholders rendered once at init
The provider SHALL substitute `{{partition_index}}` and `{{partition_count}}` in the `url` opt exactly once, at `init_provider`, from the plugin's partition opts. URL validation SHALL run on the rendered URL. A URL containing these placeholders while the plugin has no partition opts SHALL be refused at init; all other URLs behave exactly as before.

#### Scenario: Per-partition URL
- **WHEN** a member of a 3-partition group declares `url: "https://api.example/trades?shard={{partition_index}}&of={{partition_count}}"`
- **THEN** partition 1 polls `...?shard=1&of=3` and the poll worker itself needs no partition knowledge

#### Scenario: Placeholder without partition opts is refused
- **WHEN** an unpartitioned plugin's URL contains `{{partition_index}}`
- **THEN** init refuses with an error naming the unresolved placeholder

#### Scenario: Validation sees the rendered URL
- **WHEN** the rendered URL is invalid (for example the substitution lands in the host position)
- **THEN** init refuses via the existing URL validation, not at request time

### Requirement: A poller that has stopped settling polls SHALL self-report unhealthy

The `http_poll` provider SHALL track the age of its last SETTLED poll — any
outcome that completes a cycle: injected, unchanged, or failed — and SHALL report
`{:unhealthy, {:poll_idle, age_ms}}` once that age exceeds a configurable idle
window, defaulting to a small multiple of `:interval_ms`. This mirrors the
data-liveness window the `websocket` provider already carries
(`resubscribe_after_ms` → `:data_idle`) and gives the two source providers the
same contract.

A consecutive-failure run is not sufficient on its own: a poller that has stopped
scheduling polls altogether has a failure run of zero and a clean last error, so
it reads healthy under the failure-threshold rule while delivering nothing. A
source that has gone quiet SHALL be unhealthy rather than merely failure-free,
because a quiet source is indistinguishable downstream from a quiet stream — every
producing verdict is gated on input actually arriving.

The window SHALL be measured from the last settled poll, not the last injection:
`http_poll` is content-gated, so a healthy poller against unchanged content
correctly injects nothing for as long as the content holds.

#### Scenario: a poller that stopped scheduling polls is unhealthy
- **GIVEN** an `http_poll` plugin whose worker has not settled a poll for longer than the idle window
- **AND** whose consecutive-failure run is zero
- **WHEN** `health/1` is queried
- **THEN** it SHALL report `{:unhealthy, {:poll_idle, age_ms}}`
- **AND** the convergence loop SHALL plan a `restart_plugin` op under backoff

#### Scenario: unchanged content does not read as idle
- **GIVEN** an `http_poll` plugin polling on cadence against content that never changes
- **WHEN** `health/1` is queried after many intervals with no injection
- **THEN** it SHALL report `:healthy`, because each poll settled as unchanged

#### Scenario: a poll in flight within the window is healthy
- **GIVEN** an `http_poll` plugin with a request outstanding and its last settled poll inside the idle window
- **WHEN** `health/1` is queried
- **THEN** it SHALL report `:healthy`

#### Scenario: the idle window is configurable
- **GIVEN** an `http_poll` plugin configured with an explicit idle window
- **WHEN** the plugin is enabled
- **THEN** the configured value SHALL be validated with the provider's other integer options and used in place of the default

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
and under `drop_nothing` a payload larger than `queue_max_bytes` once the queue is empty,
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

### Requirement: The HTTP server SHALL be a plugin package built outside the Flezha repository

The HTTP server SHALL be the plugin application `flezha_http`, a Mix project in the
repository `Flezha/flezha_http_plugin`, which the Flezha repository carries as the git
submodule `plugins/flezha_http_plugin` on its `main` branch, so the package depends on the core
by the relative path `../..`.
It SHALL declare exactly one provider, `http_server`, in its `flezha_providers` application
environment, SHALL declare no application callback, and SHALL depend on `flezha`, Bandit and
Plug. `mix flezha.plugin.package` SHALL build it into `flezha_http-<version>.fpz` holding
the plugin application and every library the release does not ship (Bandit, Plug,
ThousandIsland, WebSock, `mime`, `plug_crypto`), and no application of Flezha's own
runtime closure. The package SHALL be installed with `flz plugin install` and loaded at the
controller's next boot, after which `http_server` SHALL be a provider `flz plugin enable`
accepts. The repository SHALL carry the Apache-2.0 licence and no continuous-integration
workflow before go-live.

#### Scenario: The package bundles the server libraries and nothing the release ships

- **WHEN** `mix flezha.plugin.package` runs in the plugin repository with `MIX_ENV=prod`
- **THEN** the archive's `lib/` SHALL hold `flezha_http`, `bandit`, `plug`, `thousand_island`, `websock`, `mime` and `plug_crypto`, and SHALL hold no `hpax`, `telemetry`, `jason` or `flezha`

#### Scenario: The installed package offers the provider after a restart

- **WHEN** the archive is installed with `flz plugin install` into a release and the controller is restarted
- **THEN** `flz plugin list --installed` SHALL show `flezha_http` as `loaded`, and `flz plugin list` SHALL list `http_server`

### Requirement: The provider SHALL declare its options and validate them without I/O

The provider SHALL declare these options: `port` (int, required, 1 to 65 535); `bind`
(string, default `127.0.0.1`, an IPv4 or IPv6 literal); the entry channel as `channel` or
`channel_in` (exactly one name, one of the two required); `deadline_ms` (int, default
30 000, 100 to 600 000); `max_body_bytes` (int, default 1 048 576, 1 to 16 777 216);
`max_waiting` (int, default 1 000, 1 to 100 000); `max_streams` (int, default 1 000, 1 to
100 000); `max_connections` (int, default 1 000, 1 to 100 000); `pass_headers` (names, none by default, each a lower-case HTTP token);
`heartbeat_ms` (int, default 15 000, 1 000 to 300 000); `stream_wait_ms` (int, default
5 000, 0 to 60 000). `init_provider/1` SHALL refuse a value outside these rules and SHALL
open no socket, resolve no name and start no process. The plugin SHALL be enabled with no
hook; an enable naming the hook `on_terminate` SHALL still be accepted, and that hook does
nothing.

#### Scenario: Loopback is the default

- **WHEN** the plugin is enabled on a relay with `port` and `channel` only
- **THEN** it SHALL listen on `127.0.0.1` and on no other address

#### Scenario: A bad bind address is refused before anything opens

- **WHEN** the plugin is enabled with `bind` set to `localhost`
- **THEN** the enable SHALL be refused naming `bind`, no row SHALL remain on the relay, and no socket SHALL have been opened

#### Scenario: A header name that is not a lower-case token is refused

- **WHEN** the plugin is enabled with `pass_headers` set to `X-Api-Key`
- **THEN** the enable SHALL be refused naming `pass_headers`

#### Scenario: A port out of range is refused

- **WHEN** the plugin is enabled with `port` set to `70000`
- **THEN** the enable SHALL be refused naming `port` and its maximum

#### Scenario: Enabled with no hook

- **WHEN** the plugin is enabled with `port` and `channel` and no `--hook`
- **THEN** it SHALL listen and every request SHALL reach the relay

### Requirement: The listener SHALL run as the plugin's declared worker and follow the plugin's lifecycle

The server SHALL be the provider's declared worker, started by the extension table when the
plugin activates and stopped when the plugin is disabled or its relay torn down, keyed by
the relay's namespace, the relay's id and the plugin's id. It SHALL serve HTTP/1.1
only. It SHALL stop when its host relay's Overwatch goes away, SHALL release its port
before its stop returns, and SHALL NOT be reused by a new Overwatch while its own host is
gone. A port it cannot bind SHALL fail the activation with the code `http.listen_failed`,
naming the address, the port and the reason. `flz relay info` SHALL show, under the
plugin, the address it listens on, the requests waiting, the streams open and the requests
refused by a cap since it started. This SHALL hold for a host relay of either kind.

#### Scenario: Disabling the plugin closes the port

- **WHEN** the plugin listening on port 18080 is disabled with `flz plugin disable`
- **THEN** a connection to port 18080 SHALL be refused afterwards

#### Scenario: A restarted Overwatch gets a new server on the same port

- **WHEN** the host relay's Overwatch process is killed and restarted from the declared set
- **THEN** the old server SHALL have stopped, a new one SHALL listen on the same port, and a request SHALL be answered by the restarted relay

#### Scenario: A port already taken fails the activation with a code

- **WHEN** a second relay on the same controller enables the plugin on a port the first one already listens on
- **THEN** its activation SHALL fail with `http.listen_failed` naming the address and the port, and the first relay's server SHALL keep answering

#### Scenario: The relay info shows the server

- **WHEN** `flz relay info` is run on a relay carrying the plugin
- **THEN** it SHALL show the listening address and port and the counts of waiting requests, open streams and refusals

### Requirement: The plugin SHALL refuse a partitioned host relay

The provider SHALL declare `partitioned: :refuse` in its source declaration, so the core's
source kit refuses a plugin whose options carry a partition identity when it is brought up, with
`plugin.partitioned_host` naming the provider and the member. `flz plugin enable` on a
partition group SHALL therefore fail on every member with that code and leave no plugin row,
and a group started with the plugin in its declared set SHALL NOT start: its start SHALL be
refused on the existing plugin-prepare failure, whose reason names `partitioned_host`,
and no member SHALL remain. This SHALL hold for a group of either relay kind.

#### Scenario: Enabling on a group is refused

- **WHEN** `flz plugin enable` attaches `http_server` to a two-member partition group
- **THEN** it SHALL fail with `plugin.partitioned_host` for both members, and neither member SHALL carry the plugin

#### Scenario: A group declaring the plugin does not start

- **WHEN** a two-member partition group is started with `http_server` in its `--plugins`
- **THEN** the start SHALL fail naming `partitioned_host`, and no member of the group SHALL run

### Requirement: Every request SHALL become one outside call carrying a request envelope

Every HTTP request the server accepts SHALL become exactly one outside call of the host
relay, made with `Flezha.Plugin.Host.inject/5` in `:call` mode on the configured entry
channel with a deadline of `deadline_ms`. The metadata the plugin hands to `inject/5` SHALL
carry `channel` and nothing else. The input's payload SHALL be one JSON object, the request envelope, holding
`method` (upper case), `path` (the request path as received), `query` (the raw query string,
`""` when there is none), `headers` (an object holding only the request headers
`pass_headers` names, by lower-case name, repeated fields joined with `", "` and `cookie`
fields with `"; "`), and either `body` (the body as text when it is valid UTF-8, `""` when
there is none) or `body_base64` (the body's bytes otherwise). The plugin SHALL NOT decode the
body. No header, query or body byte SHALL reach the input's metadata. The plugin SHALL NOT
log a header or a body.

#### Scenario: The envelope carries only the allowed headers

- **GIVEN** the plugin enabled with `pass_headers` set to `x-api-key`
- **WHEN** a client sends `POST /sessions/anna-42/messages?user=anna` with the headers `x-api-key: k1` and `authorization: Bearer t`, and the body `{"text":"hi"}`
- **THEN** the relay's input SHALL be the envelope with `method` `POST`, `path` `/sessions/anna-42/messages`, `query` `user=anna`, `headers` `{"x-api-key":"k1"}` and `body` `{"text":"hi"}`, and no field of the input SHALL hold `Bearer t`

#### Scenario: A binary body is carried in base64

- **WHEN** a client posts four bytes that are not valid UTF-8
- **THEN** the envelope SHALL carry `body_base64` holding those bytes and no `body`

#### Scenario: A client cannot set a reserved key

- **WHEN** a client sends a request with the header `flz.reply_to: x` and the query `flz.msg_id=y`
- **THEN** the metadata the plugin hands to `inject/5` SHALL hold `channel` alone, and the input SHALL carry no `flz.msg_id` and no `flz.reply_to` but the one its call entry minted

### Requirement: The request body SHALL be bounded

A request whose `content-length` exceeds `max_body_bytes` SHALL be answered 413 with the code
`http.body_too_large` before its body is read, and so SHALL a request whose body grows past
`max_body_bytes` while it is read. A body that has not arrived in full within one deadline
for the whole read SHALL be answered 408 with the code `http.body_timeout`, however slowly
its bytes arrive. A request carrying a body without `content-length` (a chunked body) SHALL
be answered 411 with the code `http.length_required` before any of it is read; a request with
neither a length nor a body SHALL be served. None of these SHALL reach the relay.

#### Scenario: A body over the limit is refused

- **GIVEN** `max_body_bytes` set to 1024
- **WHEN** a client posts 2048 bytes, once with a `content-length` and once chunked
- **THEN** both SHALL be answered 413 with the code `http.body_too_large` and a `limit` of 1024, and the relay SHALL receive no input

### Requirement: Waiting requests SHALL be bounded

At most `max_waiting` requests SHALL wait for their relay's answer at once, counting a
request from before its body is read. A request past that SHALL be answered 503 with
`retry-after: 1` and the code `http.busy` before its body is read and before anything is
injected. At most `max_connections` connections SHALL be open at once; a connection past that
SHALL be closed at once.

#### Scenario: A chunked body is refused unread

- **WHEN** a client posts `hello` with `transfer-encoding: chunked` and no `content-length`
- **THEN** it SHALL receive 411 with the code `http.length_required`, the relay SHALL receive no input, and the same body posted with its `content-length` SHALL be served

#### Scenario: A connection past the cap is closed

- **GIVEN** `max_connections` set to 1 and one open connection
- **WHEN** a second client connects and sends a request
- **THEN** its connection SHALL be closed without an answer, and the first connection SHALL still be served

#### Scenario: The waiting cap refuses at once

- **GIVEN** `max_waiting` set to 2 and a host whose answers take two seconds
- **WHEN** three requests arrive together
- **THEN** one SHALL be answered 503 with `http.busy` at once, and the relay SHALL receive two inputs

### Requirement: A plain answer envelope SHALL become the HTTP response

The plugin SHALL answer with the status, the headers and the body the call's answer names
when that answer is a JSON object with an integer `status` from 200 to 599, and optionally
`headers` and one of `body` and `body_base64`. A string `body` SHALL be sent as its UTF-8
bytes with the default content type `text/plain; charset=utf-8`; any other JSON value SHALL
be sent JSON-encoded with the default `application/json`; `body_base64` SHALL be sent as its
decoded bytes with the default `application/octet-stream`; a `content-type` header SHALL
override the default. This SHALL hold for a host relay of either kind.

#### Scenario: The gateway answers 202 at once

- **WHEN** a flow host's graph emits `{"status":202,"body":{"accepted":true}}` on an output plugin
- **THEN** the client SHALL receive status 202, `content-type: application/json` and the body `{"accepted":true}`

#### Scenario: The gateway refuses a user over budget

- **WHEN** the host answers `{"status":429,"headers":{"retry-after":"3600"},"body":"daily budget spent"}`
- **THEN** the client SHALL receive status 429, the header `retry-after: 3600` and the text body

#### Scenario: A steps host answers a request

- **WHEN** the plugin is enabled on a `per_input` steps relay whose turn emits `{"status":200,"body":"pong"}` on an output plugin, and a client sends `GET /ping`
- **THEN** the client SHALL receive status 200 and the body `pong`

### Requirement: An answer that is not an envelope SHALL be answered 502

The plugin SHALL answer 502 with the code `http.answer_invalid`, naming the relay and the
rule that failed, when the call's answer is not a JSON object; carries a key other than
`status`, `headers`, `body`, `body_base64` and `stream`; has neither `status` nor `stream`;
carries `stream` beside `status`, `body` or `body_base64`; carries both `body` and
`body_base64`; carries a `status` outside 200 to 599 or a body with 204 or 304; carries a
header name that is not a lower-case token, a header value holding CR, LF or NUL, or one of
the framing headers `connection`, `content-length`, `keep-alive`, `proxy-connection`, `te`,
`trailer`, `transfer-encoding` and `upgrade`; or names a `stream` that is empty, longer than
255 bytes or not a string.

#### Scenario: A plain string is not an envelope

- **WHEN** the host answers the payload `hello`
- **THEN** the client SHALL receive 502 with `http.answer_invalid` and a reason saying the answer is not a JSON object

#### Scenario: A namespace cannot be named

- **WHEN** the host answers `{"stream":"anna-42","namespace":"other"}`
- **THEN** the client SHALL receive 502 with `http.answer_invalid` naming the key `namespace`, and no listener SHALL be attached anywhere

#### Scenario: Framing belongs to the server

- **WHEN** the host answers `{"status":200,"headers":{"transfer-encoding":"chunked"},"body":"x"}`
- **THEN** the client SHALL receive 502 with `http.answer_invalid` naming `transfer-encoding`

### Requirement: Every failure SHALL map to one HTTP status with an api-errors body

Every failure the plugin answers SHALL carry the body `{"error": {"code", "class",
"message", "details", "hint"}}` rendered by the error model, with
`content-type: application/json`, and SHALL use this status: `call.timeout` 504;
`call.busy` and `call.unavailable` 503 with `retry-after: 1`; `call.callee_ended` 503;
`call.failed` 500; `call.absorbed` and `call.not_accepted` 409; `http.body_too_large` 413;
`http.body_timeout` 408; `http.length_required` 411; `http.busy` and `http.streams_full` 503 with `retry-after: 1`;
`http.answer_invalid` 502; `relay.not_found` 404; and any other code by its class:
`invalid` 400, `denied` 403, `not_found` 404, `conflict` 409, `unavailable` 503,
`unreachable` 502, `internal` 500. An error the controller answered (the call's, a refused
attach's, a stream's end) SHALL reach the client with its code, its class, its catalogue
message filled only from the details naming the relay, the namespace, the channel, the
attempts and the call's bounds, and no hint; the author's error text, a dead-letter entry and
an exit reason SHALL NOT reach the client. The seven `http.*` codes SHALL be entries of the
one error catalogue.

#### Scenario: A host that does not answer in time

- **GIVEN** `deadline_ms` set to 1000 and a host whose graph answers nothing for `GET /nothing`
- **WHEN** a client sends `GET /nothing`
- **THEN** after about one second the client SHALL receive 504 with the code `call.timeout` and `details.deadline_ms` 1000

#### Scenario: A host that dead-letters the request

- **WHEN** the host's tasklet fails every attempt on a request and the host dead-letters it
- **THEN** the client SHALL receive 500 with the code `call.failed`

#### Scenario: A dead-lettered call's body carries no author's text

- **WHEN** the call answers `call.failed` whose error text is `upstream key sk-123 rejected` and whose entry is `dlq-9`
- **THEN** the client SHALL receive 500 with `call.failed`, `details` holding only `relay`, a null `hint`, and a body containing neither `sk-123` nor `dlq-9`

#### Scenario: A code the plugin does not name is mapped by its class

- **WHEN** the call answers an error whose code the plugin's table does not list and whose class is `denied`
- **THEN** the client SHALL receive 403 with that error's body

#### Scenario: The plugin's codes are catalogued

- **WHEN** the error catalogue is read
- **THEN** it SHALL hold `http.body_too_large`, `http.body_timeout`, `http.length_required`, `http.busy`, `http.streams_full`, `http.answer_invalid` and `http.listen_failed`, each with its class, message and details

### Requirement: A stream answer SHALL stream the named relay of the host's namespace over SSE

When the call answers `{"stream": <id>}`, optionally with `headers`, the plugin SHALL attach
a listener with `Flezha.Service.Listener.listen/4` to the relay or partition group `<id>`
in the host relay's own namespace, and SHALL write nothing before the attach succeeds.
Once it succeeds, the plugin SHALL answer 200 with `content-type: text/event-stream`,
`cache-control: no-cache`, `x-accel-buffering: no` and the envelope's headers, and SHALL
write each event the listener hands it on the same connection. It SHALL never attach a
listener in another namespace. A refused attach SHALL be answered with its code's status
and body. The streamed relay SHALL be either kind, awake or asleep.

#### Scenario: A session's parts, then its answer

- **GIVEN** a gateway that answers `GET /sessions/anna-42/events` with `{"stream":"anna-42"}`, and a `per_input` steps session `anna-42` whose turn sends three parts and then emits its answer on the output plugin `events`
- **WHEN** a client reads the events endpoint with `curl -N` and a message is then posted to the session
- **THEN** the client SHALL read three `part` events in order and then one `output` event carrying the answer

#### Scenario: A flow relay's outputs

- **WHEN** the gateway answers a stream envelope naming a flow relay that emits on an output plugin, and that relay is sent an input
- **THEN** the client SHALL read an `output` event carrying that output

#### Scenario: An asleep session is streamed

- **GIVEN** a session relay that is asleep
- **WHEN** a client opens its stream and a message is then posted to the session
- **THEN** the attach SHALL succeed while the session sleeps, and the client SHALL read the woken turn's parts and answer

#### Scenario: The same id in another namespace is never streamed

- **GIVEN** a relay `anna-42` only in the namespace `other`, and a gateway in `default`
- **WHEN** the gateway answers `{"stream":"anna-42"}`
- **THEN** no listener SHALL be attached in `other`, and the client SHALL receive 404 with `relay.not_found` once `stream_wait_ms` has passed

### Requirement: The SSE events SHALL have fixed names and data

Each event SHALL be one `event:` line, one `data:` line holding one JSON object, and a blank
line. A part SHALL be `part` with `{"channel", "part", "attempt"}` taken from `channel`,
`flz.part` and `flz.part_attempt`, and the payload; an output SHALL be `output` with
`{"channel"}` and the payload; the payload SHALL be `payload` when it is valid UTF-8 and
`payload_base64` otherwise. A dead-lettered input SHALL be `dead_lettered` with the error
document of `input.dead_lettered`, whose `details` SHALL hold only `relay`, `channel` and
`attempts` and whose `hint` SHALL be null. The relay ending SHALL be `ended` with `{}`,
after which the response SHALL end. A listener ending with an error SHALL be `closed` with
that error's document, after which the response SHALL end. No other metadata key SHALL be
written. No `id:` line SHALL be written, and a `Last-Event-ID` header SHALL be ignored. After
every `heartbeat_ms` with no event written, the plugin SHALL write the comment line `:`.

#### Scenario: A part's data

- **WHEN** the streamed relay sends the part `Hel` on the channel `events` as the first part of its first attempt
- **THEN** the client SHALL read `event: part` and `data: {"channel":"events","part":0,"attempt":1,"payload":"Hel"}` in some key order

#### Scenario: A failed turn is told without the author's text

- **WHEN** the streamed session dead-letters a turn whose tasklet raised `upstream key sk-123 rejected`
- **THEN** the client SHALL read a `dead_lettered` event carrying `input.dead_lettered` whose data does not contain `sk-123` and holds no dead-letter entry

#### Scenario: A quiet stream sends heartbeats

- **GIVEN** `heartbeat_ms` set to 1000
- **WHEN** a stream stays without events for three seconds
- **THEN** the client SHALL read at least two comment lines in that time

#### Scenario: The relay ends

- **WHEN** the streamed session completes
- **THEN** the client SHALL read an `ended` event and the response SHALL end

#### Scenario: A reader that falls behind is closed with a code

- **WHEN** the listener ends with `listener.overrun`
- **THEN** the client SHALL read a `closed` event carrying `listener.overrun` and the response SHALL end

### Requirement: A stream SHALL end and detach when its client goes

When a write to a stream's connection fails, the plugin SHALL end the listener, which
detaches its plugin from the streamed relay. A client that closes its connection while
the stream is quiet SHALL be noticed at the latest at the second heartbeat after it left.

#### Scenario: A disconnected client leaves no listener

- **GIVEN** `heartbeat_ms` set to 1000 and a client streaming a session
- **WHEN** the client process is killed
- **THEN** within three seconds `flz relay info` on the session SHALL show no listener plugin

### Requirement: A stream for a relay not running yet SHALL wait a bounded time

When the attach answers `relay.not_found`, the plugin SHALL attach again every 250 ms until
`stream_wait_ms` has passed since the first attempt, and SHALL then answer 404 with that
error. With `stream_wait_ms` set to 0 it SHALL answer 404 at once.

#### Scenario: A session created just after its stream was opened

- **WHEN** a client opens a stream for a session the gateway has registered but whose relay the gateway's call starts one second later
- **THEN** the stream SHALL attach once the relay exists and SHALL answer 200 with the event stream

#### Scenario: A session that never exists

- **GIVEN** `stream_wait_ms` set to 1000
- **WHEN** the gateway answers a stream envelope naming a relay that never starts
- **THEN** the client SHALL receive 404 with `relay.not_found` after about one second

### Requirement: Open streams SHALL be bounded

At most `max_streams` streams SHALL be open at once, counting a stream from the moment its
envelope is read until its response ends. A stream past that SHALL be answered 503 with
`retry-after: 1` and the code `http.streams_full`, and no listener SHALL be attached for it.

#### Scenario: The stream cap refuses

- **GIVEN** `max_streams` set to 1 and one open stream
- **WHEN** a second stream envelope is answered
- **THEN** the second request SHALL receive 503 with `http.streams_full`, and the first stream SHALL go on

### Requirement: The plugin's option table SHALL be documented in the Flezha repository

`docs/public/plugins/http_server.md` in the Flezha repository SHALL be the plugin's user reference,
and SHALL carry the provider's option table between the `provider-options` markers exactly as
`Flezha.Plugin.ProviderBehaviour.Options.render_options_table/1` renders it from the
provider's declaration. A test in the plugin repository SHALL fail when the two differ.

#### Scenario: A new option without its row on the page fails the plugin's suite

- **WHEN** an option is added to the provider's declaration and the page is not updated
- **THEN** the plugin repository's test suite SHALL fail naming the page and printing the table to put there

### Requirement: The HTTP server SHALL NOT bind a port the controller's own HTTP surfaces use

An `http_server` enable SHALL be refused, naming the port and its owner, when its `port` is the
port of an enabled `http_api` binding on that controller or the configured `FLEZHA_HEALTHZ_PORT`.

#### Scenario: The HTTP API's port is refused

- **GIVEN** `http_api` is enabled in `_system` on port 8765
- **WHEN** a namespace enables `http_server` with `port: 8765`
- **THEN** the enable SHALL be refused naming port 8765 and `http_api`

#### Scenario: The health port is refused

- **GIVEN** `FLEZHA_HEALTHZ_PORT` is 9569
- **WHEN** a namespace enables `http_server` with `port: 9569`
- **THEN** the enable SHALL be refused naming port 9569 and `FLEZHA_HEALTHZ_PORT`
