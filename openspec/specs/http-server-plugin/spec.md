# http-server-plugin Specification

## Purpose

The HTTP server plugin, the package `flezha_http` built in its own repository, puts a
relay of either kind behind an HTTP/1.1 port: every request becomes one outside call of
that relay carrying a request envelope, its `response()` answer becomes the response or a
stream of another relay's events as server-sent events, every resource it holds is
bounded, and every failure is an HTTP status with the error model's document.
## Requirements
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

### Requirement: Every failure SHALL map to one HTTP status with an error-model body

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

