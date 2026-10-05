# HTTP server plugin

The HTTP server plugin puts a relay behind HTTP: a client sends a request, the
relay's graph decides the answer, and the same connection can instead carry a stream of
another relay's parts and outputs as server-sent events. `FlezhaHttp.Provider` is its
provider, under the provider id `http_server`, in a plugin package built in its own
repository, `Flezha/flezha_http_plugin`, which this repository carries as the submodule
`plugins/flezha_http_plugin`; its README covers building and testing it.

| | |
| --- | --- |
| Type | relay plugin |
| Ships | package `flezha_http` |
| Shareable | no: a second relay bound to it is refused (`plugin.not_shareable`) |
| Partitioned relays | refused (`plugin.partitioned_host`): a group behind one port needs a router by key |
| Durability | a request is one outside call of the relay, answered when the relay answers; the plugin retries nothing, and the relay's own settings decide what its input survives |

## Options

<!-- provider-options:begin -->
| Option | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `port` | int | none | yes | The TCP port the server listens on, 1 to 65535 |
| `bind` | string | `"127.0.0.1"` | no | The address it listens on: an IPv4 or IPv6 literal, never a name |
| `channel` | names | none | no | The entry channel every request is injected on, one name. Required, unless `channel_in` names it |
| `channel_in` | names | none | no | The explicit spelling of `channel`. Required, unless `channel` names it |
| `answer_channel` | string | none | no | The relay's channel whose output answers a request; the first output on a sink when unset |
| `deadline_ms` | int | `30000` | no | How long a request waits for the relay's answer, 100 to 600000 |
| `max_body_bytes` | int | `1048576` | no | The largest request body accepted, up to 16777216; a larger one is answered 413 |
| `max_waiting` | int | `1000` | no | Requests waiting for an answer at once, up to 100000; one more is answered 503 |
| `max_streams` | int | `1000` | no | Event streams open at once, up to 100000; one more is answered 503 |
| `pass_headers` | names | `[]` | no | Request headers copied into the request envelope, lower-case names; none by default |
| `max_connections` | int | `1000` | no | Connections open at once, streams and waiting requests included; one more is closed at once |
| `heartbeat_ms` | int | `15000` | no | How long a quiet stream waits before it writes a comment line, 1000 to 300000 |
| `stream_wait_ms` | int | `5000` | no | How long a stream waits for the relay it names to exist, up to 60000; 0 answers 404 at once |
<!-- provider-options:end -->

## Example

On a running relay whose entry channel is `http`:

```sh
$ flz plugin enable web http_server \
  --relay_id=gateway \
  --Xport=8080 \
  --Xchannel=http \
  --Xpass_headers=x-api-key
```

## Contract

| | |
| --- | --- |
| Package | `flezha_http`, built on Bandit and Plug, installed with `flz plugin install` and loaded at the next boot (see [installing_plugins.md](../topics/plugins/installing_plugins.md)) |
| Hooks | none needed: the server drives itself, so a binding names no hook (the provider also defines `:on_terminate`) |
| Server | HTTP/1.1 only, on `bind` and `port`, started when the plugin activates and stopped when it is disabled or its relay stops |
| Request | one outside call of the relay the plugin is enabled on, on its entry channel, with a JSON request envelope as the input |
| Answer | the first sink output the request causes, a JSON answer envelope: a plain response, or a stream |
| Host | any relay, in flow or in steps; a member of a partition group is refused (`plugin.partitioned_host`) |

## Install and enable

```sh
$ flz plugin install flezha_http-0.1.0.fpz
$ flz controller stop && flz controller start
$ flz plugin list --installed
```

```output
flezha_http 0.1.0  loaded  providers: http_api, http_server
```

The package loads at boot. Once it is loaded, the plugin can also be declared at start
(`flz relay start <factory> --id gateway --plugins <file>`):

```json
[
  {
    "name": "web",
    "provider": "http_server",
    "opts": {"port": "8080", "channel": "http", "pass_headers": "x-api-key"},
    "hooks": ["on_terminate"]
  }
]
```

A port that cannot be bound fails the activation with `http.listen_failed`, naming the
address, the port and the reason. `flz relay info gateway` shows, under the plugin, the
address it listens on, the requests waiting for an answer, the streams open and the
requests a cap refused since the server started.

## The request envelope

Every request becomes one input of the relay, on the entry channel, with this payload:

```json
{"method": "POST",
 "path": "/sessions/anna-42/messages",
 "query": "user=anna",
 "headers": {"x-api-key": "k1"},
 "body": "{\"text\":\"hello\"}"}
```

- `method` in upper case; `path` as received, percent-encoding kept; `query` the raw query
  string without `?`, `""` when there is none.
- `headers` holds only the headers `pass_headers` names, by lower-case name; repeated fields
  are joined with `", "`, `cookie` fields with `"; "`. It is `{}` by default.
- `body` is the body as text when it is valid UTF-8 (`""` when there is none); otherwise
  `body_base64` carries its bytes and `body` is absent. The plugin never decodes the body:
  the relay's entry parser does, and answers its own 400.

The input's metadata carries the entry channel and nothing else: no header, query or body
byte reaches it. The plugin has no routes; the relay's graph routes on `method` and
`path`.

## The answer envelope

The relay answers with one JSON object, the first output the request causes on a sink, or the
output on the channel `answer_channel` names. The same output also reaches the sink's plugins and
listeners. Under the effectively-once tier that sink is `.keyed()`, as every sink is.

A plain answer:

```json
{"status": 202, "headers": {"retry-after": "60"}, "body": {"accepted": true}}
```

- `status` an integer from 200 to 599, required.
- `headers` optional: lower-case header names to string values without CR, LF or NUL. The
  framing headers `connection`, `content-length`, `keep-alive`, `proxy-connection`, `te`,
  `trailer`, `transfer-encoding` and `upgrade` are refused.
- `body` optional: a string is sent as its UTF-8 bytes (`text/plain; charset=utf-8`), any
  other JSON value JSON-encoded (`application/json`); `body_base64` carries bytes instead
  (`application/octet-stream`). A `content-type` header overrides the default. A 204 or 304
  carries no body.

A stream answer:

```json
{"stream": "anna-42", "headers": {"access-control-allow-origin": "https://app.example"}}
```

- `stream` names a relay or partition group of the relay's own namespace, a
  non-empty string of at most 255 bytes; `headers` as above; nothing else.

Any other answer (not JSON, not an object, an unknown key such as `namespace`, a status out
of range, `stream` beside `status` or `body`, both `body` and `body_base64`, a refused
header) is answered 502 with `http.answer_invalid`, whose `details.reason` names the rule,
and is logged once at warning level with the relay and the rule, never the payload.

## Status of every outcome

| Outcome | Status |
| --- | --- |
| a plain answer | its `status` |
| a stream answer, listener attached | 200, `text/event-stream` |
| `http.body_too_large`: a `content-length` or a body over `max_body_bytes` | 413 |
| `http.body_timeout`: the body did not arrive in full within 15 s | 408 |
| `http.length_required`: a body sent without `content-length` (chunked) | 411 |
| `http.busy`: `max_waiting` requests already waiting | 503, `retry-after: 1` |
| `http.streams_full`: `max_streams` streams already open | 503, `retry-after: 1` |
| `http.answer_invalid` | 502 |
| `call.timeout`: no answer by `deadline_ms` | 504 |
| `call.busy`, `call.unavailable` | 503, `retry-after: 1` |
| `call.callee_ended` | 503 |
| `call.failed`: the relay dead-lettered the request | 500 |
| `call.absorbed`, `call.not_accepted` | 409 |
| `relay.not_found` for a stream, after `stream_wait_ms` | 404 |
| any other code, by its class | `invalid` 400, `denied` 403, `not_found` 404, `conflict` 409, `unavailable` 503, `unreachable` 502, `internal` 500 |

Every error body is the error model's one-line document, `{"error": {"code", "class",
"message", "details", "hint"}}`, with `content-type: application/json`; the codes are in
[errors.md](../errors.md). An error the controller answered (a `call.*` code, a refused
stream) reaches the client with its code, its class and its catalogue message filled only
from the details that name the relay, the channel and the call's bounds, and no hint:
a tasklet's exception text, a dead-letter entry and an exit reason stay with
`flz relay listen`, `flz dlq` and the controller's log. Bandit's own protocol refusals (a malformed request line, too
many headers) keep Bandit's answer.

## Streams

For a stream answer the plugin attaches a listener to the named relay, in the host's
namespace, before it writes a byte, so a refused attach is a plain error response. A
relay that does not exist yet is attached again every 250 ms until `stream_wait_ms` has
passed, then answered 404; a relay that is asleep is attached at once, and the next
input wakes it. The response then carries `content-type: text/event-stream`,
`cache-control: no-cache`, `x-accel-buffering: no` and the envelope's headers, and one
event per frame:

| Event | When | `data` |
| --- | --- | --- |
| `part` | a part of a running turn | `{"channel", "part", "attempt", "payload"}` |
| `output` | an output on a sink | `{"channel", "payload"}` |
| `dead_lettered` | an input was dead-lettered | the `input.dead_lettered` error document, `details` holding only `relay`, `channel` and `attempts`, `hint` null |
| `ended` | the relay ended; the response ends | `{}` |
| `closed` | the listener ended with an error (`listener.overrun`, `listener.detached`); the response ends | the error document |

`payload` is the payload as text when it is valid UTF-8, otherwise `payload_base64`
carries it. No other metadata is written. The author's error text and the dead-letter entry
are never in `dead_lettered`: its message keeps the catalogue's `{entry}` and `{error}`
placeholders unfilled, and the full notice stays with `flz relay listen` and `flz dlq`.
A comment line `:` is written after every `heartbeat_ms` without an event, which keeps
proxies from closing a quiet stream and is how a client that left is noticed, at the latest
at the second heartbeat after it left; the listener then detaches. No `id:` line is written
and `Last-Event-ID` is ignored: a reconnect starts at the live edge.

In a browser:

```js
const events = new EventSource("/sessions/anna-42/events?user=anna");
let preview = "";
events.addEventListener("part", (e) => {
  const part = JSON.parse(e.data);
  if (part.part === 0) preview = "";          // a retried turn starts its preview again
  preview += part.payload;
});
events.addEventListener("output", (e) => show(JSON.parse(e.data).payload));
events.addEventListener("dead_lettered", (e) => fail(JSON.parse(e.data).error));
events.addEventListener("ended", () => events.close());
```

## Security

- The server binds loopback by default; any wider `bind` is an explicit option.
- `pass_headers` is empty by default, so `authorization` and `cookie` reach no payload, log
  or input log unless named. A named header is journaled with the request wherever the
  relay journals its input.
- The plugin authenticates nobody: the relay checks a passed header, cookie or query
  parameter and answers 401 or 429 through the envelope. CORS headers are the relay's
  too.
- An `EventSource` sends no custom header, so a browser stream authenticates by cookie or
  query. The plugin streams whatever its relay names in its namespace: a relay that
  streams a client-supplied id without checking it against its own state lets any client
  read any relay of the namespace.
- Connections are capped by `max_connections` (a connection past it is closed at once), and
  a request takes its waiting slot before its body is read, so at most `max_waiting` bodies
  of `max_body_bytes` are held at once; a body must carry `content-length` (a chunked one is
  answered 411 unread) and arrive in full within 15 s.
- The plugin logs method, path, status and duration at debug level, never a header, a body
  or a query; an invalid answer's warning names the broken rule with the answer's key and
  header names escaped.

## Deployment

- **A release on a host:** leave `bind` at loopback and put a reverse proxy in front for
  TLS and HTTP/2 to browsers, with response buffering off for `text/event-stream`.
- **Docker:** build the package in the controller image's `plugin-builder` stage, so its
  OTP matches the image's; `bind` must be `0.0.0.0` inside the container and the port
  published (`-p 8080:8080`): a loopback bind inside a container is unreachable from
  outside.
- **Kubernetes:** `bind: 0.0.0.0`, the port declared on the controller container and a
  Service in front. The server runs only on the controller hosting the relay, so a
  Service over several controllers needs a proxy that retries a refused connection or
  health-checks each endpoint's port.
- **A cluster:** the server follows its relay. A handover or a rebalance stops it on the
  old controller (the port closes) and starts it on the new one; requests in flight lose
  their connection, and an `EventSource` reconnects on its own. The relay's keyed state
  moves with it only through a durability backend: with `FLEZHA_DURABILITY_BACKEND=none` a
  gateway starts on the new controller without the state it kept (a session register, a
  budget).

## Limits in v1

- A relay carrying the plugin must not declare a `sleep_after`: an instance found
  asleep when a controller boots has no plugins until something wakes it, and its only
  input is the port that is not bound.
- A member of a partition group is refused: a group behind one port needs a router by key.
- There is no drain: stopping the plugin closes every connection at once.
- No TLS in the plugin and no WebSocket yet; `Idempotency-Key` is not deduplicated (pass it
  and deduplicate in the relay's keyed state).
- The plugin's counters are shown by `flz relay info` only, not exported as metrics.

## See also

- [streamed_answers.md](../topics/advanced/streamed_answers.md): parts and server-sent events
- [relay_calls.md](../topics/advanced/relay_calls.md): outside calls and how a relay's output answers them
- [installing_plugins.md](../topics/plugins/installing_plugins.md): installing a plugin package
- [plugins.md](../topics/plugins/plugins.md): the plugin model
