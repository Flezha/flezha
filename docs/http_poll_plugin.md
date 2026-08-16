# HTTP poll plugin: an HTTP resource as an operative input

`Flezha.Plugin.Provider.HttpPoll` is the member of the **source** set whose
upstream is a slowly-changing HTTP resource rather than a socket. It feeds
runtime configuration, reference data, or any external state onto an operative's
entry channel without restarting the operative with new `init_values` and
without an operator hand-pushing frames with `flz operative send`.

Injection is content-gated: the body is hashed and a frame is emitted only when
the hash changes. A steady endpoint therefore costs one request per interval and
zero frames, so a 60-second poll against a config endpoint is close to free.

Everything downstream is ordinary: the payload arrives on the operative's input
stream as a journaled entry frame, so replay determinism, the effectively-once
tier, and the durable inbox all behave exactly as they do for any other source.

## Contract

| | |
|---|---|
| Plugin type | `:io_binding` |
| Extension points | `:on_terminate` — the poller drives itself; the subscription exists because every plugin needs at least one |
| Upstream | one `http://` or `https://` URL, `GET` only, redirects not followed |
| Injection | the response body, on `:channel`, iff its SHA-256 changed |
| Delivery | the confirming ingest — the hash advances only once the operative takes the payload |
| Seed | the first successful poll after (re)start always injects |
| Failure posture | inject nothing, keep the last hash, count toward health |

```elixir
Flezha.Service.PluginManager.enable_plugin(
  "signal-config",
  Flezha.Plugin.new(
    id: "signal-config",
    provider: Flezha.Plugin.Provider.HttpPoll,
    opts: %{
      url: "https://api.internal/internal/signal-config",
      channel: "config_in",
      interval_ms: 30_000
    },
    namespace: "default",
    operative_id: "detector",
    subscriptions: [{:on_terminate, []}]
  )
)
```

As a plugin spec at operative start (`flz operative start --plugins`):

```json
[
  {
    "id": "signal-config",
    "provider": "operative_http_poll",
    "opts": {
      "url": "https://api.internal/internal/signal-config",
      "channel": "config_in",
      "interval_ms": "30000",
      "headers": "{\"authorization\":\"Bearer ${TOKEN}\"}"
    },
    "subscriptions": ["on_terminate"]
  }
]
```

## Parameters

| Option | Required | Default | Meaning |
|---|---|---|---|
| `url` | yes | — | `http://` or `https://` resource to GET |
| `channel` | yes | — | entry channel the body is injected on |
| `interval_ms` | no | `60_000` | poll period; below `1_000` is refused |
| `timeout_ms` | no | `10_000` | connect and response timeout per poll; must be **below** `interval_ms` so a poll cannot outlast its own interval |
| `max_body_bytes` | no | `1_048_576` | the transfer is aborted past this; ceiling `67_108_864` |
| `failure_threshold` | no | `3` | consecutive failures before `health/1` reports unhealthy |
| `headers` | no | `%{}` | static request headers; a JSON object over the CLI |
| `plugin_id` | no | the redacted URL | label carried on telemetry and logs |

Any other key is **refused** at attach rather than ignored, so a config asking
for something this provider does not offer — a POST body, an auth flow, a
backoff ladder — fails loudly instead of reading as accepted. The refusal lives
in `init_provider/1`, which is where a `--plugins` spec actually lands: the
plugin decoder never builds the provider struct, so a check that only guarded
`build/1` would never run for an operator.

### Per-partition URLs

`url` may carry `{{partition_index}}` and `{{partition_count}}`, substituted
**exactly once** at `init_provider` from the partition identity a partitioned
start's plugin fan-out injects:

```
url: "https://api.example/trades?shard={{partition_index}}&of={{partition_count}}"
```

Partition 1 of 3 then polls `…?shard=1&of=3`, and the poll worker itself needs
no partition knowledge at all. URL validation runs on the **rendered** URL, so
a substitution that lands in the scheme or the host is refused at init rather
than at request time. A URL carrying a placeholder while the plugin has no
partition identity is refused, naming the unresolved placeholder. See
[`partitioned_operatives.md`](partitioned_operatives.md).

A URL carrying credentials (`https://user:pass@host/…`) is refused — put the
secret in `headers`. The default `plugin_id`, every log line, and every attach
error use the URL with its userinfo and query stripped, because telemetry labels
are exported to the observability sinks and config endpoints commonly carry an
API key in the query string.

## Change detection

The gate is the SHA-256 of the raw response bytes, which keeps the provider
content-agnostic: JSON, plain text, and binary all work, and an application that
wants field-level semantics puts its own version or hash inside the body.

When the server offers an `ETag`, the next request carries `If-None-Match` and a
`304` is a successful unchanged poll — bandwidth only. The local hash remains the
injection gate, so a server that omits or ignores the validator changes nothing
but transfer size.

## The seed, and why consumers must be idempotent

A freshly started poller holds no hash, so its first success always injects. That
is deliberate: provider state is not portable across a handover, and the seed is
what converges an operative that has just been started, recovered, migrated, or
had its plugin restarted onto current content within one interval — rather than
leaving it stale until the endpoint happens to change.

The cost is that a rejoining operative may see content it already had.
**Consumers of a polled channel must treat its frames idempotently.** For
configuration-shaped payloads — the intended use — that is free: applying the
same config twice is applying it once. The frames are journaled either way, so
replay stays deterministic regardless.

## Delivery is confirmed, not assumed

Injection uses the operative's **confirming** ingest, and the hash advances only
once the operative has taken the payload. That matters because the
fire-and-forget route drops silently in several ordinary states — a parked
operative, an unbound stream, a durable-inbox append that failed — and a hash
advanced against a dropped frame would be a config push lost until the endpoint
next happened to change.

A refused or not-ready ingest is therefore a failure like any other: it counts
toward health and leaves the hash alone, so the next poll re-offers the same
content. `absorbed` (a duplicate the operative already holds) and `suspended`
(journaled, delivered on wake) both count as delivered.

## Failures

Timeouts, connection errors, statuses outside 2xx (including redirects), a body
over `max_body_bytes`, a body that is not valid UTF-8, and a refused ingest are
all failures. A failure injects nothing and **retains the last delivered hash** —
being unable to re-read the resource does not invalidate the content the
operative already holds.

Non-UTF-8 is a failure rather than a frame because the SDKs seal entry payloads
to strings; injecting raw bytes would only move the error to the runner.

A failing poll also leaves the **ETag uncommitted**. Storing a validator for a
body that was rejected would make the next request conditional on it, the server
would answer `304`, that reads as an unchanged poll, and the failure run would
reset — leaving a plugin permanently healthy, permanently silent, and holding no
content at all.

Consecutive failures accumulate. At `failure_threshold` the provider's `health/1`
reports `{:unhealthy, {:poll_failing, reason}}`, which is the standard provider
health contract — convergence escalates from there, restarting the plugin under
its bounded governor. The next success clears the run on the same poll that
reports it, and if the content changed during the outage that poll also injects
it: one recovery, one frame. Note that if convergence has already restarted the
plugin, the fresh poller carries no hash and therefore re-seeds — recovery via
escalation costs one frame whether or not the content moved.

The size cap is enforced **as the body arrives**: the response is streamed and
the request is cancelled at the boundary, so a misbehaving endpoint costs the
controller the cap rather than the response.

Redirects are not followed. `:httpc` would replay the static headers — which is
where a bearer token lives — to a host this provider never validated, and would
silently drop an `https://` request's verification posture on a downgrade to
`http://`. A `3xx` is reported as the status failure it is.

## Lifecycle and threading

The two-phase provider contract is honoured strictly:

* `init_provider/1` validates and normalises and performs **no upstream I/O**, so
  a slow or unreachable endpoint cannot stall plugin preparation.
* `activate/1` — called only after the operative's SDK side confirms bound —
  starts the poller and returns immediately.
* Every HTTP wait happens in a short-lived child of the poller, not in the poller
  itself. The host's dispatch path is never inside an upstream request, and the
  poller stays answerable while a request is outstanding — so `health/1` can
  report on exactly the hanging endpoint that would otherwise block it.

A tick that lands while a request is still outstanding is dropped rather than
queued: a slow endpoint stretches the cadence instead of stacking concurrent
requests.

The host tracks the poller, kills it on unsubscribe, and re-runs both phases when
it dies; the poller monitors the host in turn, so it cannot outlive the operative
it feeds.

## Telemetry

| Event | Measurements | Metadata |
|---|---|---|
| `[:flezha, :plugin, :http_poll, :injected]` | `bytes`, `latency_ms` | `plugin_id`, `channel`, `seed` |
| `[:flezha, :plugin, :http_poll, :unchanged]` | `latency_ms` | `plugin_id`, `channel` |
| `[:flezha, :plugin, :http_poll, :failed]` | `consecutive_failures`, `latency_ms` | `plugin_id`, `channel`, `reason` |

All three bridge to the observability bus under source `:flezha_http_poll`.
`:injected` and `:unchanged` bridge as **metrics**; `:failed` bridges as an
**event**, because its `reason` label carries per-poll values (the observed size
of an oversized body, an ingest verdict) that would be unbounded cardinality on a
metric. Alert on the absence of `:injected`/`:unchanged` or on the plugin's
health verdict, not on a failure-rate metric.

## Limits

* **`GET` only.** No POST, no streaming, no webhooks, no auth flow beyond static
  headers. A resource needing more belongs behind an adapter.
* **No per-item diffing.** The body is one opaque payload; a changed byte
  anywhere re-injects all of it.
* **The `channel` name is not validated at attach.** Channel names are an
  SDK-side topology fact that never reaches the controller — declarations ride
  `AddOperativeResponse.metadata`, which carries capability facts only — and
  attach happens before the operative binds to a runner at all. A name the class
  does not declare surfaces as a dispatch failure on the runner, exactly as a
  mistyped `flz operative send --channel` does.
* **HTTPS is verified** against the system CA store; a host with no usable store
  fails the poll rather than silently dropping to an unverified connection.
* **Requests share the default `:httpc` profile** with the rest of the
  controller. Per-request options (redirects, TLS, timeouts) are set explicitly
  and nothing global is mutated, but connection pooling is shared.
