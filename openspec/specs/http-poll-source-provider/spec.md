# http-poll-source-provider Specification

## Purpose
Feed an Operative's entry channel from a slowly-changing HTTP resource — runtime
configuration, reference data, external state — without restarting the Operative
with new `init_values` and without an operator hand-pushing frames. Injection is
gated on content change, plus one unconditional seed per provider (re)start so a
restarted or handed-over Operative converges on current content within one
interval. All input stays on the journaled entry path, so replay determinism and
the durability tiers are unaffected.
## Requirements
### Requirement: An `http_poll` source provider SHALL inject hash-gated entry frames

A built-in provider `http_poll` SHALL be attachable as a plugin with config `{url, interval_ms ≥ 1000 (default 60000), channel, headers?, timeout_ms?, max_body_bytes? (default 1 MiB)}`. It SHALL poll `url` each interval and inject the response body as an entry frame on `channel` only when the body's SHA-256 differs from the last-injected hash. On provider start and on handover/recovery re-application, the first successful poll SHALL inject unconditionally (seed frame). Attach-time validation SHALL reject a scheme other than http/https, a URL with no host, an interval below the minimum, a non-positive tunable, a body cap above the ceiling, a missing or empty `channel`, malformed headers, and any unrecognised option.

The `channel` name SHALL NOT be checked against the operative's declared entry channels: channel names are an SDK-side topology fact that never reaches the controller (`AddOperativeResponse.metadata` carries capability declarations only), and attach runs before the operative binds to a runner. An undeclared name surfaces as a dispatch failure on the runner, as a mistyped `flz operative send --channel` already does.

#### Scenario: Unchanged content injects nothing

- **WHEN** three consecutive polls return byte-identical bodies after a seed injection
- **THEN** exactly the one seed frame SHALL have been injected

#### Scenario: Changed content injects once

- **WHEN** the polled body changes between two polls
- **THEN** exactly one new frame with the new body SHALL be injected on `channel`

#### Scenario: Rejoining operative is seeded

- **WHEN** an operative recovers/hands over and the plugin is re-applied
- **THEN** the first successful poll SHALL inject the current body even if its hash matches the pre-handover value

#### Scenario: Invalid attach is rejected

- **WHEN** a plugin declares a non-http scheme, a hostless URL, an interval below the minimum, a non-positive tunable, a body cap above the ceiling, a missing `channel`, or an unrecognised option
- **THEN** the attach SHALL fail with an error naming the offending option

### Requirement: Polling SHALL be worker-based, non-blocking, and health-conformant

`init_provider/1` SHALL perform no upstream I/O (first poll deferred to the worker). All HTTP waiting SHALL occur off the host's dispatch path, and off the poller process itself, so a hanging endpoint blocks neither dispatch nor the health probe. Failures (timeout, connect, status outside 2xx, oversized body, non-UTF-8 body, refused ingest) SHALL inject nothing, retain the last-delivered hash AND validator, and count toward the standard provider health contract (consecutive-failure threshold → unhealthy; success → healthy, and inject if the content changed during the outage).

The health verdict SHALL additionally carry the idle window defined in "A poller
that has stopped settling polls SHALL self-report unhealthy": the consecutive-failure
threshold catches a poller that keeps trying and keeps failing, the idle window
catches one that has stopped trying at all. Both verdicts are unhealthy; the
failure run takes precedence when both apply, because it names the actual error.

Injection SHALL use the operative's confirming ingest, and the recorded hash SHALL advance only on a delivered verdict, so a frame the operative did not take is re-offered on the next poll rather than treated as delivered. Redirects SHALL NOT be followed, and the response-size cap SHALL be enforced as the body arrives so it bounds controller memory.

#### Scenario: A refused ingest is re-offered

- **WHEN** the operative refuses an injected frame and the endpoint content does not change
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

