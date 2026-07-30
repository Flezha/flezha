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

