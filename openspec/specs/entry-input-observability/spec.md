# entry-input-observability Specification

## Purpose

The SDK owns the IO threads, so an application never sees the data stream: when
input goes silent every procedure simply stops being called, output freezes, and
the process stays alive with no signal to observe. This capability exposes the
one fact only the SDK has — when each entry channel last received a frame — and
an observer that reports silence against an application-supplied threshold.

The split is the point: the *signal* (when a frame last arrived) is SDK
knowledge, the *policy* (what gap is anomalous, and what to do about it) is
domain knowledge, so the SDK ships no default threshold and no default action.
See `docs/sdk/entry_input_idle.md`.

## Requirements
### Requirement: Per-entry-channel last-delivery instant is tracked and readable

For every declared entry channel, the SDK SHALL record the instant it last delivered a frame to that channel, updated on the delivery path itself (an atomic store; no allocation, no lock held across the store). The instant SHALL be readable through the runner handle by channel name, and SHALL read as absent until the first frame is delivered. Reading it SHALL never block delivery.

#### Scenario: Delivery stamps the instant

- **WHEN** the runner delivers a frame to entry channel `trades_in`
- **THEN** a subsequent read of `trades_in`'s last-delivery instant SHALL return a time at or after the delivery

#### Scenario: Absent before the first frame

- **WHEN** an operative has started and `trades_in` has never received a frame
- **THEN** reading `trades_in`'s last-delivery instant SHALL return absent, not a zero or start-time value

### Requirement: Idle observer fires once per idle episode, never on cold start

The SDK SHALL accept, per entry channel, an idle-observer registration carrying a threshold duration and a callback. On an SDK-owned background tick (period derived from the threshold, never on the delivery path), the SDK SHALL invoke the callback exactly once per idle episode — when at least one frame has ever been delivered and the gap since the last delivery first exceeds the threshold. Cold start (no frame ever delivered) SHALL never fire the observer. When a frame arrives after a fired episode, the SDK SHALL invoke the observer's resume notification once and re-arm the episode latch. A callback that panics (Rust) or throws (Java) SHALL be caught and logged; delivery and the background tick SHALL continue unaffected.

#### Scenario: Gap past threshold fires once

- **WHEN** `trades_in` has delivered frames, then delivers nothing for longer than the registered threshold
- **THEN** the idle callback SHALL be invoked exactly once for the episode, and not again while the silence continues

#### Scenario: Cold start never fires

- **WHEN** an operative runs for longer than the threshold with no frame ever delivered to the observed channel
- **THEN** the idle callback SHALL NOT be invoked

#### Scenario: Resume re-arms the episode

- **WHEN** a frame arrives on the observed channel after an idle episode fired
- **THEN** the resume notification SHALL be invoked once, and a later gap past the threshold SHALL fire the idle callback again

#### Scenario: A throwing observer cannot disturb delivery

- **WHEN** the registered callback panics or throws on invocation
- **THEN** the SDK SHALL log the failure, frame delivery SHALL be unaffected, and subsequent episodes SHALL still invoke the observer

