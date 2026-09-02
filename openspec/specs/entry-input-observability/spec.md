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

For every declared entry channel, every SDK SHALL record the instant it last delivered a frame to that channel, updated on the delivery path itself (an atomic store; no allocation, no lock held across the store) and interned by the channel's bare name, so one name is one observed input however many classes declare it. The SDKs hosted on `flz_core` — Rust, Java, Node, Ruby, PHP and Python — SHALL share the core's one cell per name; the Elixir SDK, which is its own core, SHALL re-express the same cell (one `:atomics` cell per name, stamped before the frame can be declined, on a monotonic clock anchored at the runtime's start so that the never-delivered value is `0` there too). The instant SHALL be readable through the runner handle by channel name, SHALL read as absent until the first frame is delivered and for a name no declared operative has, and SHALL be readable before the runner is started. Reading it SHALL never block delivery.

#### Scenario: Delivery stamps the instant

- **WHEN** the runner delivers a frame to entry channel `trades_in`
- **THEN** a subsequent read of `trades_in`'s last-delivery instant SHALL return a time at or after the delivery

#### Scenario: Absent before the first frame

- **WHEN** an operative has started and `trades_in` has never received a frame
- **THEN** reading `trades_in`'s last-delivery instant SHALL return absent, not a zero or start-time value

#### Scenario: A name nothing declares reads absent and interns nothing

- **WHEN** the application reads the last-delivery instant of a channel name no declared operative has, before or after the runner is started
- **THEN** the read SHALL return absent and SHALL NOT create an observation cell for that name

### Requirement: Idle observer fires once per idle episode, never on cold start

Every SDK SHALL accept, per entry channel, an idle-observer registration carrying a threshold duration and a callback, legal before and after the runner is started. On an SDK-owned tick (period a quarter of the threshold clamped to `[250 ms, 1 s]`, never on the delivery path), the SDK SHALL invoke the callback exactly once per idle episode — when at least one frame has ever been delivered and the gap since the last delivery first exceeds the threshold. Cold start (no frame ever delivered) SHALL never fire the observer. When a frame arrives after a fired episode, the SDK SHALL invoke the observer's resume notification once and re-arm the episode latch. The latch SHALL compare delivery stamps, not clock readings, so a resume is exact. For the SDKs hosted on `flz_core` the latch SHALL be the core's one implementation, reached by the Rust SDK directly and by the veneers over the C ABI (`flz_core_idle_watch_new` / `_poll` / `_free`), so no host re-derives the state machine; the Elixir SDK SHALL re-express it. The tick SHALL run where the host has a thread of its own for it — a dedicated thread (Rust, Java, Ruby, Python), a timer on the main event loop (Node), between polls of the thread that started the runner (PHP), a timer message in an SDK process (Elixir) — and the "within one tick" bound holds when that thread is free: on a host where the observer shares its thread with handler bodies or with the interpreter lock, a body that runs past the tick delays the report by its length and never changes the count. A callback that fails in the host's own way (a panic, a throw, a raise, an exit) SHALL be caught and logged; delivery and the tick SHALL continue unaffected.

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

#### Scenario: A held host thread delays the report and never the count

- **WHEN** the thread that ticks is held by a handler body past the moment an episode became reportable — a synchronous body on Node's single loop, a body under PHP's `sync` executor, a pure-Ruby or pure-Python body holding the interpreter lock
- **THEN** the idle callback SHALL be invoked once, on the first tick the host reaches after the body returns, and a frame that arrived meanwhile SHALL be reported as one resume rather than as a second episode

