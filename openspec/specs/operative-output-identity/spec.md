# operative-output-identity Specification

## Purpose
Replay-stable identity on emitted outputs: per-channel `.keyed()` opt-in, `emit_keyed`, the `flz.segment`/`flz.seq`/`flz.key` reserved stamps, the protected `flz.` metadata namespace, and the `flz.seq` exactness boundary. Non-keyed channels stay byte-identical and cost-free.

## Requirements

### Requirement: Keyed channel declaration

The channel builder SHALL support a `.keyed()` declaration on plugin-bound and
hybrid (`also_to_plugin_with`) channels, recorded on the channel entry. Declaring
`.keyed()` on a pure inter-operation channel (no plugin wire) SHALL be a build
error. The channel declaration is the single source of truth for whether output
identity exists on that channel.

#### Scenario: Keyed plugin channel accepted

- **WHEN** a topology declares `c.channel::<Candle>("ohlc").from::<OhlcOp>()
  .also_to_plugin_with("kafka", ser).keyed()`
- **THEN** the operative builds and the channel entry is marked keyed

#### Scenario: Keyed inter-operation channel rejected

- **WHEN** a topology declares `.keyed()` on a channel with no plugin sink
- **THEN** the build fails with a typed error naming the channel

### Requirement: `emit_keyed` supplies the natural key on keyed channels

`OperationCtx::emit_keyed(key, value)` SHALL emit on the invocation's channel with
the caller-supplied natural key. On a keyed channel, plain `emit` /
`emit_with_meta` (no key) SHALL fail with a typed error; on a non-keyed channel,
`emit_keyed` SHALL fail with a typed error. The key SHALL be replay-stable by the
determinism contract (derived from input/state/ctx, never from ambient sources).

#### Scenario: Keyed emit on keyed channel

- **WHEN** `run` calls `ctx.emit_keyed("BTC/1m/1720000000", candle)` on a keyed
  channel
- **THEN** the output is emitted and carries the supplied key

#### Scenario: Un-keyed emit on keyed channel rejected

- **WHEN** `run` calls `ctx.emit(candle)` on a keyed channel
- **THEN** the call returns a typed `OperationError` and nothing is emitted

#### Scenario: Keyed emit on non-keyed channel rejected

- **WHEN** `run` calls `ctx.emit_keyed(key, value)` on a channel not declared
  `.keyed()`
- **THEN** the call returns a typed `OperationError` and nothing is emitted

### Requirement: Identity stamps on keyed-channel output frames

Every plugin-wire emit on a keyed channel SHALL carry reserved metadata stamps:
`flz.segment` (the emitting Segment's identity as
`operative_class/operation_id/segment_key`), `flz.seq` (the current durable change
sequence plus the emit ordinal within the invocation, formatted
`<change_seq>.<emit_ordinal>`), and `flz.key` (the natural key from `emit_keyed`).
Stamps SHALL be appended after user metadata so they cannot be clobbered. The
stamps SHALL be replay-stable: replaying the same inputs over the same restored
state reproduces identical stamp values.

#### Scenario: Stamps present and replay-stable

- **WHEN** a keyed-channel emit is produced, the runner crashes, state is
  restored, and the same input is replayed
- **THEN** the re-emitted frame carries `flz.segment`, `flz.seq`, and `flz.key`
  values identical to the original emission

#### Scenario: Controller passes stamps through to the plugin chain

- **WHEN** a stamped output frame reaches the controller
- **THEN** the `:on_output` plugin chain observes the `flz.*` metadata unchanged

### Requirement: `flz.seq` exactness boundary

The spec SHALL state the exactness boundary of `flz.seq`: it is a sufficient dedup
handle for changelog-style consumers that upsert on `flz.key`, but it is NOT a
sufficient handle for exact append-only dedup until a durable outbox journals
output identity (consecutive invocations that apply no state change can produce
colliding `<change_seq>.<emit_ordinal>` pairs). Documentation SHALL warn against
building append-only dedup on `flz.seq` alone.

#### Scenario: Collision case is documented and detectable

- **WHEN** two consecutive invocations of a Segment emit without applying any
  state change
- **THEN** their `flz.seq` stamps may collide, and the documented guidance directs
  exact append-dedup consumers to the effectively-once outbox capability

### Requirement: Protected `flz.` metadata namespace

User-supplied metadata keys beginning with `flz.` SHALL be rejected at emit time
with a typed error, on all emit variants, so runtime stamps can never be spoofed or
clobbered by operative code.

#### Scenario: Spoofed stamp rejected

- **WHEN** `run` calls `ctx.emit_with_meta(value, {"flz.seq": "999.0"})`
- **THEN** the call returns a typed `OperationError` and nothing is emitted

### Requirement: Non-keyed channels are unchanged and cost-free

Channels not declared `.keyed()` SHALL take the existing emit path byte-for-byte:
no identity stamps, no additional per-emit work. The reference dispatch benchmark
SHALL show no regression on non-keyed emit paths.

#### Scenario: Non-keyed frame is byte-identical

- **WHEN** an operative emits on a channel without `.keyed()`
- **THEN** the produced wire frame is identical to the pre-change frame layout
  (instance id, payload, `channel` metadata, user metadata only)

#### Scenario: No perf regression on the hot path

- **WHEN** the existing dispatch throughput bench runs on a non-keyed workload
- **THEN** throughput is within the established regression tolerance of the
  pre-change baseline
