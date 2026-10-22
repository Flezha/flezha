# relay-output-identity Specification

## Purpose
Replay-stable identity on emitted outputs: per-channel `.keyed()` opt-in, `emit_keyed`, the `flz.cell`/`flz.seq`/`flz.key` reserved stamps, the protected `flz.` metadata namespace, and the `flz.seq` exactness boundary. Non-keyed channels stay byte-identical and cost-free.
## Requirements
### Requirement: Keyed channel declaration

The channel builder SHALL support a `.keyed()` declaration on plugin-bound
(an `output` with no consumers) and hybrid (an `output` with consumers) channels, recorded on
the channel entry. Declaring `.keyed()` on a pure inter-tasklet channel (no
plugin wire) SHALL be a build error. The channel declaration is the single
source of truth for whether output identity exists on that channel.

#### Scenario: Keyed plugin channel accepted

- **WHEN** a topology declares `.output_with("kafka", ser).keyed()` on an
  `OhlcTask` output stream
- **THEN** the relay builds and the channel entry is marked keyed

#### Scenario: Keyed inter-tasklet channel rejected

- **WHEN** a topology declares `.keyed()` on a channel with no output plugin
- **THEN** the build fails with a typed error naming the channel

### Requirement: `emit_keyed` supplies the natural key on keyed channels

`ctx.emit_keyed(key, value)` SHALL emit on the invocation's channel with
the caller-supplied natural key, in a steps tasklet exactly as in a flow tasklet: no SDK
SHALL refuse a keyed emit, or drop its key or metadata, because the tasklet runs inside a
steps relay. On a keyed channel, plain `emit` /
`emit_with_meta` (no key) SHALL default to the invoking cell's own key whenever
the cell has one — a dynamically keyed cell or one of a declared static
keyset — so the stamped key SHALL equal the routing key by construction; it SHALL
fail with a typed error in a single-cell tasklet, which has no cell key to
default to. On a non-keyed channel,
`emit_keyed` SHALL fail with a typed error. Explicitly supplied keys SHALL be
replay-stable by the determinism contract (derived from input/state/ctx, never
from ambient sources); the cell-key default is replay-stable inherently, the
cell key being part of the invocation's identity.

#### Scenario: Keyed emit on keyed channel

- **WHEN** `run` calls `ctx.emit_keyed("BTC/1m/1720000000", candle)` on a keyed
  channel
- **THEN** the output is emitted and carries the supplied key

#### Scenario: Keyed emit from a steps tasklet

- **WHEN** a durable steps tasklet's `run` calls `ctx.emit_keyed("order-7", ticket)` on a keyed channel, in any SDK
- **THEN** the output is recorded with the key and reaches the output plugin stamped `flz.key` `order-7`, and the same call on a non-keyed channel fails with the same typed error a flow tasklet gets

#### Scenario: Un-keyed emit in a keyed cell defaults to the cell key

- **WHEN** the cell keyed `"BTC/USD"` calls `ctx.emit(candle)` on a keyed
  channel
- **THEN** the output is emitted carrying the key `"BTC/USD"`, byte-identical
  (including `flz.key` stamping and record keying) to
  `ctx.emit_keyed("BTC/USD", candle)`

#### Scenario: Un-keyed emit in a single-cell tasklet rejected

- **WHEN** a single-cell tasklet calls `ctx.emit(value)` on a keyed channel
- **THEN** the call returns a typed `TaskletError` and nothing is emitted

#### Scenario: Keyed emit on non-keyed channel rejected

- **WHEN** `run` calls `ctx.emit_keyed(key, value)` on a channel not declared
  `.keyed()`
- **THEN** the call returns a typed `TaskletError` and nothing is emitted

### Requirement: Identity stamps on keyed-channel output frames

Every plugin-wire emit on a keyed channel SHALL carry reserved metadata stamps:
`flz.cell` (the emitting Cell's identity as
`relay_class/tasklet_id/cell_key`), `flz.seq` (the current durable change
sequence plus the emit ordinal within the invocation, formatted
`<change_seq>.<emit_ordinal>`), and `flz.key` (the natural key from `emit_keyed`).
Stamps SHALL be appended after user metadata so they cannot be clobbered. The
stamps SHALL be replay-stable: replaying the same inputs over the same restored
state reproduces identical stamp values.

#### Scenario: Stamps present and replay-stable

- **WHEN** a keyed-channel emit is produced, the runner crashes, state is
  restored, and the same input is replayed
- **THEN** the re-emitted frame carries `flz.cell`, `flz.seq`, and `flz.key`
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

- **WHEN** two consecutive invocations of a Cell emit without applying any
  state change
- **THEN** their `flz.seq` stamps may collide, and the documented guidance directs
  exact append-dedup consumers to the effectively-once outbox capability

### Requirement: Protected `flz.` metadata namespace

User-supplied metadata keys beginning with `flz.` SHALL be rejected at emit time
with a typed error, on all emit variants, so runtime stamps can never be spoofed or
clobbered by relay code.

#### Scenario: Spoofed stamp rejected

- **WHEN** `run` calls `ctx.emit_with_meta(value, {"flz.seq": "999.0"})`
- **THEN** the call returns a typed `TaskletError` and nothing is emitted

### Requirement: Non-keyed channels are unchanged and cost-free

Channels not declared `.keyed()` SHALL take the existing emit path byte-for-byte:
no identity stamps, no additional per-emit work. The reference dispatch benchmark
SHALL show no regression on non-keyed emit paths.

#### Scenario: Non-keyed frame is byte-identical

- **WHEN** a relay emits on a channel without `.keyed()`
- **THEN** the produced wire frame is identical to the pre-change frame layout
  (instance id, payload, `channel` metadata, user metadata only)

#### Scenario: No perf regression on the hot path

- **WHEN** the existing dispatch throughput bench runs on a non-keyed workload
- **THEN** throughput is within the established regression tolerance of the
  pre-change baseline

### Requirement: The core mints the identity stamps for every host, and the ordinal rule is one rule

The shared core SHALL mint the identity stamps for every SDK, on the output list a host hands it at settle and on the typed Rust host's plugin outputs alike, through one function; no SDK SHALL write `flz.cell`, `flz.seq` or `flz.key` itself. A host SHALL hand each plugin output with the natural key `emit_keyed` gave it, or the Cell key for a keyless emit, in the output's key field. The stamps SHALL be written when, and only when, an output is plugin-targeted, its channel is declared keyed, and its class records no effects: a flow class that does not declare effectively-once and a steps class that opted out of persistence. Under the tier, and for a durable steps class, the controller derives the identity from the durable effect record at flush, and a core that also stamped would produce a second answer to the same question. The per-item emit ordinal SHALL advance on every plugin-targeted output, in the order the host handed them, whether or not that output was stamped, so a class carrying both a keyed and a non-keyed plugin channel yields the same ordinal sequence on every host. The ordinal SHALL be distinct from the ordinal `stable_id` consumes, SHALL start at zero for each work item, and SHALL NOT be reset within one item. `flz.cell` SHALL be spelled `<relay>/<tasklet>/<cell_key or empty>`, the label the core derives for the Cell, and `flz.seq` SHALL read, on a hosted SDK, the change sequence the activation carried at handout, and on the typed Rust SDK the Cell's change sequence read after that emit's own changes are flushed.

#### Scenario: The ordinal advances across emits within one invocation

- **WHEN** one invocation emits twice on a keyed plugin channel at change sequence 5
- **THEN** the first output carries `flz.seq` `"5.0"` and the second `"5.1"`

#### Scenario: A fresh work item restarts the ordinal

- **WHEN** a second work item of the same activation emits once on the same keyed plugin channel
- **THEN** its output's `flz.seq` ordinal is `0`, not a continuation of the previous item's

#### Scenario: A non-keyed plugin emit advances the ordinal without being stamped

- **WHEN** an invocation emits once on a non-keyed plugin channel and then once on a keyed one
- **THEN** the first output carries no `flz.` metadata and the second carries `flz.seq` with ordinal `1`

#### Scenario: A host writes no stamps under the effectively-once tier

- **WHEN** a class declaring effectively-once emits on its keyed plugin channel
- **THEN** the output the core ships carries no `flz.cell`, `flz.seq` or `flz.key`

#### Scenario: A host writes no stamps for a durable steps class

- **WHEN** a durable steps class's tasklet on a Java runner emits on its keyed plugin channel
- **THEN** the output the core ships carries no `flz.cell`, `flz.seq` or `flz.key`, and the output plugin receives them from the controller's flush of the Cell's record

#### Scenario: A host stamps an opted-out steps relay's output

- **WHEN** a steps class that opted out of persistence emits on its keyed plugin channel from a host runner
- **THEN** the output carries `flz.cell`, `flz.seq` and `flz.key` from the core, as an at-least-once flow relay's does

#### Scenario: Every hosted SDK's stamps are byte-identical

- **WHEN** the same keyed class emits the same outputs from the same input on each SDK hosted on `flz_core`
- **THEN** every output's `flz.cell`, `flz.seq` and `flz.key` are equal across the runners, and no SDK's source writes those keys

#### Scenario: The typed Rust SDK reads the sequence after the emit's changes

- **WHEN** a stateful keyed Cell handed an activation at change sequence 5 changes its state once and then emits on its keyed plugin channel, on the typed Rust SDK and on a hosted SDK
- **THEN** the typed Rust output's `flz.seq` carries the sequence after that change is applied, and the hosted output's carries `"5.0"`

