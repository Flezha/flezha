# operative-output-identity Specification

## Purpose
Replay-stable identity on emitted outputs: per-channel `.keyed()` opt-in, `emit_keyed`, the `flz.entity`/`flz.seq`/`flz.key` reserved stamps, the protected `flz.` metadata namespace, and the `flz.seq` exactness boundary. Non-keyed channels stay byte-identical and cost-free.
## Requirements
### Requirement: Keyed channel declaration

The channel builder SHALL support a `.keyed()` declaration on plugin-bound
(a `sink` with no consumers) and hybrid (a `sink` with consumers) channels, recorded on
the channel entry. Declaring `.keyed()` on a pure inter-procedure channel (no
plugin wire) SHALL be a build error. The channel declaration is the single
source of truth for whether output identity exists on that channel.

#### Scenario: Keyed plugin channel accepted

- **WHEN** a topology declares `.sink_with("kafka", ser).keyed()` on an
  `OhlcProc` output stream
- **THEN** the operative builds and the channel entry is marked keyed

#### Scenario: Keyed inter-procedure channel rejected

- **WHEN** a topology declares `.keyed()` on a channel with no plugin sink
- **THEN** the build fails with a typed error naming the channel

### Requirement: `emit_keyed` supplies the natural key on keyed channels

`ProcedureCtx::emit_keyed(key, value)` SHALL emit on the invocation's channel with
the caller-supplied natural key. On a keyed channel, plain `emit` /
`emit_with_meta` (no key) SHALL default to the invoking entity's own key whenever
the entity has one — a dynamically keyed entity or one of a declared static
keyset — so the stamped key SHALL equal the routing key by construction; it SHALL
fail with a typed error in a single-entity procedure, which has no entity key to
default to. On a non-keyed channel,
`emit_keyed` SHALL fail with a typed error. Explicitly supplied keys SHALL be
replay-stable by the determinism contract (derived from input/state/ctx, never
from ambient sources); the entity-key default is replay-stable inherently, the
entity key being part of the invocation's identity.

#### Scenario: Keyed emit on keyed channel

- **WHEN** `run` calls `ctx.emit_keyed("BTC/1m/1720000000", candle)` on a keyed
  channel
- **THEN** the output is emitted and carries the supplied key

#### Scenario: Un-keyed emit in a keyed entity defaults to the entity key

- **WHEN** the entity keyed `"BTC/USD"` calls `ctx.emit(candle)` on a keyed
  channel
- **THEN** the output is emitted carrying the key `"BTC/USD"`, byte-identical
  (including `flz.key` stamping and record keying) to
  `ctx.emit_keyed("BTC/USD", candle)`

#### Scenario: Un-keyed emit in a single-entity procedure rejected

- **WHEN** a single-entity procedure calls `ctx.emit(value)` on a keyed channel
- **THEN** the call returns a typed `ProcedureError` and nothing is emitted

#### Scenario: Keyed emit on non-keyed channel rejected

- **WHEN** `run` calls `ctx.emit_keyed(key, value)` on a channel not declared
  `.keyed()`
- **THEN** the call returns a typed `ProcedureError` and nothing is emitted

### Requirement: Identity stamps on keyed-channel output frames

Every plugin-wire emit on a keyed channel SHALL carry reserved metadata stamps:
`flz.entity` (the emitting Entity's identity as
`operative_class/procedure_id/entity_key`), `flz.seq` (the current durable change
sequence plus the emit ordinal within the invocation, formatted
`<change_seq>.<emit_ordinal>`), and `flz.key` (the natural key from `emit_keyed`).
Stamps SHALL be appended after user metadata so they cannot be clobbered. The
stamps SHALL be replay-stable: replaying the same inputs over the same restored
state reproduces identical stamp values.

#### Scenario: Stamps present and replay-stable

- **WHEN** a keyed-channel emit is produced, the runner crashes, state is
  restored, and the same input is replayed
- **THEN** the re-emitted frame carries `flz.entity`, `flz.seq`, and `flz.key`
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

- **WHEN** two consecutive invocations of an Entity emit without applying any
  state change
- **THEN** their `flz.seq` stamps may collide, and the documented guidance directs
  exact append-dedup consumers to the effectively-once outbox capability

### Requirement: Protected `flz.` metadata namespace

User-supplied metadata keys beginning with `flz.` SHALL be rejected at emit time
with a typed error, on all emit variants, so runtime stamps can never be spoofed or
clobbered by operative code.

#### Scenario: Spoofed stamp rejected

- **WHEN** `run` calls `ctx.emit_with_meta(value, {"flz.seq": "999.0"})`
- **THEN** the call returns a typed `ProcedureError` and nothing is emitted

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

### Requirement: Every host mints the identity stamps, and the ordinal rule is one rule

An SDK whose procedures run above the erased host boundary SHALL mint the identity stamps itself, on the output list it hands the core, rather than relying on the core to derive them. The stamps SHALL be written when, and only when, an output is plugin-targeted, its channel is declared keyed, and its class does not declare effectively-once; under the tier the controller derives the identity from the durable effect record at flush, and a host that also stamped would produce a second answer to the same question. The per-invocation emit ordinal SHALL advance on every plugin-targeted output, whether or not that output was stamped, so a class carrying both a keyed and a non-keyed plugin channel yields the same ordinal sequence on every host. The ordinal SHALL be distinct from the ordinal `stable_id` consumes, SHALL start at zero for each work item, and SHALL NOT be reset within one item. `flz.entity` SHALL be spelled `<operative>/<procedure>/<entity_key or empty>`, matching the label the core derives for the same Entity, and `flz.seq` SHALL read the change sequence the activation carried at handout.

An SDK that cannot yet carry the stamps SHALL defer `keyed_channels` by name in its own conformance manifest, naming the follow-up that owes it. The Elixir SDK — not a poll host, inheriting nothing from `flz_core` — mints them in its own core at the routing seam where a sink frame is built: the same three keys, the seq the invocation started at, the ordinal advancing per plugin output of the invocation, appended after the user's metadata; it carries no tier yet, so every keyed emit is stamped, and the tier guard arrives with the change that carries `effectively_once` there.

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
- **THEN** the output the host hands the core carries no `flz.entity`, `flz.seq` or `flz.key`

#### Scenario: An SDK that defers the capability says so in its manifest

- **WHEN** an SDK does not mint the stamps
- **THEN** its `conformance.json` lists `keyed_channels` under `deferred`, and the conformance gate refuses a manifest that carries the capability without the reference's fixture set

#### Scenario: The Elixir SDK mints the same stamps in its own core

- **WHEN** a dynamic Entity on an Elixir runner emits twice on a sink declared `keyed/1` in one invocation that started at change sequence 5
- **THEN** the two frames carry `flz.entity` as `<operative>/<procedure>/<key>`, `flz.seq` `"5.0"` then `"5.1"`, and `flz.key` as the Entity's key, after the user's metadata

