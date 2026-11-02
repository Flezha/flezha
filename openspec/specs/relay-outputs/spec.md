# relay-outputs Specification

## Purpose

What a relay's outputs carry beyond their value. A channel can opt into replay-stable identity with
`.keyed()` and `emit_keyed`: its frames carry the `flz.cell`, `flz.seq` and `flz.key` stamps the
core mints for every host, the `flz.` metadata namespace is protected, and non-keyed channels stay
unchanged. A tasklet can also send parts while it runs: previews of the output it is producing that
are never durable, are dropped rather than waited on when they cannot leave, reach only the relay's
listeners, and never reach an output or a waiting caller.

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

### Requirement: A tasklet SHALL be able to send a part while it runs

Every SDK SHALL give a tasklet's invocation context one call that sends a **part**: a
preview of the output the invocation is producing, which leaves the runner when the call is
made rather than when the invocation returns. The call SHALL be available to a tasklet of a
flow relay and of a steps relay, in `run` and in a timer handler, and SHALL be
callable any number of times, from any point of the invocation, including between two reads
of a long blocking call made inside `run`. The value SHALL be encoded exactly as an emit of
the same value on the tasklet's output channel would be. The call SHALL return without
waiting for the controller, for a listener or for any acknowledgement. A part SHALL preview
the tasklet's own output channel, the one channel a tasklet node owns; it SHALL take no
channel argument.

#### Scenario: A flow tasklet streams parts before its output

- **GIVEN** a flow relay whose tasklet sends the parts `he`, `hel` and `hell` and
  then emits `hello` on its output `answer`
- **WHEN** a listener is attached and the relay is sent one input
- **THEN** the listener SHALL receive the three parts in order, each on channel `answer`,
  and then the output `hello`

#### Scenario: A steps session streams parts within its turn

- **GIVEN** a `per_input` steps relay whose tasklet sends two parts and then emits its
  answer on the output `answer`
- **WHEN** a listener is attached and the instance is sent one message
- **THEN** the listener SHALL receive both parts before the answer, and the answer SHALL
  leave at the stage's commit as it does today

#### Scenario: A part leaves before the invocation returns

- **GIVEN** a tasklet that sends a part and then blocks for two seconds before it emits
- **WHEN** it runs under a listener
- **THEN** the part SHALL reach the listener before the two seconds have passed

#### Scenario: A part leaves before the effect record under the tier

- **GIVEN** a flow relay on the effectively-once tier whose tasklet sends a part
  and then emits
- **WHEN** the input is processed under a listener
- **THEN** the part SHALL reach the listener before the effect record is durable, and the
  output SHALL reach it only after, as today

### Requirement: A part SHALL carry its channel, its place in the attempt and its cell

A part SHALL carry, beside its payload, the metadata `channel` (the tasklet's output
channel), `flz.part` (its sequence number within the attempt, counted from `0` and
increasing by one per part the attempt sends), `flz.part_attempt` (the invocation's attempt
number as the tasklet's retry ladder counts it, from `1`), `flz.cell` (the Cell's label, the
same string a keyed output's `flz.cell` carries) and, when the Cell has a key, `flz.key`.
The sequence number SHALL be counted per invocation attempt, so a retried invocation's parts
SHALL start again from `0` with the next attempt number. An input the controller forwards
again after a crash or a rebind SHALL be a new invocation whose parts start from `0` with
attempt `1`. A listener SHALL treat a part with `flz.part` `0` as the start of a new
preview for its channel and Cell. No user code SHALL be able to set a `flz.` key on a part.

#### Scenario: Parts are numbered within their attempt

- **WHEN** an invocation sends three parts
- **THEN** they SHALL carry `flz.part` `0`, `1` and `2` and `flz.part_attempt` `1`

#### Scenario: A retry restarts the preview

- **GIVEN** a tasklet that sends two parts and then fails retryably on its first attempt
- **WHEN** its second attempt sends two parts and succeeds
- **THEN** the listener SHALL receive `flz.part` `0` and `1` with attempt `1`, then `0` and
  `1` with attempt `2`, then the output

#### Scenario: A keyed Cell's part names its Cell and key

- **WHEN** a tasklet keyed by `session` sends a part from the Cell whose key is `s-7`
- **THEN** the part SHALL carry `flz.key` `s-7` and a `flz.cell` naming the relay, the
  tasklet and `s-7`

### Requirement: A part SHALL never be durable and SHALL never reach an output

A part SHALL NOT be written into an effect record, a Cell's WAL, an outbox, a task journal,
the input log or the dead-letter store, SHALL NOT be replayed, re-forwarded or re-sent by
any recovery path, SHALL NOT be counted as an output by any counter, and SHALL NOT be
offered to any plugin hook but `on_preview`. A output plugin SHALL never receive a part. A
part SHALL NOT change the invocation's outputs, state changes, timers, completion or
retry outcome: an invocation that sends parts SHALL produce exactly the outputs and effects
it would produce without them. The part SHALL NOT be the answer: the output is.

#### Scenario: An output plugin sees only the output

- **GIVEN** a relay whose tasklet sends three parts and emits one output on a channel with
  a file output plugin attached, and a listener attached beside it
- **WHEN** one input is processed
- **THEN** the file SHALL hold exactly the one output, and the listener SHALL have received
  three parts and one output

#### Scenario: A crash replays no part

- **GIVEN** an effectively-once relay whose tasklet sent two parts before the runner was
  killed mid-invocation
- **WHEN** the relay is rebound and the input is forwarded again
- **THEN** no part of the first incarnation SHALL be sent again by any controller or runner
  path, and the parts the listener receives after the rebind SHALL be the ones the new
  invocation sends, starting at `flz.part` `0`

#### Scenario: A steps session's parts leave nothing in its journal

- **WHEN** a `per_input` steps session's turn sends parts and commits
- **THEN** its task journal, its Cells' WAL and its outbox SHALL hold exactly what the same
  turn without parts would have written

### Requirement: A part SHALL be dropped, never waited on, when it cannot leave at once

A part that cannot leave the runner at once SHALL be dropped and counted, and the call
SHALL return normally: when no data stream is bound, when the stream refuses the frame or
could not take it without waiting, and when the frame would exceed the wire frame limit. A
part SHALL NOT be held for a reconnect or replayed onto a new stream. A part sent by an
invocation of a replayed input (an input carrying `flz.replay`, a correction's replay slice)
SHALL send nothing, as a spawn request from such an invocation sends nothing. A local run
with no controller SHALL drop every part. The runner SHALL count every dropped part by
reason, and the count SHALL be readable where the runner's other liveness counters are.

#### Scenario: No stream, no part, no wait

- **GIVEN** a runner whose data stream to the controller is down
- **WHEN** a tasklet sends a part
- **THEN** the call SHALL return at once, the part SHALL be counted dropped, and no part
  SHALL be sent when the stream comes back

#### Scenario: A replayed input sends no part

- **WHEN** a correction replay re-runs an input whose tasklet sends parts
- **THEN** no part frame SHALL leave the runner for that invocation

#### Scenario: A part over the frame limit is dropped

- **WHEN** a tasklet sends a part whose encoded frame exceeds the wire frame limit
- **THEN** the call SHALL return normally, the part SHALL be counted dropped, and the
  invocation SHALL go on

### Requirement: A part SHALL be refused when it has nowhere to preview or no invocation

The part call SHALL raise the SDK's one error type (in Elixir, answer the error) and send
nothing when the tasklet's output channel does not end in an output, with the code
`part.no_output`: only an output satisfies a part, and a `call` end
is not one, since its output is a request taken aside
before any listener. It SHALL also refuse when it is made through a context whose invocation has
already returned, with the code `part.outside_invocation`. Both codes SHALL be of class `invalid`
and SHALL be listed in the error catalogue. A raise from the part call inside `run` SHALL fail the
invocation as any raise does.

#### Scenario: A tasklet with no output cannot send a part

- **GIVEN** a tasklet whose output channel only feeds another tasklet
- **WHEN** it sends a part
- **THEN** the call SHALL fail with `part.no_output` naming the tasklet and its channel, and
  no frame SHALL leave the runner

#### Scenario: A tasklet whose channel calls another relay cannot send a part

- **GIVEN** a tasklet whose output channel ends in a `call` end, and no output
- **WHEN** it sends a part
- **THEN** the call SHALL fail with `part.no_output`, and no frame SHALL leave the runner

#### Scenario: A context kept past its invocation cannot send a part

- **WHEN** code holds an invocation's context after `run` returned and sends a part
  through it
- **THEN** the call SHALL fail with `part.outside_invocation`

### Requirement: Every SDK SHALL spell the part call beside its emit

The part call SHALL be spelled `ctx.part(value)` in Rust, Java, Node, Python and Ruby,
`$ctx->part($value)` in PHP, `ctx.Part(value)` in Go and `part(ctx, value)` in Elixir,
where a `use Flezha.SDK.Tasklet` module imports it as it imports `emit/2`. Its documentation SHALL sit beside the emit's in
each SDK's reference. Every SDK SHALL reach the part through the
core (the Rust SDK through its crate API, the others through `flz_core_part`), and
SHALL NOT re-express the numbering, the stamps or the drop rules.

#### Scenario: One fixture, every SDK

- **WHEN** the `part_echo` fixture runs under each runtime
- **THEN** every runtime SHALL deliver the same parts, with the same `channel`, `flz.part`,
  `flz.part_attempt` and payloads, followed by the same output

### Requirement: The controller SHALL hand a part to its relay's listeners only

A part SHALL travel from the runner as a data frame whose payload id is `output_part` and
whose metadata names the instance under `flz.relay`. The stream channel SHALL route it
by that id through the same per-session route an output frame uses, so a part from a
session that no longer owns the instance is dropped. The instance's Overwatch SHALL run the
`on_preview` hook over it and nothing else: it SHALL NOT enter `on_output`, the outbox, the
held-output list of a binding in flight or any output. A relay with no subscriber on
`on_preview` SHALL drop a part at the hook's active-bit check. A part SHALL be handed to
each listener without making the Overwatch wait on that listener or on the distribution
link to it.

#### Scenario: No listener, no cost

- **WHEN** a part reaches a relay with no listener
- **THEN** the Overwatch SHALL drop it without building a hook context, and no plugin SHALL
  be called

#### Scenario: A superseded session's part is dropped

- **GIVEN** an instance that has been rebound to a new runner session
- **WHEN** a part frame from the old session arrives
- **THEN** it SHALL be dropped at the stream channel and no listener SHALL receive it

#### Scenario: A part is never held behind a binding

- **WHEN** a part reaches an Overwatch whose wake or rebind is binding
- **THEN** the part SHALL go to the listeners it has at that moment or be dropped, and
  SHALL NOT join the outputs held for that binding

### Requirement: A controller that does not know parts SHALL drop them

Because a part travels under its own payload id, a controller without this change SHALL
read the frame as addressed to an instance named `output_part`, find no route and drop it.
No part SHALL reach an output of a controller that does not know parts, unless a user has named
a relay `output_part` on the same runner session, which the older controller cannot
tell from a part's destination.

#### Scenario: A newer runner, an older controller

- **GIVEN** a runner with this change connected to a controller without it
- **WHEN** a tasklet sends a part and then emits
- **THEN** the controller SHALL drop the part, and its outputs SHALL receive the output alone

### Requirement: A caller waiting for an answer SHALL NOT receive parts

A caller waiting for an answer SHALL receive only the answer, never a part: an outside caller
waiting for a relay's answer and a relay waiting on a call alike. A part SHALL NOT answer a
call, even when it is sent on the channel whose output answers. A caller that wants the preview
attaches a listener.

#### Scenario: An outside caller sees the answer alone

- **GIVEN** a relay declaring `input("turn") |> into(Turn) |> output("events")`,
  whose `Turn` sends parts and then emits its answer
- **WHEN** an outside caller sends it an input and waits, with a listener attached beside it
- **THEN** the caller SHALL receive `Turn`'s output only, and the listener SHALL receive
  `Turn`'s parts and then its output, each on channel `events`
