# output-parts Specification

## Purpose
A part: a preview of the output a tasklet's invocation is producing, sent while it runs, what it
carries, how it leaves the runner and the controller, when it is dropped or refused, and that it is
never durable and never reaches a sink or a waiting caller.

## Requirements
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
  then emits `hello` on its sink `answer`
- **WHEN** a listener is attached and the relay is sent one input
- **THEN** the listener SHALL receive the three parts in order, each on channel `answer`,
  and then the output `hello`

#### Scenario: A steps session streams parts within its turn

- **GIVEN** a `per_input` steps relay whose tasklet sends two parts and then emits its
  answer on the sink `answer`
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

### Requirement: A part SHALL never be durable and SHALL never reach a sink

A part SHALL NOT be written into an effect record, a Cell's WAL, an outbox, a task journal,
the input log or the dead-letter store, SHALL NOT be replayed, re-forwarded or re-sent by
any recovery path, SHALL NOT be counted as an output by any counter, and SHALL NOT be
offered to any plugin hook but `on_preview`. A sink plugin SHALL never receive a part. A
part SHALL NOT change the invocation's outputs, state changes, timers, completion or
retry outcome: an invocation that sends parts SHALL produce exactly the outputs and effects
it would produce without them. The part SHALL NOT be the answer: the output is.

#### Scenario: A sink sees only the output

- **GIVEN** a relay whose tasklet sends three parts and emits one output on a sink with
  a file sink plugin attached, and a listener attached beside it
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
nothing when the tasklet's output channel does not end in a sink, with the code
`part.no_sink`: only a sink satisfies a part, and neither a `response()` end
nor a `call` end is
one, since each is a plugin end whose output is taken aside before any listener. It SHALL
also refuse when it is made through a context whose invocation has already returned, with
the code `part.outside_invocation`. Both codes SHALL be of class `invalid` and SHALL be listed in the
error catalogue. A raise from the part call inside `run` SHALL fail the invocation as any
raise does.

#### Scenario: A tasklet with no sink cannot send a part

- **GIVEN** a tasklet whose output channel only feeds another tasklet
- **WHEN** it sends a part
- **THEN** the call SHALL fail with `part.no_sink` naming the tasklet and its channel, and
  no frame SHALL leave the runner

#### Scenario: A tasklet whose channel answers a caller cannot send a part

- **GIVEN** a tasklet whose output channel ends in `response()` or in a `call` end, and no sink
- **WHEN** it sends a part
- **THEN** the call SHALL fail with `part.no_sink`, and no frame SHALL leave the runner

#### Scenario: A context kept past its invocation cannot send a part

- **WHEN** code holds an invocation's context after `run` returned and sends a part
  through it
- **THEN** the call SHALL fail with `part.outside_invocation`

### Requirement: Every SDK SHALL spell the part call beside its emit

The part call SHALL be spelled `ctx.part(value)` in Rust, Java, Node, Python and Ruby,
`$ctx->part($value)` in PHP and `part(ctx, value)` in Elixir, where a `use Flezha.SDK.Tasklet`
module imports it as it imports `emit/2`. Its documentation SHALL sit beside the emit's in
each SDK's reference. The six SDKs hosted on `flz_core` SHALL reach the part through the
core (the Rust SDK through its crate API, the other five through `flz_core_part`), and
SHALL NOT re-express the numbering, the stamps or the drop rules. The Elixir SDK SHALL
implement the same numbering, stamps and drop rules in its own core, and a shared fixture
SHALL pin the frame both produce for the same invocation.

#### Scenario: One fixture, seven SDKs

- **WHEN** the `part_echo` fixture runs under each of the seven runtimes
- **THEN** every runtime SHALL deliver the same parts, with the same `channel`, `flz.part`,
  `flz.part_attempt` and payloads, followed by the same output

### Requirement: The controller SHALL hand a part to its relay's listeners only

A part SHALL travel from the runner as a data frame whose payload id is `output_part` and
whose metadata names the instance under `flz.relay`. The stream channel SHALL route it
by that id through the same per-session route an output frame uses, so a part from a
session that no longer owns the instance is dropped. The instance's Overwatch SHALL run the
`on_preview` hook over it and nothing else: it SHALL NOT enter `on_output`, the outbox, the
held-output list of a binding in flight or any sink. A relay with no subscriber on
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
No part SHALL reach a sink of a controller that does not know parts, unless a user has named
a relay `output_part` on the same runner session, which the older controller cannot
tell from a part's destination.

#### Scenario: A newer runner, an older controller

- **GIVEN** a runner with this change connected to a controller without it
- **WHEN** a tasklet sends a part and then emits
- **THEN** the controller SHALL drop the part, and its sinks SHALL receive the output alone

### Requirement: A caller waiting for an answer SHALL NOT receive parts

A caller waiting for an answer SHALL receive only the answer, never a part: an outside
caller waiting for a relay's answer and a relay waiting
on a call alike. A caller
that wants the preview attaches a listener.

#### Scenario: An outside caller sees the answer alone

- **GIVEN** a relay declaring `input("turn") |> into(Turn) |> sink("events") |> into(Usage) |> response()`,
  whose `Turn` sends parts and emits its answer, and whose `Usage` answers the caller
- **WHEN** an outside caller sends it an input and waits, with a listener attached beside it
- **THEN** the caller SHALL receive `Usage`'s answer only, and the listener SHALL receive
  `Turn`'s parts and then its output, each on channel `events`

