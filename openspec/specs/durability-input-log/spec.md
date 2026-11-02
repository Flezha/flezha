# durability-input-log Specification

## Purpose

The per-relay input log that makes delivery durable. On the tiers that ask for it, every input is
appended to an offset-addressable log before it is forwarded, carries its offset with it, and is
replayed from the retained tail on every bind. The log owns its watermark and keeps records down to
the floor the controller confirms processed, longer for a record carrying a live call or a kept
effect result; appends never wait on a sync, a parked append parks the source and never the host,
and a sender's message id is the one duplicate identity. Across every tier an accepted input is
delivered or refused, never silently dropped, and a frame for an undeclared channel is parked.

## Requirements

### Requirement: Per-relay offset-addressable input-log, gated by the tier

For an instance with durable input, the controller SHALL journal every inbound payload (after
the `:on_input` transform chain) into one durable per-instance input log before forwarding it to
the runner, assigning a strictly monotonic offset. An instance has durable input when it is a
stream on the effectively-once tier or a durable steps relay; there SHALL be one log module, one
file and one offset space for both, keyed by the instance's on-disk state key, and no second
input log of any kind. A flow relay that declares no durability SHALL NOT write an input log and its
ingest path SHALL perform no journaling work.

#### Scenario: Payload journaled before forward

- **WHEN** a input plugin delivers a payload to a flow relay on the tier
- **THEN** the payload is appended to the instance's input log with offset N before the runner
  receives it

#### Scenario: A steps relay's input is journaled to the same log

- **WHEN** a message is sent to a durable steps instance
- **THEN** it is appended to that instance's input log, the same module and file layout a tier
  stream uses, before the runner receives it, and no other durable input file exists for the
  instance

#### Scenario: Tier off means no log

- **WHEN** a flow relay that declares no durability receives payloads
- **THEN** no input log file exists for it and the forward path performs no journaling work

### Requirement: Offset travels with the forwarded input

The assigned offset SHALL be stamped on the forwarded frame as reserved metadata `flz.offset`,
for an instance of either kind, and there SHALL be no other offset stamp. The SDK SHALL thread it
through to the durable record that consumes it: a flow relay's atomic effect record SHALL carry it as
its consumed offset, and a steps relay's iteration commit SHALL carry the offset of the input that
seeded the iteration. A steps relay Cell's own effect record SHALL carry no consumed offset, whether
or not the Cell is the one the message entered through: the iteration is the steps relay's unit of
consumption and its `iter_committed` entry is the instance's one reporter into the log's
watermark, so a Cell that runs rarely never holds the instance's floor. An iteration or an
effect record that consumed no journaled input SHALL carry no offset; `0` is an offset, never
"none".

#### Scenario: Offset round-trips

- **WHEN** input at offset 4711 is dispatched to a flow Cell and the invocation produces an
  effect record
- **THEN** the effect record's consumed offset equals 4711

#### Scenario: Offset round-trips through an iteration commit

- **WHEN** input at offset 0 seeds a durable steps relay's iteration
- **THEN** that iteration's commit carries consumed offset 0, and the journal records 0, not
  "none"

#### Scenario: A steps relay Cell's record carries no offset

- **WHEN** input at offset 4711 seeds a durable steps relay's iteration and the entry tasklet's Cell writes an effect record for it
- **THEN** that record's consumed offset is absent, the Cell reports no watermark row, and the instance's floor moves only when the iteration commits

#### Scenario: A pass that consumed nothing carries no offset

- **WHEN** a chained iteration is seeded from the previous iteration's handoff, or a wake carrying
  no journaled input runs
- **THEN** its commit carries no consumed offset

### Requirement: Crash-safe, replayable log records

Input-log records SHALL be crc-protected appends (same durability class as the
per-Cell WAL, fsync-batched). An append SHALL be answered, and its input
forwarded, only once the batch holding its record has synced, so an input the
runner receives is never one a power loss could take back; every append of one
batch shares that one sync. On restart the controller SHALL be able to read
back any retained offset range in order; a torn tail record SHALL be discarded
without affecting earlier records.

A record that fails its crc32 with complete records behind it SHALL NOT be
discarded as a torn tail. The log SHALL refuse to serve the class, naming the
path, the last offset that verified and the byte offset of the failure, and
SHALL leave the file on disk. The input log opens through the same reader the
per-Cell WAL opens through, so the two SHALL give the same answer to the same
corrupt record; an input the Controller acknowledged is as durable as a change
it persisted, and neither is deleted to make a log parse.

#### Scenario: An append is answered once its record is synced

- **WHEN** an input is appended while the batch sync has not yet run
- **THEN** the append is not answered and the input is not forwarded until that sync completes,
  and a log that stops first answers the appends its last sync made durable

#### Scenario: Replay after crash

- **WHEN** the controller restarts after a crash and a Cell's durable
  consumed-offset is 4700 while the log holds offsets up to 4720
- **THEN** the controller re-forwards offsets 4701..4720 to the restored Cell
  in order

#### Scenario: Torn tail discarded

- **WHEN** the last append was interrupted mid-write
- **THEN** reopening the log yields all complete records and drops only the torn
  tail

#### Scenario: A corrupt record mid-log stops the log instead of truncating it

- **GIVEN** an input log whose second of three records has a flipped bit in its
  body
- **WHEN** the class's log is opened
- **THEN** the third record SHALL still be on disk, the log SHALL refuse to
  serve rather than resume from the first record, and the refusal SHALL be
  reported at error severity naming the class and the offset

### Requirement: Min-watermark retention

The controller SHALL track the instance's floor and SHALL truncate the input log only strictly
below `min(floor, now − hold_window)`. The floor SHALL be the confirmed processed prefix
(Requirement: The instance floor is a processed prefix the controller confirms durable), and
before one is confirmed, the minimum durable consumed offset across the instance's reporters (the
watermark). A flow relay's reporters are its Cells, each reporting from its durable
records (the effect record under the tier, the change record otherwise); a durable steps
relay's reporter is its journal, reporting the consumed offset of each iteration commit and
each released input under the journal's reserved identity, seeded on every bind from the highest
offset the journal released. A reporter that has consumed nothing holds the watermark at the log
start. A truncation SHALL rewrite the file only once the prefix below the floor exceeds the
record-log cut threshold, so a loop's log is rewritten once per threshold rather than once per
turn. A record carrying a live call or a recent `flz.msg_id` holds the truncation lower still
(Requirement: A sender's message id is the one duplicate identity; Requirement: A record carrying
a live call SHALL be kept until its deadline has passed).

`hold_window` SHALL resolve **per instance** as the greater of the user-configured value and
the instance's own declared `correction_window`. It SHALL default to zero for an instance that
declares no correction window and for which the user has configured none, so a
non-correcting instance truncates at its floor exactly as it does with no hold window at all.

#### Scenario: Truncation respects the slowest cell before a marker is confirmed

- **WHEN** no processed prefix is confirmed yet, cell A has durably consumed to offset 4711 and cell B to 4710
- **THEN** truncation removes only offsets below 4710

#### Scenario: An iteration commit advances a steps relay's watermark

- **WHEN** a durable steps relay's iteration N commits with consumed offset 7
- **THEN** the instance's watermark is 7, offsets below 7 become eligible for truncation, and
  the source credit those records held is released

#### Scenario: A rewind does not lower the watermark

- **WHEN** a steps relay whose watermark is 7 is rewound to an earlier iteration
- **THEN** the watermark stays 7 and no released input is re-delivered

#### Scenario: A small released prefix is not rewritten

- **WHEN** iteration commits have released fewer bytes than the record-log cut threshold
- **THEN** the sweep cuts nothing yet, and the file is rewritten once the released prefix exceeds
  the threshold

#### Scenario: A declared correction window holds the log

- **WHEN** a flow relay declares `correction_window(60_000)`, its floor is offset 4711,
  and offset 4711 was appended 10 s ago
- **THEN** offset 4711 is retained, because it is newer than the derived 60 s hold window

#### Scenario: A relay with no correction window has no hold

- **WHEN** an instance declares no correction window, the user configures no hold window,
  and its floor is offset 4711
- **THEN** truncation removes every offset below 4711 immediately, except from the lowest record
  carrying a `flz.msg_id` appended within `call_retention_margin_ms` or a live call on

### Requirement: The log owns its watermark

The input-log process SHALL keep each Cell's durable consumed-offset itself
and derive the relay's min-watermark from that map; a consumed-offset
report SHALL update the log's copy and SHALL NOT scan any shared table. A
burst of reports SHALL fold into one watermark recompute and one credit drain.
The published watermark table SHALL be a rebuildable cache for readers outside
the log — the retention sweep, replay cursors, the CLI — created at controller
boot and owned by the application so it outlives every process that reports
into it, the state supervisor included; a
(re)started log SHALL seed its own map from that table. Offsets are assigned
monotonically, so the bytes a watermark advance frees SHALL be taken as a
prefix of the tracked offsets, costing the number freed rather than the number
still in flight.

A Cell's stored consumed-offset SHALL be monotonically non-decreasing. A
reported advance SHALL take effect only when it is strictly greater than the
stored offset, and SHALL otherwise leave the stored offset unchanged; the
comparison and the write SHALL be atomic with respect to concurrent reporters,
so two reporters racing resolve to the higher offset rather than to whichever
wrote last. A report that does not move the offset SHALL still trigger the
credit drain and unpark, so which reporter arrives last cannot determine
whether waiting appends are released, and SHALL carry the offset now stored
rather than the one reported, so the log's own copy of a Cell's
consumed-offset cannot be taken backwards either.

The single exception SHALL be an explicit rewind, used only after a correction
has rewound a Cell's WAL and the records reporting the higher offset no
longer exist. No other path SHALL lower a stored consumed-offset.

#### Scenario: A restarted log recovers its watermark

- **WHEN** the input-log process restarts while its Cells' last
  consumed-offsets are in the published table
- **THEN** the reopened log computes the same min-watermark as before the
  restart without waiting for a fresh report from every Cell

#### Scenario: The table survives its reporters

- **WHEN** every `WalWriter` that reported a consumed-offset has exited
- **THEN** the published table and its rows are intact, because the
  application owns it

#### Scenario: A burst of advances recomputes once

- **WHEN** N Cells report consumed-offset advances within one scheduling
  window of the log
- **THEN** the log recomputes its min-watermark and drains source credit once
  for the burst, not once per report

#### Scenario: A lower report does not move the watermark back

- **GIVEN** a Cell whose stored consumed-offset is 2
- **WHEN** a consumed-offset of 1 is reported for that Cell
- **THEN** the stored offset SHALL remain 2

#### Scenario: A nested window's advance survives the outer window closing

- **GIVEN** an outbox window whose completion re-enters dispatch and closes a
  nested window that advances the Cell's consumed-offset to 2
- **WHEN** the outer window then reports its own furthest offset of 1
- **THEN** the Cell's stored consumed-offset SHALL be 2, and the input-log
  retention floor and credit accounting SHALL reflect 2

#### Scenario: A rejected report still drains credit

- **GIVEN** a Cell whose stored consumed-offset is 2 and a source parked on
  credit
- **WHEN** a consumed-offset of 1 is reported
- **THEN** the stored offset SHALL remain 2 and the credit drain and unpark
  SHALL still run

#### Scenario: An explicit rewind still lowers the offset

- **GIVEN** a Cell whose stored consumed-offset is 5 and a correction that
  has rewound its WAL to 2
- **WHEN** the rewind path reports 2
- **THEN** the stored offset SHALL become 2, so the next sweep cannot truncate
  the inputs the correction is about to replay

### Requirement: An append never waits on a sync

The log SHALL sync on a companion process holding a second handle on the same
file (a datasync covers the inode, not the handle), driven by the fsync
batcher's tick, so an append is answered while a datasync is in progress.
Appends queued behind one another SHALL be folded into one write in arrival
order, each answered with its own offset; the caller encodes its record before
the call. The record that first overruns source credit SHALL park, and so
SHALL every record queued behind it in that batch, so a later record is never
admitted ahead of an earlier parked one. The synchronous flush (`flush_now`,
shutdown) SHALL round-trip to the companion, and truncation SHALL reopen the
companion's handle on the replacement file.

#### Scenario: An append is answered during a datasync

- **WHEN** the companion is inside a datasync when an append arrives
- **THEN** the append is written and answered without waiting for the sync
  to return

#### Scenario: A burst of appends is one write, in order

- **WHEN** several appends are queued at the log at once
- **THEN** they are written with one write call, assigned consecutive offsets
  in arrival order, and each caller receives its own offset

#### Scenario: A credit overrun parks the whole tail in order

- **WHEN** the second of four queued appends overruns source credit while the
  third is small enough to fit
- **THEN** the first is written and the second, third and fourth park in that
  order; when credit mints, they are admitted in the same order

#### Scenario: Truncation keeps the companion on the live file

- **WHEN** the log truncates below its watermark, replacing the file
- **THEN** the next sync covers the replacement file, not the unlinked one

### Requirement: A truncation costs the tail and never the sweeper

The log SHALL keep a sparse offset-to-position index of its own records (one
sample per fixed number of appends), locate a truncation floor within one
sample's span, and copy only the retained tail into the replacement file, so a
truncation's work is the retained tail rather than the whole log. The
retention sweeper SHALL request a truncation without waiting on the log, and a
truncation whose file has vanished (its class directory removed) SHALL be a
no-op. Records written before a log restart are not indexed; the first
truncation after a restart MAY scan from the start of the file once.

#### Scenario: A large consumed prefix is cut in tail time

- **WHEN** the log holds hundreds of thousands of consumed records below the
  floor and a few thousand above it
- **THEN** the truncation decodes at most one index span to find the floor,
  copies the tail as bytes, and appends resume immediately after

#### Scenario: A busy log cannot crash the sweeper

- **WHEN** the retention sweep requests a truncation of a log that is busy
- **THEN** the sweeper continues with its other work and the log truncates in
  its turn; no call timeout exists to kill the sweeper

#### Scenario: A vanished file is nothing to truncate

- **WHEN** a queued truncation runs after the log's class directory was removed
- **THEN** the log keeps running and cuts nothing

### Requirement: A parked append parks the source, never the host

The host process of an effectively-once relay SHALL NOT wait on the input log: it SHALL hand each append to the log and forward the input when the log answers, keeping forward order equal to offset order and answering each confirming call in call order. While the log parks appends at zero source credit, the host SHALL keep running its output chains, its control calls and its timers; the park SHALL be observable only as the source's owed call. A log that dies with an append in flight SHALL cause that append to be re-issued once and then answered as refused, never left owed.

#### Scenario: Zero credit parks the source's call and nothing else

- **WHEN** the relay's source credit is exhausted and a source's confirming ingest is in flight
- **THEN** the source's call stays unanswered, and the host still answers a control call and runs an `{:io_out}` chain dispatched to it meanwhile

#### Scenario: The host's own confirmations free its parked appends

- **WHEN** the appends of a relay are parked and the output plugin confirms outputs the host's chain delivered while parked
- **THEN** the resulting watermark advance mints credit, the log answers the parked appends in arrival order, and the host forwards the inputs and answers their verdicts in the same order

#### Scenario: Forward order is offset order

- **WHEN** three inputs are ingested in call order a, b, c
- **THEN** they are forwarded to the runner in offset order a, b, c and their verdicts answer in that order

#### Scenario: A log that dies mid-append answers refused

- **WHEN** the input log exits while an append is in flight and the re-issued append finds no log
- **THEN** the confirming call receives `{:error, :refused}` and the input is not forwarded

### Requirement: A dead sync companion stops the log

The log SHALL stop when its sync companion exits, rather than continue with a
companion that syncs nothing. It SHALL first report the death, naming the
relay class and the exit reason, so the loss of durability is observable
rather than inferred from a missing sync. Its termination SHALL datasync and
close its own handle, so the window the companion owed reaches disk, and the
restart that follows SHALL open a fresh handle pair, creating the class
directory if it is absent.

The synchronous flush SHALL answer a failure rather than wait when the
companion cannot answer. The failure is what the fsync batcher already reads as
"this writer did not confirm its sync": the writer stays dirty and is synced on
the next drain, so a lost sync is retried rather than reported durable. The
flush SHALL NOT depend on its caller's timeout to bound it.

A message the log does not route SHALL be reported at error level, naming the
message, and SHALL NOT stop the log.

#### Scenario: A dead companion receives no further sync ticks

- **WHEN** the sync companion exits and the fsync batcher's tick then drives the
  log's class
- **THEN** no sync tick is sent to the exited companion, the death is reported
  naming the relay class and the reason, and the class's log is serving
  again with a live companion

#### Scenario: A synchronous flush against a dead companion is answered

- **WHEN** a synchronous flush reaches the log while its companion is dead and
  the companion's exit has not yet been handled
- **THEN** the flush is answered with a failure, the writer stays dirty for the
  next drain, and the log does not park waiting for an answer that cannot come

#### Scenario: An unrouted message is reported and survived

- **WHEN** a message the log does not route arrives
- **THEN** it is reported at error level naming the message, and the log keeps
  serving

### Requirement: A sender's message id is the one duplicate identity

The log SHALL absorb an append whose metadata carries a non-empty `flz.msg_id` equal to that of
a record it still retains, answering the retained record's offset, and the caller SHALL forward
nothing for it. A message carrying no id SHALL always be appended: two identical payloads without
ids are two messages, for a flow relay and for a steps relay alike. The log SHALL keep the id index
itself, in memory over its retained records, built from the file on the first append that carries
an id, maintained on every such append and trimmed by every truncation; an append SHALL NOT
re-read the file. The record header SHALL keep carrying the append time; the id rides the
metadata the record already holds. The retention sweep SHALL NOT truncate at or above the lowest
record carrying an id whose append time plus `call_retention_margin_ms` is still ahead, whatever
the watermark and the hold window allow, so a sender that never saw its answer can send again
within that margin and be absorbed. Only an id below the sweep's floor SHALL be judged, so a sweep
with none walks nothing; otherwise the sweep SHALL read the append times from the index sample at
or below the lowest such id up to the first record younger than the margin or the floor, so it
reads only what that sweep releases plus one sample span, and an append SHALL read none. A log that
does not answer the sweep's question in time SHALL hold every record for that sweep.

#### Scenario: A re-sent message is absorbed once

- **WHEN** two appends carrying `flz.msg_id` `"m1"` reach the log before the first is truncated
- **THEN** exactly one record exists, the second append answers the first record's offset, and
  the relay receives the message once

#### Scenario: Two identical payloads without an id are two inputs

- **WHEN** the same payload is appended twice with no `flz.msg_id`
- **THEN** two records exist at consecutive offsets and both are forwarded

#### Scenario: The index survives a log restart

- **WHEN** the log process is restarted and an id already retained in the file is appended again
- **THEN** the append answers the retained record's offset from the index rebuilt off the file

#### Scenario: A consumed record keeps its id for the margin

- **WHEN** a record carrying `flz.msg_id` `"m1"` is consumed, the watermark passes it, and its
  append time plus the margin is ahead
- **THEN** the sweep SHALL keep it and an append carrying `"m1"` SHALL answer its offset; once the
  margin has passed, the next sweep SHALL truncate it and `"m1"` SHALL be a new message

#### Scenario: A log that never sees an id builds no index

- **WHEN** every append to a log carries no `flz.msg_id`
- **THEN** the log walks its file for no index and each append costs what it cost before

### Requirement: The retained tail is what every bind re-forwards

On every bind of an instance with durable input, of either kind, the controller SHALL re-forward
the retained records strictly above the instance's floor, in offset order, each stamped with its
original `flz.offset` and never re-journaled, before the instance's input plugins are brought
up. There SHALL be one such re-forward function, shared by a fresh bind, a crash resume, a
migration, a rehydrate from suspension, the bind of a flow relay that could not carry
an earlier input and the rebind a parked relay waits out. A re-forward SHALL be safe to repeat: every receiver, a Cell or a steps
driver, SHALL drop an input at or below its own consumed floor, so a second bind, an orphaned
bind or a repeated tail changes no state and emits nothing twice. An inter-tasklet value a
producer's record kept SHALL be re-delivered only by a re-forward after the Cells were
restored, for origins above the floor and at or below the log's last offset at that restore:
any other moved inside the runner, and a consumer that took it live would take it twice.
Whether an instance owes undelivered input SHALL be answered from the log's tail and floor,
without decoding the file.

#### Scenario: A rehydrated steps relay receives its tail once

- **WHEN** a durable steps instance whose journal has released offset 6 rehydrates while the
  log holds offsets 0..9
- **THEN** offsets 7..9 are forwarded in order, each stamped with its own `flz.offset`, and
  nothing at or below 6 is forwarded

#### Scenario: A tier flow relay receives its tail once

- **WHEN** an effectively-once flow relay whose floor is 4700 rebinds while the log holds
  offsets up to 4720
- **THEN** offsets 4701..4720 are forwarded in order, and a Cell ahead of 4700 skips the offsets
  it already consumed

#### Scenario: A tail forwarded twice is taken once

- **WHEN** the same tail 7..9 reaches a steps driver twice, from an orphaned bind and from the
  Overwatch's own bind
- **THEN** each of 7, 8 and 9 SHALL seed exactly one iteration

#### Scenario: A lost stream on a live session re-delivers no inter-tasklet value

- **WHEN** a flow relay's data stream closes under a live session and its rebind re-forwards
  the tail, with no restore since the bind
- **THEN** the tail's inputs are forwarded, no kept inter-tasklet value is delivered again, and
  each consumer counts each acknowledged input once

#### Scenario: Owed input is judged without a file read

- **WHEN** the controller asks whether a suspended instance owes undelivered input
- **THEN** the answer is derived from the log's last offset and the instance's floor, and no
  record body is decoded

### Requirement: The instance floor is a processed prefix the controller confirms durable

After each drain pass the runner SHALL send, for every instance that took new input, a marker
naming offset `P`: every input at or below `P` has been processed and every record it produced,
in any Cell of the instance including a downstream Cell fed by an edge, has been written to the
data stream ahead of the marker. An input still queued, in flight, held back, retrying or
unflushed in any Cell SHALL hold `P` below it; a Cell with nothing pending SHALL NOT hold `P`.
The marker SHALL travel on the data stream behind the records it covers and SHALL never be held
in the reconnect ring: a marker the wire refuses is dropped, and a runner that evicted a record
of an instance from the ring SHALL send no marker covering that record's input.

The controller SHALL confirm `P` only once every Cell writer of the instance has reported an
fsync drain past the stream position it had reached when the marker arrived. A confirmed `P`
SHALL be the instance's floor for retention and for where a bind's re-forward starts. Before any
`P` is confirmed, and after a controller restart, the floor SHALL be the minimum durable
consumed offset over the instance's reporters.

#### Scenario: An idle Cell does not hold the floor

- **WHEN** a stream keyed by symbol takes offset 1 for `BTC-USD` and offsets 2..1005 for `SOL-USD`, every record is fsynced, and no `BTC-USD` input follows
- **THEN** the confirmed floor SHALL reach 1005 and retention MAY release every offset at or below 1005

#### Scenario: A marker is not confirmed before its records are durable

- **WHEN** the controller receives a marker for `P = 5` while the record for offset 4 is received but not yet fsynced, and the controller is killed before that fsync
- **THEN** the floor SHALL NOT have reached 5, offset 4 SHALL still be in the input log after the restart, and the rebind SHALL re-forward it

#### Scenario: Work pending downstream holds the floor

- **WHEN** offset 7 has been folded by an entry Cell whose edge value is still unflushed in a downstream Cell
- **THEN** no marker SHALL name a `P` at or above 7 until the downstream record is on the stream

#### Scenario: A record lost from the reconnect ring is never covered

- **WHEN** the reconnect ring evicts the record for offset 4 of an instance and later records of that instance are replayed after the reconnect
- **THEN** the runner SHALL send no marker with `P` at or above 4 for that instance

#### Scenario: A controller restart falls back safely

- **WHEN** the controller restarts with a confirmed floor of 1005 and Cell offsets of 1 and 1005
- **THEN** until the next confirmed marker the floor SHALL be 1, and a rebind SHALL re-forward from 2, every Cell skipping what it already consumed

### Requirement: A record carrying a live call SHALL be kept until its deadline has passed

The log SHALL learn, with its message-id index and at no extra read of the file, the
`flz.deadline_at` of every record that carries one: a call's request in the callee's log and a
call's `Response` in the caller's. The retention sweep SHALL NOT truncate at or above the lowest
such record whose `flz.deadline_at` plus `call_retention_margin_ms` is still ahead, whatever the
watermark and the hold window allow. A record whose deadline plus the margin has passed SHALL be
retained by the watermark, the hold window and its message id's margin alone, as any record is. A
log whose records carry neither a deadline nor a message id SHALL truncate exactly as it did
before.

#### Scenario: A consumed request is kept until its call is over

- **WHEN** a callee consumes a call's request, its watermark passes the request's offset, and the call's deadline plus the margin is ahead
- **THEN** the sweep SHALL keep the request, so a request sent again for that call is absorbed by its message id

#### Scenario: A finished call's records go with the watermark

- **WHEN** a call's deadline plus the margin and its records' append times plus the margin have passed, and the watermark is above its records
- **THEN** the next sweep SHALL truncate them like any consumed record

#### Scenario: A log without calls is unchanged

- **WHEN** no record of a log carries `flz.deadline_at` or `flz.msg_id`
- **THEN** the sweep SHALL truncate below `min(watermark, now - hold_window)` as before

### Requirement: The log's server SHALL keep effect results under the input's retention

The input log's server SHALL own a second file, `effects.log`, beside `input.log`, holding
effect results as `{offset, identity, payload}` in the `WalWriter` framing. It SHALL answer
an effect result's append once a sync covers it, synced by the sync companion it already
has through a descriptor of its own, so an input append never waits on it, and at
`durable: remote` once the durability tier has also
shipped it. It SHALL keep the first payload for an identity and answer a later append for
that identity with the first. A truncation below a floor SHALL drop the effect results of
every offset below it, and rewrite `effects.log` under the same cut threshold as `input.log`.
On start it SHALL read the retained effect results back. The durability tier SHALL ship and
hydrate `effects.log` with the input log's artifacts.

#### Scenario: Results follow the floor

- **WHEN** effect results are kept for offsets 4 and 7 and the floor moves to 5
- **THEN** after the truncation the log keeps only the results of offset 7

#### Scenario: Results survive losing the disk under the tier

- **WHEN** the durability backend is on, an effect result is kept, and the controller is
  replaced with an empty disk
- **THEN** the hydrated instance's re-run invocation is served the kept result

#### Scenario: The input path is unchanged

- **WHEN** an instance never calls `effect`
- **THEN** no `effects.log` is created and the input log's appends, reads and truncation
  behave exactly as before

### Requirement: An accepted input SHALL be delivered or refused, never silently dropped

An input the Controller reports as accepted SHALL either reach the relay's
tasklet or be refused to the sender. It SHALL NOT be possible for the
Controller to answer `:ok`, record the input as accepted, and then deliver it to
nothing.

An unbound data stream SHALL NOT by itself make an input undeliverable. Where
the Controller journals the input, the bind SHALL deliver it; where it does not
journal the input, the Controller SHALL NOT accept the input while the stream is
unbound.

A relay SHALL NOT be brought up on a data stream that has not bound: its
bind SHALL wait for the stream, so nothing issued by the bind — a durable
restore's re-forward included — is sent onto a stream that would drop it. A
relay whose stream does not bind SHALL be parked rather than run.

#### Scenario: A journaled input arriving before the stream binds is delivered

- **GIVEN** a relay of a journaled class whose data stream has not yet bound
- **WHEN** an input is sent and the Controller reports it accepted
- **THEN** the tasklet SHALL receive that input once the stream binds

#### Scenario: An un-journaled input is not accepted while the stream is unbound

- **GIVEN** a relay of a class the Controller does not journal, whose data stream has not yet bound
- **WHEN** an input is sent
- **THEN** the Controller SHALL NOT report the input accepted

#### Scenario: The journal cannot claim an input the system dropped

- **GIVEN** an input written to the journal during an unbound-stream window
- **WHEN** the relay's stream binds and the Cell's replay range is computed
- **THEN** that input SHALL fall inside the range and be forwarded

### Requirement: A data-stream bind SHALL re-forward the journal's undelivered tail

A journaled input that an unbound data stream could not carry SHALL be forwarded
by that stream's next bind, in offset order, using the same replay path a
restore uses. This applies to relays of a journaled class. A bind SHALL NOT
introduce a second re-forward mechanism beside the restore's.

The re-forward SHALL be bounded by the Cell's consumed offset, so an input the
Cell has already consumed is not re-delivered, and SHALL be idempotent across
repeated binds: a flow relay that binds, drops and rebinds without the Cell making
progress SHALL forward the same tail rather than an accumulating one.

A bind that owes nothing — every input so far having been delivered — SHALL
forward nothing, so an ordinary runner reconnect does not re-deliver.

#### Scenario: The bind forwards what the Cell has not consumed

- **GIVEN** a Cell whose consumed offset is 3 and whose journal holds offsets up to 6
- **WHEN** offsets 4, 5 and 6 could not be delivered and the relay's data stream then binds
- **THEN** offsets 4, 5 and 6 SHALL be forwarded in order, and offsets at or below 3 SHALL NOT

#### Scenario: A flapping stream does not multiply deliveries

- **GIVEN** a Cell with an undelivered journal tail whose stream binds, drops and rebinds three times without the Cell consuming anything
- **THEN** the number of distinct inputs the tasklet receives SHALL equal the size of the tail

#### Scenario: The bind uses the restore's replay path

- **WHEN** the bind re-forward is implemented
- **THEN** it SHALL call the same replay-slice function the restore path calls, rather than a second forwarding implementation

### Requirement: The unbound-stream drop counter SHALL mean a genuine fault

`[:flezha, :stream, :drop]` and the `nil_drops` counter SHALL NOT fire during
ordinary relay startup or rebind. A drop SHALL indicate an input that could
be neither delivered nor accounted for, so a non-zero count is actionable.

#### Scenario: A clean run drops nothing

- **WHEN** the end-to-end suite runs to completion with the drop probe attached
- **THEN** no `[:flezha, :stream, :drop]` event SHALL be emitted on a start or rebind path

### Requirement: The test suite SHALL NOT compensate for undelivered input

No test helper SHALL exist whose purpose is to wait for a data stream to bind
before sending, and no test SHALL re-send an input until it is forwarded. The
suite SHALL fail on a delivery regression rather than settling around it.

#### Scenario: The bind settle helper is gone

- **WHEN** the test tree is searched for a helper that polls stream-bind status before sending
- **THEN** there SHALL be none, and the end-to-end suite SHALL be green without one

#### Scenario: No test re-sends until forwarded

- **WHEN** the test tree is searched for a loop that re-sends an input until it is observed forwarded
- **THEN** there SHALL be none

### Requirement: An entry frame naming an undeclared channel SHALL be parked, never dropped or rerouted

A frame whose metadata names an entry channel SHALL be delivered to that channel, or parked when the
relay declares no ENTRY channel of that name. A name the relay declares as a channel of
another kind — a output plugin, an inter-tasklet channel — is not an entry channel and SHALL be
parked the same way, because channel names are one namespace across every kind. The frame SHALL NOT
be discarded, and it SHALL NOT be delivered to a different channel. Every SDK SHALL answer the same
way: every SDK is hosted on `flz_core` and answer through the core's one resolution.

The park SHALL record the requested name in the entry's channel field and nowhere else. A name off
the wire SHALL NOT reach the identity the dead-letter store keys its row by, because the store makes
that identity's tasklet component a directory; the SDK SHALL supply a fixed component of its own
for a park that resolved no channel.

The park SHALL be immediate rather than laddered. The name is a fact of the frame, so a re-attempt
resolves against the same declared set and fails identically; walking the channel's retry budget
would delay the report without changing it.

A frame carrying no channel in its metadata SHALL continue to resolve to the first declared entry
channel. That is the default a input plugin relies on and it is not an unresolved name.

A relay that declares no entry channel at all SHALL park such a frame with a reason naming that
condition, rather than reporting an unresolved name against an empty set.

Until the controller acks it, a park SHALL be held by the runner as its payload, not as a thread
per frame. The runner SHALL hold a bounded number of unacked parks; past the bound, the thread
handing over the next park, a frame's reader or a Cell's worker whose output could not be delivered,
SHALL wait for an ack, counted and logged once per wait, and the park SHALL NOT be dropped.

#### Scenario: A named channel the relay does not declare is parked

- **GIVEN** a relay declaring one entry channel `prices`
- **WHEN** a frame arrives whose metadata carries `channel = "not_prices"`
- **THEN** the frame SHALL be parked in the dead-letter store with its payload bytes, and SHALL NOT reach any tasklet

#### Scenario: A misspelled channel at rate holds no thread per frame

- **GIVEN** a relay declaring one entry channel `prices` and a controller that has not acked any park
- **WHEN** 2 000 frames arrive naming `pricse`
- **THEN** every frame SHALL be shipped as a park, none SHALL hold a thread of its own, and a frame arriving while the runner holds its bound of unacked parks SHALL wait for an ack rather than be dropped

#### Scenario: A frame with no channel still reaches the first entry channel

- **GIVEN** a relay declaring one entry channel `prices`
- **WHEN** a frame arrives whose metadata carries no `channel` key
- **THEN** the frame SHALL be delivered to `prices`

#### Scenario: The default is the first declared entry channel, whatever its name sorts as

- **GIVEN** a relay declaring entry channels `volumes` and then `prices`, in that order
- **WHEN** a frame arrives whose metadata carries no `channel` key
- **THEN** the frame SHALL be delivered to `volumes`, on every SDK

#### Scenario: A name the relay declares as another kind of channel is parked

- **GIVEN** a relay declaring one entry channel `prices` and one output plugin `out`
- **WHEN** a frame arrives whose metadata carries `channel = "out"`
- **THEN** the frame SHALL be parked under the name `out`, and SHALL NOT reach any tasklet

#### Scenario: A requested name never becomes a path component

- **GIVEN** a relay declaring one entry channel `prices`
- **WHEN** a frame arrives whose metadata carries a `channel` naming a parent directory
- **THEN** the parked entry SHALL record that name as its channel, and the identity it is stored under SHALL carry the SDK's own fixed component instead

#### Scenario: The park is immediate

- **GIVEN** a relay whose entry channel declares a retry budget above one
- **WHEN** a frame naming an undeclared channel arrives
- **THEN** the parked entry SHALL record one attempt, and no retry delay SHALL be waited

#### Scenario: A relay with no entry channel at all

- **GIVEN** a relay declaring no entry channel
- **WHEN** any frame arrives for it
- **THEN** the frame SHALL be parked with a reason naming that the relay declares no entry channel
