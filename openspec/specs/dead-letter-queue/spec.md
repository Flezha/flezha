# dead-letter-queue Specification

## Purpose

The dead-letter-queue capability guarantees that a permanently-failing (poison) payload in a Flow relay is never silently dropped and never freezes the relay's progress. A failing tasklet invocation — returned error or panic/unhandled exception, one policy in both SDKs — is retried up to a per-channel budget (default 3 total attempts, exponential backoff + jitter; author-classified non-retryable errors park immediately). After exhaustion the payload is durably parked to the controller's DLQ store (`storage/app/dlq/<relay_class>/<tasklet_id>/`, JSONL metadata index + per-entry payload blobs) strictly BEFORE the cell advances: tier-off sessions hold the cell until the fsynced append is acknowledged (`park_payload` data-stream frame + `ParkPayloadAck` control command), while effectively-once sessions record the park and the consumed-offset advance in one atomic WAL record so a crash can never advance past a lost park — parking un-freezes the min-watermark, keeping input-log retention and source credit moving. Users inspect, replay (explicit-only, through the ordinary input path), or drop entries via `flz dlq`; retention is bounded by age and total size with loud telemetry per deletion. A steps relay's escape is AbortAll + saga compensation, with two exceptions that park: a message its entry channel cannot read, and a failed turn of a `per_input` loop declaring keep-progress, whose message the controller parks under the `_turn` slot before the turn is undone and the loop takes its next message.
## Requirements
### Requirement: Failing tasklet invocations are retried with backoff before parking

A Flow tasklet invocation that fails — by returned error or by panic/unhandled exception — SHALL be retried up to a per-channel maximum (default 3 attempts) with exponential backoff and jitter. Authors SHALL be able to classify errors as non-retryable, which parks immediately without further attempts. A panic is retryable by default. The Rust and Java SDK dispatchers SHALL implement identical retry semantics, and the current catch-log-continue path SHALL be removed.

#### Scenario: Transient failure recovers within the retry budget

- **WHEN** a tasklet invocation fails twice and succeeds on the third attempt
- **THEN** processing continues normally, no payload is parked, and attempt telemetry records the retries

#### Scenario: Non-retryable error parks immediately

- **WHEN** a tasklet returns an error the author classified non-retryable
- **THEN** the payload is parked without further attempts

#### Scenario: Panic follows the same policy

- **WHEN** a tasklet panics on every attempt until the retry budget is exhausted
- **THEN** the payload is parked exactly as a returned error would be, and the cell continues with the next payload — never a silent drop

### Requirement: A poison payload is durably parked before the cell advances

After the retry budget is exhausted the SDK SHALL ship a park frame (payload bytes, channel, cell key, origin offset where the effectively-once tier provides one, error context, attempt count) on the data stream, and SHALL NOT advance past the payload until the controller acknowledges the park as durable. Tier off: the controller appends the entry to the DLQ store and fsyncs before acking. Tier on: the park entry and the consumed-offset advance SHALL be recorded in one atomic WAL record (no state change, DLQ entry, consumed offset) so a crash can never both advance the offset and lose the parked payload; the cell's consumed offset advances past the poison payload so the relay's min-watermark can move.

#### Scenario: Park is durable before advance

- **WHEN** the controller crashes after acking a park but before the cell processes the next payload
- **THEN** on recovery the DLQ entry exists and the cell resumes after the parked payload — the payload is in exactly one place

#### Scenario: Crash between exhaustion and ack does not lose the payload

- **WHEN** the runner crashes after exhausting retries but before the park is acked
- **THEN** recovery replays the cell from its consumed offset, the poison payload is retried and parked again, and no duplicate DLQ entry survives (the park is idempotent on origin identity)

#### Scenario: Parking un-freezes the min-watermark (tier on)

- **WHEN** a tier-on cell's poison payload froze the relay's min-watermark
- **THEN** after the park the cell's consumed offset advances past it, the min-watermark resumes, input-log retention proceeds, and a parked source-credit loop resumes granting

### Requirement: DLQ storage is per-tasklet, durable, and steps relay

The controller SHALL store parked payloads under `storage/app/dlq/<relay_class>/<tasklet_id>/` as a JSONL metadata index (entry id, cell key, channel, origin offset, error, attempts, replay generation, timestamp) with payload bytes in adjacent per-entry blob files. Retention SHALL be bounded by age and total size (defaults 30 days / 4 GiB, more restrictive wins); deleting an entry by retention SHALL emit a dedicated loud telemetry event, since it discards already-parked data. Parking and retention SHALL be observable via counters (`dlq_parked_total`, `dlq_dropped_by_retention_total`, per-tasklet entry count and bytes).

#### Scenario: Parked entry is inspectable after restart

- **WHEN** a payload is parked and the controller restarts
- **THEN** the entry and its payload bytes are still present and listable

#### Scenario: Retention deletion is loud

- **WHEN** the DLQ exceeds its size cap and the oldest entry is deleted
- **THEN** `dlq_dropped_by_retention_total` increments and a dedicated telemetry event names the deleted entry

### Requirement: CLI surface for inspect, replay, and drop

The CLI SHALL provide `flz dlq list <relay>` (entries with cell, channel, error, age — nearest-to-expiry first), `flz dlq show <entry>` (metadata + payload preview), `flz dlq replay <entry>` (re-dispatch through the normal input path; tier on mints fresh output identity by construction — the replay is journaled as a new input, so its effect records carry fresh WAL seqs; a failing replay re-parks with an incremented generation), and `flz dlq drop <entry>` (delete entry + blob, emitting an audit event). Replay SHALL be explicit-only — the system SHALL NOT automatically re-drive parked payloads.

#### Scenario: Replay re-enters the normal path

- **WHEN** a user runs `flz dlq replay` on a parked entry after fixing the underlying bug
- **THEN** the payload is dispatched to its cell through the ordinary path, processes successfully, and the DLQ entry is removed

#### Scenario: Failing replay re-parks with generation

- **WHEN** a replayed payload exhausts its retry budget again
- **THEN** it is parked again with `replay generation` incremented, not dropped

#### Scenario: Drop is audited

- **WHEN** a user runs `flz dlq drop`
- **THEN** the entry and payload are deleted and an audit event records the drop with the entry's identity

### Requirement: A parked entry SHALL be built by one constructor

Every writer that parks a payload SHALL build the entry through one constructor
in the DLQ directory — the runner-side settlement, the outbox, the records path
and the relay-forward provider alike — that validates the required fields
and answers the record the store appends; no writer SHALL hand the store a map it
built by hand. The entry's on-disk keys and directory layout SHALL be unchanged
by the constructor, and the runner's wire frame SHALL remain the wire form of
that entry rather than a further copy.

#### Scenario: Every append site passes the constructed entry

- **WHEN** the tree is searched for appends to the DLQ store
- **THEN** every site SHALL pass the constructed entry, and a site that passes a
  bare map SHALL fail the DLQ's own test naming the file

#### Scenario: A missing required field is refused before the write

- **WHEN** a writer constructs an entry without the cell key, the channel or
  the error
- **THEN** the constructor SHALL answer `{:error, reason}` naming the field and
  no partial row SHALL be written

#### Scenario: Existing parked entries are still listed

- **WHEN** entries parked before this constructor existed are listed and shown
- **THEN** they SHALL be read back unchanged, under the same directories, by the
  same verbs

### Requirement: A park with no resolved channel SHALL be attributed to the name that was requested

A parked entry SHALL carry a channel attribution even when no declared channel resolved, and that
attribution SHALL be the name the frame requested rather than a blank, a placeholder, or the name of
some channel the relay happens to declare. The name the user typed is the one fact that
makes the row actionable, so it is the one the row records.

The entry's error SHALL name the declared entry channels of the relay, so the row answers the
question it raises. A relay declaring none SHALL say so rather than printing an empty set.

#### Scenario: The row names the requested channel

- **GIVEN** a relay declaring one entry channel `prices`
- **WHEN** a frame naming `not_prices` is parked
- **THEN** `flz dlq list` SHALL show the entry with channel `not_prices`

#### Scenario: The error names the declared set

- **GIVEN** a relay declaring entry channels `prices` and `config`
- **WHEN** a frame naming `quotes` is parked
- **THEN** the entry's error SHALL name both `prices` and `config`

#### Scenario: A replayed entry is re-routed by the corrected name

- **GIVEN** a parked entry recorded against the requested channel `not_prices`
- **WHEN** the user replays it after the relay is redeclared with a channel of that name
- **THEN** the payload SHALL reach that channel

### Requirement: A steps relay SHALL dead-letter only an unreadable message and a failed per-input turn

A steps relay SHALL write to the dead-letter queue in two cases only: a message its
entry channel cannot read, which is parked without a pass under the entry channel's slot as
today, and a failed turn of a `per_input` loop declaring keep-progress (see
`steps-durable-loop`). In every other case a permanently-failing cell SHALL continue to
fail its tasklet (AbortAll) and trigger saga compensation, the profile's existing escape.
Documentation SHALL state both cases and this equivalence, and that configurable
`on_cell_failure` policies (DeadLetter | MinSuccess) remain deferred.

A failed turn's entry SHALL be filed under the relay's class and the slot `_turn`, with
the cell key `_turn`, the turn's entry channel as its channel, the input-log offset the
turn consumed as its origin offset, the failing tasklet and its error text as its error,
and the tasklet's attempts. Its payload SHALL be the turn's message bytes as the input log
recorded them, which the controller SHALL read from the instance's input log before the
append (the offset is still above the instance's floor then, so the record is retained). A
class that keeps no input log SHALL park the entry with no payload. The entry's identity
SHALL be the store's existing origin identity, `_turn@<offset>`, so a turn that fails again
after a crash lands on the same entry. At every bind of a durable steps instance, the
controller SHALL remove the instance's `_turn` entries whose origin offset is above the
instance's floor before it re-forwards the input-log tail, because such a turn never
reached its `dead_lettered` record and will run again.

#### Scenario: Steps relay failure path unchanged

- **WHEN** a steps relay that is not a `per_input` keep-progress loop has a cell that
  fails permanently on a message it could read
- **THEN** the tasklet aborts and compensation runs exactly as before this change, and
  nothing is written to the DLQ store

#### Scenario: A failed turn is parked with its message

- **WHEN** a durable `per_input` keep-progress session dead-letters the turn it took from
  offset 7 on entry channel `in`
- **THEN** `flz dlq show` of the entry SHALL show the slot `_turn`, the channel `in`, the
  origin offset 7, the failing tasklet in the error, and the exact bytes that were sent

#### Scenario: A turn that did not reach its record is not left in the queue

- **GIVEN** a session whose turn at offset 9 was parked and whose runner died before the
  `dead_lettered` record was journaled
- **WHEN** the instance is rebound and the re-run of the turn succeeds
- **THEN** the DLQ SHALL hold no entry for offset 9

### Requirement: A flow relay's failure handling SHALL NOT change

The failure handling of a flow relay SHALL be the reference the steps turn
follows and SHALL NOT change with it: the retry ladder, the park before the Cell advances
on either tier, the entry's slot, identity and fields, and `flz dlq` SHALL behave exactly as
before this change.

#### Scenario: The stream suite is untouched

- **WHEN** `test/system/dlq_e2e_test.exs` runs under each of the seven runtimes after this
  change
- **THEN** it SHALL pass unmodified

### Requirement: A replayed turn SHALL enter its session as a new turn

`flz dlq replay` of a `_turn` entry SHALL send the entry's bytes to the relay's entry
channel through the ordinary input path, as every replay does: the session SHALL take it as
a new message with a new input-log offset, after any message already waiting, as the next
iteration. A replayed turn SHALL answer no caller of the original message. A replay that
fails again SHALL be dead-lettered as a new entry under its new offset, and the replayed
entry SHALL be removed once the replay grace has passed, as a replay of an entry that
carries an origin offset is today. A replay of an entry without payload bytes SHALL be
refused as it is today, and a replay to a session that has ended SHALL be refused with the
code a replay to a missing relay answers.

#### Scenario: A replayed turn is answered

- **GIVEN** a session that dead-lettered the message `boom` while the model was down
- **WHEN** the model is back and `flz dlq replay <entry>` is run
- **THEN** the session SHALL answer `boom` as its next iteration and the entry SHALL be
  removed

#### Scenario: A replay to an ended session is refused

- **WHEN** the session has completed and its `_turn` entry is replayed
- **THEN** the replay SHALL be refused and the entry SHALL stay parked

### Requirement: A sink's park SHALL record a catalogue error

A sink that parks an output through the outbox SHALL hand the dead-letter store a catalogue
error, and the entry's `error` SHALL be that error's JSON object (code, class, message, details).
The HTTP push, RabbitMQ, Kafka and websocket sinks SHALL park with `http_push.exhausted`,
`rabbitmq.exhausted` or `rabbitmq.unroutable`, `kafka.exhausted` and `websocket.unacknowledged`.
No entry SHALL carry an inspected Elixir term as its error.

#### Scenario: A websocket park reads as a code

- **WHEN** a websocket sink exhausts a frame's retries and `flz dlq list` is read
- **THEN** the entry's error SHALL carry the code `websocket.unacknowledged`

