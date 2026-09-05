# dead-letter-queue Specification

## Purpose

The dead-letter-queue capability guarantees that a permanently-failing (poison) payload in a Stream operative is never silently dropped and never freezes the operative's progress. A failing procedure invocation — returned error or panic/unhandled exception, one policy in both SDKs — is retried up to a per-channel budget (default 3 total attempts, exponential backoff + jitter; author-classified non-retryable errors park immediately). After exhaustion the payload is durably parked to the controller's DLQ store (`app_data/dlq/<operative_class>/<procedure_id>/`, JSONL metadata index + per-entry payload blobs) strictly BEFORE the entity advances: tier-off sessions hold the entity until the fsynced append is acknowledged (`park_payload` data-stream frame + `ParkPayloadAck` control command), while effectively-once sessions record the park and the consumed-offset advance in one atomic WAL record so a crash can never advance past a lost park — parking un-freezes the min-watermark, keeping input-log retention and source credit moving. Operators inspect, replay (explicit-only, through the ordinary input path), or drop entries via `flz dlq`; retention is bounded by age and total size with loud telemetry per deletion. Workflow operatives are out of scope — their escape is AbortAll + saga compensation.
## Requirements
### Requirement: Failing procedure invocations are retried with backoff before parking

A Stream procedure invocation that fails — by returned error or by panic/unhandled exception — SHALL be retried up to a per-channel maximum (default 3 attempts) with exponential backoff and jitter. Authors SHALL be able to classify errors as non-retryable, which parks immediately without further attempts. A panic is retryable by default. The Rust and Java SDK dispatchers SHALL implement identical retry semantics, and the current catch-log-continue path SHALL be removed.

#### Scenario: Transient failure recovers within the retry budget

- **WHEN** a procedure invocation fails twice and succeeds on the third attempt
- **THEN** processing continues normally, no payload is parked, and attempt telemetry records the retries

#### Scenario: Non-retryable error parks immediately

- **WHEN** a procedure returns an error the author classified non-retryable
- **THEN** the payload is parked without further attempts

#### Scenario: Panic follows the same policy

- **WHEN** a procedure panics on every attempt until the retry budget is exhausted
- **THEN** the payload is parked exactly as a returned error would be, and the entity continues with the next payload — never a silent drop

### Requirement: A poison payload is durably parked before the entity advances

After the retry budget is exhausted the SDK SHALL ship a park frame (payload bytes, channel, entity key, origin offset where the effectively-once tier provides one, error context, attempt count) on the data stream, and SHALL NOT advance past the payload until the controller acknowledges the park as durable. Tier off: the controller appends the entry to the DLQ store and fsyncs before acking. Tier on: the park entry and the consumed-offset advance SHALL be recorded in one atomic WAL record (no state change, DLQ entry, consumed offset) so a crash can never both advance the offset and lose the parked payload; the entity's consumed offset advances past the poison payload so the operative's min-watermark can move.

#### Scenario: Park is durable before advance

- **WHEN** the controller crashes after acking a park but before the entity processes the next payload
- **THEN** on recovery the DLQ entry exists and the entity resumes after the parked payload — the payload is in exactly one place

#### Scenario: Crash between exhaustion and ack does not lose the payload

- **WHEN** the runner crashes after exhausting retries but before the park is acked
- **THEN** recovery replays the entity from its consumed offset, the poison payload is retried and parked again, and no duplicate DLQ entry survives (the park is idempotent on origin identity)

#### Scenario: Parking un-freezes the min-watermark (tier on)

- **WHEN** a tier-on entity's poison payload froze the operative's min-watermark
- **THEN** after the park the entity's consumed offset advances past it, the min-watermark resumes, input-log retention proceeds, and a parked source-credit loop resumes granting

### Requirement: DLQ storage is per-procedure, durable, and workflow

The controller SHALL store parked payloads under `app_data/dlq/<operative_class>/<procedure_id>/` as a JSONL metadata index (entry id, entity key, channel, origin offset, error, attempts, replay generation, timestamp) with payload bytes in adjacent per-entry blob files. Retention SHALL be bounded by age and total size (defaults 30 days / 4 GiB, more restrictive wins); deleting an entry by retention SHALL emit a dedicated loud telemetry event, since it discards already-parked data. Parking and retention SHALL be observable via counters (`dlq_parked_total`, `dlq_dropped_by_retention_total`, per-procedure entry count and bytes).

#### Scenario: Parked entry is inspectable after restart

- **WHEN** a payload is parked and the controller restarts
- **THEN** the entry and its payload bytes are still present and listable

#### Scenario: Retention deletion is loud

- **WHEN** the DLQ exceeds its size cap and the oldest entry is deleted
- **THEN** `dlq_dropped_by_retention_total` increments and a dedicated telemetry event names the deleted entry

### Requirement: CLI surface for inspect, replay, and drop

The CLI SHALL provide `flz dlq list <operative>` (entries with entity, channel, error, age — nearest-to-expiry first), `flz dlq show <entry>` (metadata + payload preview), `flz dlq replay <entry>` (re-dispatch through the normal input path; tier on mints fresh output identity by construction — the replay is journaled as a new input, so its effect records carry fresh WAL seqs; a failing replay re-parks with an incremented generation), and `flz dlq drop <entry>` (delete entry + blob, emitting an audit event). Replay SHALL be explicit-only — the system SHALL NOT automatically re-drive parked payloads.

#### Scenario: Replay re-enters the normal path

- **WHEN** an operator runs `flz dlq replay` on a parked entry after fixing the underlying bug
- **THEN** the payload is dispatched to its entity through the ordinary path, processes successfully, and the DLQ entry is removed

#### Scenario: Failing replay re-parks with generation

- **WHEN** a replayed payload exhausts its retry budget again
- **THEN** it is parked again with `replay generation` incremented, not dropped

#### Scenario: Drop is audited

- **WHEN** an operator runs `flz dlq drop`
- **THEN** the entry and payload are deleted and an audit event records the drop with the entry's identity

### Requirement: Workflow profile is explicitly out of scope with a documented equivalence

The dead-letter queue SHALL apply to Stream operatives only. For Workflow operatives a permanently-failing entity SHALL continue to fail its procedure (AbortAll) and trigger saga compensation — the profile's existing escape. Documentation SHALL state this equivalence and that configurable `on_entity_failure` policies (DeadLetter | MinSuccess) remain deferred.

#### Scenario: Workflow failure path unchanged

- **WHEN** a Workflow operative's entity fails permanently
- **THEN** the procedure aborts and compensation runs exactly as before this change, and nothing is written to the DLQ store

### Requirement: A parked entry SHALL be built by one constructor

Every writer that parks a payload SHALL build the entry through one constructor
in the DLQ directory — the runner-side settlement, the outbox, the records path
and the operative-forward provider alike — that validates the required fields
and answers the record the store appends; no writer SHALL hand the store a map it
built by hand. The entry's on-disk keys and directory layout SHALL be unchanged
by the constructor, and the runner's wire frame SHALL remain the wire form of
that entry rather than a further copy.

#### Scenario: Every append site passes the constructed entry

- **WHEN** the tree is searched for appends to the DLQ store
- **THEN** every site SHALL pass the constructed entry, and a site that passes a
  bare map SHALL fail the DLQ's own test naming the file

#### Scenario: A missing required field is refused before the write

- **WHEN** a writer constructs an entry without the entity key, the channel or
  the error
- **THEN** the constructor SHALL answer `{:error, reason}` naming the field and
  no partial row SHALL be written

#### Scenario: Existing parked entries are still listed

- **WHEN** entries parked before this constructor existed are listed and shown
- **THEN** they SHALL be read back unchanged, under the same directories, by the
  same verbs

