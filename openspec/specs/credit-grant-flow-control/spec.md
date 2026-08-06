# credit-grant-flow-control Specification

## Purpose
TBD - created by archiving change credit-grant-flow-control. Update Purpose after archive.
## Requirements
### Requirement: Credit window is negotiated at stream attach

The controller SHALL offer credit-grant flow control to every SDK session at state-stream attach via a `CreditWindowInit{stream_id, window_bytes, low_watermark_bytes}` control-channel frame. A session that acknowledges the handshake is a credit-speaking session; a session that does not acknowledge (a pre-credit SDK) SHALL receive the legacy `EntityMailboxStatus` behaviour unchanged. The window SHALL be configurable per Operative (`:credit_window_bytes`), defaulting to `wal_high_watermark_bytes`.

#### Scenario: Credit-speaking session established

- **WHEN** an SDK that supports credit attaches its state stream and acknowledges `CreditWindowInit`
- **THEN** the controller marks the session credit-speaking, stops shipping `EntityMailboxStatus` frames to it, and enforces the byte ledger on that stream

#### Scenario: Pre-credit SDK keeps legacy behaviour

- **WHEN** an SDK attaches and does not acknowledge `CreditWindowInit`
- **THEN** the controller applies the legacy soft-watermark `EntityMailboxStatus` protocol and hard-bound shedding, byte-for-byte as before this change

#### Scenario: Kill switch disables the handshake

- **WHEN** `FLEZHA_CREDIT_FLOW_CONTROL=false` is set on the controller
- **THEN** no `CreditWindowInit` is offered and every session runs the legacy path

### Requirement: Per-stream byte ledger debits state-change frames

The controller SHALL keep one credit ledger per credit-speaking state stream, owned by the stream's channel process. Every state-change frame received on the stream SHALL debit the ledger by the frame's wire size. The ledger SHALL bound the controller's un-fsynced buffer for that stream at `window_bytes`.

#### Scenario: Receipt debits the ledger

- **WHEN** a credit-speaking SDK sends a state-change frame of N wire bytes
- **THEN** the stream's ledger decreases by N and the controller's un-fsynced bytes attributable to that stream never exceed `window_bytes`

### Requirement: WAL fsync mints credit; grants are watermark-batched

Credit SHALL be minted only when appended bytes become durable: on fsync completion each `WalWriter` SHALL report the fsynced byte count to its stream's ledger. The controller SHALL NOT send a grant per record; it SHALL send one `CreditGrant{stream_id, bytes}` control frame when the freed credit brings the un-fsynced buffer below `low_watermark_bytes` (batched replenishment). Append without fsync SHALL NOT mint credit.

#### Scenario: Fsync batch replenishes the window

- **WHEN** an fsync batch completes covering M bytes of a credit-speaking stream's records and the stream's un-fsynced buffer falls below `low_watermark_bytes`
- **THEN** the controller sends one `CreditGrant` for the freed bytes and the SDK's window increases by M

#### Scenario: Stalled fsync stops minting

- **WHEN** the disk stalls so no fsync completes
- **THEN** no grants are sent, the sender exhausts its window and parks, and the controller's un-fsynced buffer stays bounded by `window_bytes` instead of growing

### Requirement: A sender out of credit parks, never drops

When an SDK's credit window is insufficient for the next state-change frame, the SDK SHALL park the producing Entity's emit path until a grant arrives. The park SHALL NOT block the control channel (heartbeats and control traffic keep flowing), SHALL NOT drop or reorder the pending frame, and SHALL be observable: the SDK SHALL bump a `credit_parked` liveness counter visible to the existing liveness self-report so a credit-parked runner is classifiable as parked rather than silently frozen. Both the Rust and Java SDKs SHALL implement identical park semantics.

#### Scenario: Exhausted window parks the producer

- **WHEN** an Entity produces state changes faster than fsync mints credit and the window reaches zero
- **THEN** the producer parks on the next send, no frame is dropped, and processing resumes when the next `CreditGrant` arrives

#### Scenario: Park is visible to liveness

- **WHEN** an SDK is parked on credit for longer than one watchdog sweep
- **THEN** the `credit_parked` counter has advanced and the runner's stall classification reports a credit park, not a silent freeze

#### Scenario: Control channel unaffected by a parked data path

- **WHEN** an Entity's emit path is parked on credit
- **THEN** control-channel heartbeats from that runner continue uninterrupted

### Requirement: Credit overrun is a counted anomaly, not a load response

A state-change frame arriving on a credit-speaking stream whose ledger is at or below zero is a protocol violation. The controller SHALL shed the frame via the existing hard-bound path, increment a `credit_overrun_total` counter, and log a warning identifying the session. For a conformant credit-speaking sender the shed path SHALL be unreachable; a non-zero `credit_overrun_total` or `wal_shed_total` on a credit-speaking session SHALL be treated as an anomaly signal in observability, never as expected back-pressure.

#### Scenario: Overrunning frame is shed and counted

- **WHEN** a credit-speaking SDK sends a frame while its ledger is exhausted
- **THEN** the frame is shed, `credit_overrun_total` increments, a warning names the session, and the seq-gap resync path repairs the gap

### Requirement: Per-Entity receiver queues prevent head-of-line blocking

After stream demux the controller SHALL queue state changes per Entity, so one Entity with a slow WAL cannot stall appends of sibling Entities sharing the stream. A slow Entity consumes its share of the shared window only; siblings continue while window credit remains.

#### Scenario: Slow entity does not stall siblings

- **WHEN** Entity k1's WAL drains slowly while sibling k2's WAL is healthy, both on one stream with remaining credit
- **THEN** k2's appends continue to be written and fsynced while k1's queue grows within the window bound

### Requirement: Reconnect resets the ledger

Credit SHALL NOT carry across sessions. On stream re-attach (reconnect, takeover) the handshake SHALL re-run and both sides SHALL reset to a full window; grants in flight for the dead session are void.

#### Scenario: Reconnect starts from a full window

- **WHEN** a credit-speaking SDK reconnects after a connection loss with grants in flight
- **THEN** `CreditWindowInit` re-establishes a full window on both sides and no stale grant is applied

### Requirement: Source-ingestion credit is minted by min-watermark advance (effectively-once tier only)

When an Operative runs with the effectively-once tier's durable input-log, the controller SHALL keep a per-Operative source credit ledger: dispatching a payload from the input-log to the Operative debits it by the payload's size, and the Operative's min-watermark advancing past consumed offsets credits back the corresponding bytes (batched, low/high watermark). At zero source credit the source adapter's ingest path SHALL park — stop reading its socket or pause its consumer — rather than growing the input-log without bound. When the tier is off, no source credit ledger exists and source adapters behave exactly as today.

#### Scenario: Frozen watermark parks the source

- **WHEN** one Entity stops advancing its consumed offset so the Operative's min-watermark freezes while ingestion continues
- **THEN** the source credit ledger drains to zero and the source adapter parks, bounding input-log growth, until the watermark advances again

#### Scenario: Tier off means no source ledger

- **WHEN** an Operative runs without the effectively-once tier
- **THEN** no source credit accounting occurs and the source adapter's behaviour is unchanged from today

#### Scenario: Per-operative isolation of source park

- **WHEN** an adapter feeds two Operatives and only one has a frozen min-watermark
- **THEN** only the starved Operative's dispatch parks; the healthy Operative keeps receiving input

