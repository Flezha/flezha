# credit-grant-flow-control Specification

## Purpose
TBD - created by archiving change credit-grant-flow-control. Update Purpose after archive.
## Requirements
### Requirement: Credit window is negotiated at stream attach

The controller SHALL offer credit-grant flow control to every SDK session at state-stream attach via a `CreditWindowInit{stream_id, window_bytes, low_watermark_bytes}` control-channel frame. A session that acknowledges the handshake is a credit-speaking session; a session that does not acknowledge (a pre-credit SDK) SHALL receive the legacy `CellMailboxStatus` behaviour unchanged. The window SHALL be configurable per Relay (`:credit_window_bytes`), defaulting to `wal_high_watermark_bytes`.

#### Scenario: Credit-speaking session established

- **WHEN** an SDK that supports credit attaches its state stream and acknowledges `CreditWindowInit`
- **THEN** the controller marks the session credit-speaking, stops shipping `CellMailboxStatus` frames to it, and enforces the byte ledger on that stream

#### Scenario: Pre-credit SDK keeps legacy behaviour

- **WHEN** an SDK attaches and does not acknowledge `CreditWindowInit`
- **THEN** the controller applies the legacy soft-watermark `CellMailboxStatus` protocol and hard-bound shedding, byte-for-byte as before this change

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

When an SDK's credit window is insufficient for the next state-change frame, the SDK SHALL park the producing Cell's emit path until a grant arrives. The park SHALL NOT block the control channel (heartbeats and control traffic keep flowing), SHALL NOT drop or reorder the pending frame, and SHALL be observable: the SDK SHALL bump a `credit_parked` liveness counter visible to the existing liveness self-report so a credit-parked runner is classifiable as parked rather than silently frozen. Both the Rust and Java SDKs SHALL implement identical park semantics.

#### Scenario: Exhausted window parks the producer

- **WHEN** a Cell produces state changes faster than fsync mints credit and the window reaches zero
- **THEN** the producer parks on the next send, no frame is dropped, and processing resumes when the next `CreditGrant` arrives

#### Scenario: Park is visible to liveness

- **WHEN** an SDK is parked on credit for longer than one watchdog sweep
- **THEN** the `credit_parked` counter has advanced and the runner's stall classification reports a credit park, not a silent freeze

#### Scenario: Control channel unaffected by a parked data path

- **WHEN** a Cell's emit path is parked on credit
- **THEN** control-channel heartbeats from that runner continue uninterrupted

### Requirement: Credit overrun is a counted anomaly, not a load response

A state-change frame arriving on a credit-speaking stream whose ledger is at or below zero is a protocol violation. The controller SHALL shed the frame via the existing hard-bound path, increment a `credit_overrun_total` counter, and log a warning identifying the session. For a conformant credit-speaking sender the shed path SHALL be unreachable; a non-zero `credit_overrun_total` or `wal_shed_total` on a credit-speaking session SHALL be treated as an anomaly signal in observability, never as expected back-pressure.

#### Scenario: Overrunning frame is shed and counted

- **WHEN** a credit-speaking SDK sends a frame while its ledger is exhausted
- **THEN** the frame is shed, `credit_overrun_total` increments, a warning names the session, and the seq-gap resync path repairs the gap

### Requirement: Per-Cell receiver queues prevent head-of-line blocking

After stream demux the controller SHALL queue state changes per Cell, so one Cell with a slow WAL cannot stall appends of sibling Cells sharing the stream. A slow Cell consumes its share of the shared window only; siblings continue while window credit remains.

#### Scenario: Slow cell does not stall siblings

- **WHEN** Cell k1's WAL drains slowly while sibling k2's WAL is healthy, both on one stream with remaining credit
- **THEN** k2's appends continue to be written and fsynced while k1's queue grows within the window bound

### Requirement: Reconnect resets the ledger

Credit SHALL NOT carry across sessions. On stream re-attach (reconnect, takeover) the handshake SHALL re-run and both sides SHALL reset to a full window; grants in flight for the dead session are void.

#### Scenario: Reconnect starts from a full window

- **WHEN** a credit-speaking SDK reconnects after a connection loss with grants in flight
- **THEN** `CreditWindowInit` re-establishes a full window on both sides and no stale grant is applied

### Requirement: Source-ingestion credit is minted by min-watermark advance (effectively-once tier only)

When a Relay runs with the effectively-once tier's durable input-log, the controller SHALL keep a per-Relay source credit ledger: dispatching a payload from the input-log to the Relay debits it by the payload's size, and the Relay's min-watermark advancing past consumed offsets credits back the corresponding bytes (batched, low/high watermark). At zero source credit the source adapter's ingest path SHALL park — stop reading its socket or pause its consumer — rather than growing the input-log without bound. The park SHALL land on the source's call, never on the Relay's host process, which SHALL keep running the output chains whose confirmations mint the credit — a host parked on its own log could never mint the credit it waits for. When the tier is off, no source credit ledger exists and source adapters behave exactly as today.

The min-watermark advance that drains the ledger SHALL be computed by the input-log from its own consumed-offset map (see `durable-input-log`), with a burst of per-Cell advances folded into one drain and at most one unpark pass, and the bytes credited back SHALL be those of the log's tracked offsets at or below the new watermark.

#### Scenario: Frozen watermark parks the source

- **WHEN** one Cell stops advancing its consumed offset so the Relay's min-watermark freezes while ingestion continues
- **THEN** the source credit ledger drains to zero and the source adapter parks, bounding input-log growth, until the watermark advances again

#### Scenario: A sink outage longer than the window does not wedge the relay

- **WHEN** the sink stops confirming for longer than `window / ingest rate`, so source credit reaches zero with outputs still owed, and then recovers
- **THEN** the host keeps delivering the owed outputs while the source is parked, their confirmations mint credit, and ingestion resumes without a restart

#### Scenario: A burst of advances drains the ledger once

- **WHEN** several Cells' consumed offsets become durable within one scheduling window of the input-log
- **THEN** the ledger is drained once for the bytes the new min-watermark freed, and parked appends are unparked in arrival order by that one grant

#### Scenario: Tier off means no source ledger

- **WHEN** a Relay runs without the effectively-once tier
- **THEN** no source credit accounting occurs and the source adapter's behaviour is unchanged from today

#### Scenario: Per-relay isolation of source park

- **WHEN** an adapter feeds two Relays and only one has a frozen min-watermark
- **THEN** only the starved Relay's dispatch parks; the healthy Relay keeps receiving input

### Requirement: A stream close clears the credit flag on the channel's own counters and nothing else

A data channel's liveness counters SHALL be one `:counters` array for the life of the channel process, registered once at init and read back by `{runner_id, name}`; a stream close SHALL clear `credit_active` and `credit_ledger_level` on that same array and SHALL leave `credit_overrun_total` — the count of frames shed over the channel's whole life — and the heartbeat-mirrored `hb_credit_parked` as they were, so a reader that named the channel at the handshake reads the burst's overrun count after any later close or re-bind rather than finding no credit-speaking row.

#### Scenario: The burst's channel is read by name after a close

- **WHEN** a credit-speaking secondary stream carried a burst and its socket then closed before the next state frame, so the credit reset ran with no handshake after it
- **THEN** the row named at the handshake reports `credit_active` 0 and the burst's `credit_overrun_total` and `hb_credit_parked`, while a search for a row with `credit_active == 1` finds none until the next state frame re-runs the handshake

