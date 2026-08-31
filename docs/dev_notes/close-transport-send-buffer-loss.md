# Close the transport's send-buffer loss (KI-3, KI-56, KI-70, KI-71, KI-73)

Row 6.5 of the refactor programme, run as three segments in one shared tree.
What follows is what cost time, not what the change does — `design.md` and the
`controller-send-buffer-bound` spec carry that.

## The gate is read by the sender and written by the sender

The whole change is one bit per stream. The trap is *where the bit is written*,
and segment B shipped the wrong answer with a green suite behind it.

`SendGate::observe` runs on the **driver thread**, behind an unbounded command
channel. A sender's `send_frame` queues a command and returns; the driver
dequeues it later. So a sender reading a bit the driver writes is reading a
value that lags by however many frames it has already queued — ~210 of them in
a 400-frame burst — and the gate rises *after* the sender has gone past it.
Everything queued in between arrives at a full buffer and is turned away at the
ceiling, with `:ok` already returned for every one. Measured on KI-3's harness
with the gate and the park both in place:

```
HELD:   send_gate_held=1 send_frame_calls=211
DRIVER: frames_written=131 write_buffer_dropped=79
SENDER: %{ok: 400}
```

**Parking the sender does not touch that.** The park removes the *repeat
refusals* — `send_gate_held` fell from 186 to 1 — and the *overshoot* is
everything queued before the bit was set, which no park can precede. The two
numbers looked alike (186 refusals, 83 overshoot) and are unrelated; `design.md`
D13 claimed the second was what the park removed, on a measurement, and is
corrected in place rather than deleted.

The fix is a two-sided ledger, and it is small: `SendGate` gains `inflight`
(bytes queued that the driver has not taken) and `buffered` (the depth the
driver last observed), `send_frame` adds its length and **raises the bit
itself** when the sum crosses the high-water mark, and each driver releases a
frame's bytes as it takes them. The gate then weighs a total no sender can
outrun, because the half it is racing is the half it writes. On the same
harness, four runs, `write_buffer_dropped=0` and `frames_written=400`.

**Every reservation needs exactly one release, including the ones that never
travel.** A `cmd_tx.send` that fails (a dead driver thread) discharges its own
reservation in `send_frame`; a client stream's writer thread discharges each
frame as it dequeues it. Miss one of those and `inflight` only grows, the gate
latches raised, and the stream wedges — which is worse than the defect being
fixed. The releases are `saturating_sub` for the same reason.

**The client stream's gate is `SendGate::ungated()` on purpose.** Its writer
thread holds no send buffer of the drivers' kind, so there is no depth to
signal about; making its gate live would turn a full command queue into
`{:error, :send_buffer_full}` at `Operative.emit_output/3`, which logs it as an
undelivered frame — congestion reported as loss, which is the shape this change
exists to remove. Its command channel being unbounded is a real and separate
gap, and it is not this row's.

## What the channel may believe about the gate, and what it may not

Once the sender raises the bit, `StreamChannel`'s mirrored `gate_raised?` is
strictly staler than `send_frame`'s own answer — and the driver stops reporting
the raise at all, because `observe` finds the bit already up and returns no
transition. A channel answering `:open` from that mirror sends a refused sender
straight back round the loop: refuse, ask, "open", refuse. So the mirror is
deleted, `{:await_send_gate, pid}` records interest whenever it is asked for,
and only the **lowered** report matters — it is what releases every holder.

The residual race is the one the hold interval already exists for: a drain that
completes between a sender's refusal and its registration leaves that sender
waiting out `:send_gate_hold_ms` and answering `{:error, :send_buffer_full}`
with nothing lost. It needs the peer to consume megabytes in the microseconds
between two statements, and it predates this segment.

## The counters lie about timing, and one assertion was built on that

`DriverStats` is rebuilt **once per driver-loop iteration**, at the end. A
driver writing 25 MiB inside one iteration has already put every byte on the
socket while its snapshot still reads the count from the previous iteration —
so `frames_written == 400` read straight after the peer has drained everything
fails with `left: 108`, ~1 run in 5. Delivery is asserted at the far end of the
socket instead: the peer counts bytes and reports once it has taken the burst's
payload total, a threshold 399 framed frames cannot reach even with their
headers.

`write_buffer_dropped` is read from the same snapshot and can therefore only be
stale *low*, which is why the load-bearing assertion is the one taken mid-burst
while the driver is looping hard, not the one at the end.

## Proving the gate engages

Every arm here has a control, because a gate that never engages looks exactly
like a gate that works.

- **Sender-side raise removed** (the KI-73 mechanism, alone): the two
  `write_buffer_dropped == 0` assertions in
  `test/flezha/runner/stream_channel_send_gate_test.exs` fail and nothing else
  does — 2 of 4, on two seeds. Restored: 4 of 4, on six seeds.
- **Gate forced off, ceiling kept** (`with_watermarks(cap, MAX, MAX)`): KI-3
  returns byte for byte as filed — `%{ok: 400}`, `frames_written=131`,
  `write_buffer_dropped=269`, `write_buffer_len=8357440`.
- **Gate forced off, ceiling removed** (`SendGate::ungated()`): no drops and a
  UDS buffer at 24.8 MiB against an 8 MiB cap — KI-56's failure mode on the
  other transport, which is what a bound-without-a-signal or a
  signal-without-a-bound each get you.
- **The credit interval knob**: a reader hard-wired to the default fails the
  test under `FLEZHA_CREDIT_PARK_INTERVAL_MS=7` with `left: 50ms right: 7ms`.
- **The watermark knobs**, driven live on the harness: `0.2`/`0.1` holds the
  sender after 31 calls instead of 108, and both malformed pairs (one mark
  alone; low above high) fall back to 108 — the refusal is a live path, not a
  parse-time claim.

## The knobs are fallbacks, and the wording is load-bearing

`FLEZHA_CREDIT_PARK_INTERVAL_MS` and `:send_gate_hold_ms` are lost-wake
fallbacks: `grant`/`arm` and the drain report both wake the waiter on the event,
so tuning either buys no latency in the normal case. `docs/configuration.md`
says so at both, because a knob documented as a throughput dial is worse than
an undocumented one.

The credit interval is read **inside `flz_core`**, with the crate's own
`util::env_parse_positive`, and that is a constraint rather than a taste: thread
it through a builder or an API parameter and the Java SDK immediately owes a
`HostClassDecl` key and a JNI setter for a diagnostic — a no-Java-work change
becomes a cross-SDK one. The Elixir SDK has no credit window at all.

The two watermarks are **taken whole or refused whole**. One configured mark
beside one default is how a low-water mark ends up above its high-water one,
which is a gate that raises and lowers on the same byte; anything but two
fractions with `0 < low < high < 1` leaves both defaults standing.

## Smaller things that cost time

- `\bD\d{1,2}\b` is a narrative marker in `scripts/refactor_metrics.py`, so a
  code comment may not cite a design decision by number. So is `no longer` —
  which matched a comment describing current behaviour, not history.
- Adding the drop verdict to `StallClassifier.classify/3`'s `||` chain took it
  from cyclomatic complexity 10 to 11 — a credo ratchet that would have been
  baked into the re-recorded baseline. The chain is split in two along the
  boundary the file's own section comments already draw (the transport, then
  everything above it); the order across both is unchanged, which is the rule.
- The published `openspec/specs/controller-send-buffer-bound/spec.md` **Purpose**
  paragraph is reached by no requirement delta and had to be corrected by hand;
  it is where KI-56's false "each drop is counted on `DriverStats` so it is
  observable" originated. A requirement's **title** changes only through a
  `## RENAMED Requirements` block, and `openspec validate --strict` passes
  without one — the failure surfaces at archive time.
