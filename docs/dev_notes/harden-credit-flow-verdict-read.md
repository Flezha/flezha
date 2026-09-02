# The credit verdict is read from the row the test settled on

`harden-credit-flow-verdict-read` (KI-128): `credit_flow_e2e_test.exs` no longer searches the
liveness registry afresh for a `credit_active == 1` row after the burst. Test-only; the
proposal and design say what moved. This is what the next person reading a red — or another
test that reads a liveness row — needs.

## A liveness row is a value, and its key is the handle

`LivenessCounters.snapshot_all/0` reads every slot into a map. A row captured at a settle is
the numbers **at the settle**; it never updates. What addresses the live array is the row's
key, `{runner_id, name}`, and `counters_row/2` reads it. KI-128's *A fix should* ("its
counters ref is per attach and holds the burst's numbers whatever happens after") was wrong
on both halves, in the direction that helps: the array is registered **once**, in
`StreamChannel.init/1`, and the `{:flz_closed}` handler keeps the process (`{:noreply, …}`) so
a re-bind (`{:bind, new_ref}`) lands on the same array. Only an owner death prunes an entry.

`reset_credit/1` zeroes `credit_active` and `credit_ledger_level` on that array and nothing
else: `credit_overrun_total` is bumped only, and `hb_credit_parked` is set only from heartbeat
metadata. Measured in the probe: the same row by key went `hb_credit_parked 91 → 95`,
`hb_count 22 → 22`, `credit_active 1 → 0`, `credit_ledger_level 60041 → 0`,
`credit_overrun_total 0 → 0` across the close.

## A controller-side close reaches no handler

The driver's `close_stream` (`flz_nif/src/uds_driver.rs:411-417`) removes the stream and drops
the fd with **no** `send_closed_msg`; only an EOF read or a write error emits
`{:flz_closed, id, reason}`. So `Transport.Stream.close/1` from a test leaves the channel
bound to a dead ref — the acceptor then answers the runner's reconnect with `:already_bound`
on that channel and binds the next one — which is another defect's shape, not KI-128's. The
probe sent the tuple the driver builds (`{:flz_closed, stream_id, "connection_closed"}`) to
the channel process the settled row names, with the real id from `:stream_ref_status`. The
runner-side reconnect (`RunnerMessage::Reconnect`, `control_unit.rs:282`) is not commandable.

## Every secondary channel heartbeats; one is credit-speaking

The runner opens the whole pool (`schedulers_online` secondary streams — eight here) and
heartbeats each, so all eight `scs_*` rows carry `hb_count` and `hb_credit_parked`, and
`parked_counter/1`'s max across them is right. Only the channel that carried the priming
state frame ran the handshake, so exactly one row is ever `credit_active == 1`; the burst's
`credit_overrun_total` lives on that one row and nowhere else, which is why the read is by
name and not by flag.

## The one way the by-name read could lie

A channel-process crash after the burst registers a fresh zeroed array under the same key. That
is a supervisor restart logged at `:error`, which the suite's log gate refuses, and it would
show as a different failure with a different message — not a silent pass to guard here.

## Left as found

- `test/load/credit_soak_test.exs:162,189` reads `active_credit_row/1` the same way; opt-in
  behind `MIX_RUN_CREDIT_SOAK=1`, never in the arms, out of this row.
- Every run of this file prints two `[error] Child Flezha.State.FsyncBatcher … terminated
  ** (exit) normal` lines: `restart_fsync_batcher/0` stops a permanent child with
  `GenServer.stop/1`. At HEAD, unchanged.
- In a background shell, a runner's stderr shares the log's file description with `mix`; lines
  it writes while draining after `mix` exits land at that description's offset and overwrite
  anything appended to the file meanwhile. Pipe through `cat` when the exit line matters.
