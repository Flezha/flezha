# `fix-outbox-watermark-overwrite` — dev notes

## The shape: two writers, one value, and the outer one arrives last

An Entity's consumed-offset has one home, `InputLog.@watermarks`, and three
production sites that report an advance into it:

| site | what it reports |
|---|---|
| `wal_writer.ex` boot re-drive | the furthest offset the WAL scan found |
| `wal_writer.ex` `advance_consumed/2` | a closing outbox window's furthest offset |
| `hydrator.ex` `seed_consumed_offset/3` | the cut a remote snapshot shipped at |

and one that reports a rewind, `wal_writer.ex`'s `rewind_input_watermark/2` →
`InputLog.rewind_consumed/3`.

The defect was never that two windows exist. It is that the invariant all four
assume was written down twice and enforced nowhere: `furthest_consumed/2` folds a
window with `max/2` and says "Consumed-offsets only move forward" as its reason;
`rewind_consumed/3`'s doc argues the same rule from the other side; and
`record_consumed/3` was an unconditional `:ets.insert`. The `max/2` fold is
*window-local*, so nothing made the rule hold **between** windows.

## Why a window nests inside another window

`take_outbox_window/3` calls `await_entry/4` **before** it recurses, and
`await_entry/4` → `register_entry/4` → `tally/3` completes an entry whose tally is
already `pending: 0`. **An entry with no outputs is exactly that**, and an entry
with no outputs is exactly what a dead-letter park produces. So:

```
window-open depth=1
  take seq=0                       # the park record, consumed_offset 1, no outputs
  complete_entry {:cursor, 0}      # completes inside the window that dispatched it
    window-open depth=2            # try_dispatch_outbox re-enters
      take seq=1                   # the next record, consumed_offset 2
      apply_effect seq=1
    window-close depth=2 consumed=2
    record_consumed 2              # <-- the nested window's advance
  apply_effect seq=0 dlq=true      # the park materializes HERE
window-close depth=1 consumed=1    # the outer window's own furthest offset
record_consumed 1                  # <-- writes 1 back over 2
```

That trace is the probe output from the regression test, and it is the whole
defect. `advance_consumed/2` on the way out of the outer window is the second
writer, and it wins by arriving last.

Nothing else is repeated. Each record is taken exactly once (`take seq=0` at
depth 1, `take seq=1` at depth 2); `apply_effect/2` runs once per record; the
outer window dispatches an empty batch and `Outbox.dispatch(_, [])` is a no-op.
The **only** duplicated act is the watermark write.

## The rule belongs at the value, not at the call sites

`docs/known_issues.md`'s KI-1 said a fix must "make the nested and outer windows
agree on who owns the watermark, rather than making `record_consumed/3`
conditional and leaving two writers". The change's design overrules that
deliberately, and the reason is worth keeping: ordering the two writers fixes
this instance and nothing else. Any future re-entry, or a hydration racing a
closing window, reintroduces it — and it encodes a control-flow fact into two
files that must then stay in step, which is the parallel mechanism the defect is
made of. **No caller is in a position to hold the rule**: a window folds its own
entries forward with `max/2` and knows nothing of a window nested inside its own
dispatch; a hydration knows nothing of a window closing beside it. Only the value
they all report into sees both.

So `record_consumed/3` advances the stored offset or leaves it, and the
enforcement is one match spec:

```elixir
[{{key, :"$1"}, [{:<, :"$1", offset}], [{:const, {key, offset}}]}]
```

`:ets.select_replace/2` makes deciding the guard and acting on it indivisible,
which matters because reporters are separate `WalWriter` processes and a
read-then-write pair can interleave and lose an advance. `0` from the replace
means either "a row already stands at or past `offset`" — read it, it is the
authority — or "no row yet", claimed with `:ets.insert_new/2` and re-decided if
that claim loses.

Two things about that spec that are easy to get wrong, and a wrong spec silently
matches nothing, which looks exactly like the bug being fixed:

- the key is a *literal tuple in the head*, which a match spec matches
  structurally — fine here because nothing in it is `$`-prefixed;
- the body must be `{:const, {key, offset}}`. A bare `{key, offset}` in a match
  spec body is read as a function call, not as a tuple.

The new unit tests assert both directions and the absent-key first write, so a
spec that matches nothing fails the advance case immediately.

## The notify carries the STORED offset, not the reported one

This is the part the design's D3 states only half of. The rejected write still
notifies — but it notifies with the value now **stored**, not the value asked
for. The `{:consumed, entity_id, offset}` cast is what the log server folds into
its own `consumed` map (`note_consumed/3` is an unconditional `Map.put`, and that
map is what `advance_watermark/1` mins over for the credit drain). Notifying with
the *reported* offset would leave the guard protecting only the ETS row while the
log's own copy still took the lower value — half the defect unfixed. Reporting
the stored value instead needs no change to the server at all, because for a
given Entity there is exactly one live reporter (`Supervisor` keys `WalWriter`s
uniquely per Entity, and `Hydrator.apply_manifest/3` seeds only when
`Artifact.has_local_state?/1` is false), so its casts arrive in order.

## What the three advance sites do under the new rule

- **Boot re-drive** (`handle_continue(:open, …)`): reports the max
  `consumed_offset` over the *whole* WAL, so it can only be at or above anything
  a window ever reported from that WAL. Retention drops records from the front
  only, so a compacted WAL yields `nil` (not reported at all) rather than a lower
  integer. Never legitimately lower.
- **`advance_consumed/2`**: the case being fixed. A window whose furthest offset
  is below the stored one is now ignored.
- **`hydrator.ex`**: `apply_manifest/3` is a no-op when
  `Artifact.has_local_state?(ref)`, so the seed only ever runs against an Entity
  with no local files — in practice an absent row, taking the `insert_new/2`
  path. If a stale higher row did survive, the seed is ignored, which is the
  conservative direction for credit and the *aggressive* one for retention: a
  higher watermark truncates more. Worth knowing, not worth guarding: hydration
  to an earlier cut is semantically a rewind, and `rewind_consumed/3` is the tool
  for that.

## Measurements, and one that contradicts the proposal

- **`dlq_e2e_test.exs:121` before the fix: 0 of 6 runs passed.** The proposal
  claims 14 of 20 failures; on this box the failure is effectively deterministic.
  Each run costs ~33 s failing (30 s of it the `wait_until` timeout) and ~1.5 s
  passing, so the pass/fail split is visible in the wall clock alone.
- After the fix: the whole file (both tests) passed 5 of 5 runs. No other failure
  in that file to record.
- The proposal's task 4.1 says to "un-skip" that test. **There is nothing to
  un-skip.** Its `skip:` is `FlezhaTest.SystemCase.deferral(:effectively_once)`,
  which is `nil` for every runtime whose conformance manifest does not defer that
  capability — `test/apps/rust_runner/conformance.json` defers nothing, so under
  the default runtime the test runs, and it ran and failed on HEAD.
- The e2e needs no fsync-interval manipulation to reproduce this. The unit test
  needs none either: `TestHelpers.in_one_window/1` suspends the shared
  `FsyncBatcher` and `WalWriter.fsync/1` then makes both records durable in one
  `do_fsync/1`, which is the "same fsync batch" precondition, forced rather than
  sampled. The regression test is a single 0.4 s run and fails deterministically
  on the defective code with `left: 1, right: 2`.
- `refactor-baseline-2` runs the same regression test red.
  `git diff refactor-baseline-2 HEAD -- lib/flezha/state/input_log.ex
  lib/flezha/state/wal_writer.ex lib/flezha/state/outbox.ex
  test/flezha/state/wal_writer_outbox_test.exs test/helpers/state_test_helpers.ex`
  is **empty** — production code and harness alike are byte-identical, so the
  defect predates the refactor programme outright.

To run a test file against that tag: `git worktree add`, symlink `deps/`, set
`MIX_BUILD_PATH` and `CARGO_TARGET_DIR` to scratch paths, and give it
`FLEZHA_SOCKET_FILES_PATH` of its own or `BootLoader` refuses to start against
the main tree's socket directory. `RUSTLER_SKIP_COMPILATION=true` is not a
shortcut here: it leaves `Flezha.Transport.Native` undefined and the application
cannot boot, so the NIF has to be built (`mix compile --force`).

## Credit accounting is less damaged than the proposal says

The proposal states the retention floor **and** credit accounting under-report by
one offset. The retention floor genuinely does — `InputLog.watermark/1` reads the
ETS rows. Credit largely does not: `pop_freed/3` is destructive, so by the time
the outer window's low report arrives, the nested window's higher watermark has
already popped and granted those bytes. What the low report actually corrupts is
the log's in-memory `consumed` map, which self-heals on the Entity's next
advance. The observable failure is the watermark itself, which is what the e2e
asserts.

## Found on the way, filed separately

`docs/known_issues.md` KI-41: the nested window reports its consumed-offset
**before** the outer entry's `apply_effect/2` materializes the dead-letter park —
the exact inversion `apply_effect/2`'s own comment forbids ("the DLQ store entry
must be durable first"). The exposure is microseconds inside one `do_fsync/1` and
a crash in the window is safe (the boot re-drive re-applies an idempotent park),
but it is a real ordering inversion and it is not this change's to fix: reshaping
`complete_entry/2`'s control flow is `refactor-5a-state`'s work.

## Worth changing, not changed

`InputLog.forget_entity/2` and `rewind_consumed/3` with a negative offset are the
same operation — delete the row, notify a negative offset. Two functions, one
body. They differ only in doc intent, and collapsing them was out of scope here.
