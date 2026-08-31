# `InputLog`'s fsync companion dies loudly now

`close-input-log-syncer-blindness` (KI-53). Three edits inside
`lib/flezha/state/input_log.ex`, nothing else in `lib/`.

## The filed half was the smaller one

KI-53 filed the silent fsync: `InputLog` traps exits, `spawn_link`s its fsync
companion and defines no `handle_info/2`, so the companion's `{:EXIT, …}` lands
on `GenServer`'s default handler, is logged as "unexpected message", and every
later `send(state.syncer, :sync)` succeeds against a dead pid. The batcher's
tick uses `fsync_async/1` — a cast that cannot fail — so the class leaves the
dirty set every tick and the batcher believes it synced. **10 of 10 ticks**
reached the exited pid in the driven case, one per append.

The unfiled half is worse. `sync_now/1` was

```elixir
ref = make_ref()
send(syncer, {:sync, self(), ref})
receive do
  {^ref, :ok} -> :ok
end
```

with no `after` and no monitor, under the comment *"the caller's call timeout
bounds the wait"*. **That sentence is false and is why the hole survived**: a
caller's timeout bounds the caller, never the callee. A `:fsync` **call** with
a dead companion parks the log for ever — alive, at `status: :waiting`, stack
pinned in `sync_now/1` — and every append for that operative class parks behind
it. It is reachable through graceful shutdown (`Shutdown.Coordinator` →
`FsyncBatcher.flush_all` → `drain(state, :call)`), where it stalls the final
flush of every dirty writer past the finalise timeout.

**The wedge happens before any `handle_info` clause could run**, which is what
decides the design: a re-spawn clause is unreachable from the parked state, so
the question was never "A or B" but "bound + A" or "bound + B".

## Why stopping, and not re-spawning

`terminate/2` already datasyncs the log's own descriptor and closes it, so
**stopping flushes the window the dead companion owed**; a re-spawn only re-arms
the next tick. The `:transient` child restarts under `Flezha.State.Supervisor`
(`max_restarts: 100, max_seconds: 5`) and `handle_continue(:open)` `mkdir_p`s
before opening, so the one condition that could loop — the class directory gone,
`open_raw/1` badmatching instantly — **self-heals** rather than spinning. A
re-spawn would loop there unboundedly with no supervisor budget in the way,
because the log never dies; bounding it means an intensity counter inside a
supervised child, which is a supervisor reimplemented one layer down.

The accepted residual: repeated deaths spend the shared writer supervisor's
restart budget. No new loop class — ENOSPC/EIO already crash the log directly at
`:ok = :file.write(…)`.

## The three edits

1. **`sync_now/1` monitors.** The monitor reference replaces `make_ref/0`, so
   one term correlates the reply and the `:DOWN`; `Process.demonitor(ref,
   [:flush])` on the reply. A dead companion answers **`:error`**, which
   `FsyncBatcher.drain/2` already reads as *"a writer that did not confirm its
   sync stays dirty for the next drain"* — the existing contract, extended, not
   a new one. Nothing at any call site changed: `handle_call(:fsync, …)` already
   replies with whatever `sync_now/1` answers.
2. **`handle_info({:EXIT, syncer, reason}, %{syncer: syncer} = state)`** emits
   `[:flezha, :state, :input_log_syncer_died]` (`%{count: 1}`,
   `%{operative: class, reason: reason}` — the metadata shape
   `input_log_frozen_watermark` already uses) and stops with
   `{:input_log_syncer_died, reason}`.
3. **A catch-all `handle_info/2`.** Defining *any* `handle_info/2` replaces
   `GenServer`'s default handler, so without this a stray message becomes a
   `FunctionClauseError` that kills the log. It reproduces the default report
   verbatim, at the same level:

   ```
   [error] Flezha.State.InputLog #PID<0.455.0> received unexpected message in
     handle_info/2: :a_stray_message
   ```

   `{:noreply, state}` on an `{:EXIT, _, _}` is what the entry always forbade —
   it turns a durability hole into a *silent* durability hole.

## The proof is two rows and two negative controls

`test/flezha/state/input_log_test.exs` → `describe "the fsync companion's
death"`. **Neither row asserts that a clause exists**; both drive the production
fsync path only.

| row | at HEAD | after |
| --- | --- | --- |
| `dead_syncs() == 0` — trace the log's sends, kill the companion, append and let the **batcher's own tick** fsync | `left: 10` | `0` |
| a `:fsync` request answered within 300 ms | `status: :waiting`, stack in `sync_now/1` | `{:reply, :error}` |

Reverting **only** the `sync_now` monitor turns **row 2 only** red; reverting
**only** the `{:EXIT, …}` clause (catch-all kept) turns **row 1 only** red. That
separation is what proves two independent mechanisms rather than one wearing two
hats.

## Three things the tests had to get right

- **Row 1 must not synthesise a cast.** A hand-rolled `GenServer.cast(log,
  :fsync)` would prove the clause, not the path. Each append marks the class
  dirty and the shared `FsyncBatcher`'s 5 ms tick casts `:fsync`; the settle is
  *the class leaving the batcher's dirty set*, never a sleep.
- **`Process.exit(syncer, :kill)` does not order the `EXIT` against a following
  call.** Signals are ordered pairwise, and the `EXIT` comes from the companion
  while the call comes from the test, so the log could answer `:last_offset`
  *before* handling the `EXIT`, hand the next append to a log about to stop, and
  fail with `{:input_log_syncer_died, :killed}` out of `do_append/4`. Waiting
  until `Process.alive?(syncer)` is false first is what makes it deterministic —
  a local process's link signals are delivered as part of its termination.
- **"Alive" is not "serving".** A log inside `terminate/2` is alive and takes no
  more requests, so the helper that waits for the restarted log requires it to
  *answer* a `:last_offset` call, not merely to be alive.

Row 2 needs the `:fsync` request queued **ahead** of the `EXIT` — the order the
shutdown flush arrives in, and the only order in which the park is reachable.
`:sys.suspend/1`, `:gen_server.send_request/2`, wait for
`message_queue_len > 0`, kill, `:sys.resume/1`.

## Where it landed, and why not in its own file

The two tests are in **`input_log_test.exs`**, the module's behaviour file, not
a new `input_log_syncer_test.exs`. A new `async: false` test file moves the
**blocking** `test` area's `async_false_share` from 0.813 to 0.814 (230 → 231
files, 43 async) and fails `refactor_metrics.py --check`. Charter §9 wants the
fold anyway; the gate makes it mandatory. The module cannot be async: it writes
`:flezha` app env, drives the shared `FsyncBatcher` singleton and traces a pid.

## One shape deliberately not copied

`WalWriter` (`wal_writer.ex:808`) traps exits and has
`handle_info(_msg, state), do: {:noreply, state}`. It is benign today — its only
link is its parent, which `gen_server` handles itself — but it is exactly the
shape that would hide this defect if a `spawn_link` were ever added there.
Untouched on purpose; `InputLog`'s catch-all is loud instead.
