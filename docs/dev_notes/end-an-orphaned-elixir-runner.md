# The Elixir runner's stdin watcher hangs when its stdout breaks

`end-an-orphaned-elixir-runner` (KI-134). Two lines in
`test/apps/elixir_runner/lib/application.ex` — one before the loop, one condition
inside it — a rename beside them, two subprocess tests in
`test/flezha/sdk/elixir_runner_exit_test.exs`, one MODIFIED requirement in
`sdk-conformance`.

## Two facts, and only the second is surprising

**One device, both directions.** Erlang's standard io is a single process,
registered `user`, owning descriptors 0 and 1 together. Killing it takes reading
and writing at once. That is already unlike every other SDK: Rust
(`io::stdin().read`), Java (`System.in.read`), Python
(`os.read(sys.stdin.fileno(), 1)`), Ruby (`$stdin.readpartial`), Node
(`native.watchStdinEof`) and PHP (`Shutdown`'s non-blocking `STDIN`) all read
descriptor 0 directly, where a broken standard output cannot reach.

**The group leader is a relay, not the device.** A process spawned from an
application's `start/2` — which the watcher is — has the **application master** as
its group leader, and Elixir's `:stdio` *is* the group leader. The master forwards
io requests to `user` without being it. `:io.request/2` monitors only the process
it sent to. So when `user` dies the request is never answered and never reported:
the watcher sits in `:io.execute_request/3` for ever.

The failing arm, step by step:

1. The harness's `Port.close/1` drops both pipes.
2. The SDK logs its disconnect — eight `stream closed during disconnect:
   connection_closed` lines — into a pipe with no reader.
3. The device's writer takes `EPIPE` and dies. Standard error is inherited from
   the harness's shell, not the port, so `Failed to write log message to stdout,
   trying stderr` and `[error] Writer crashed (:epipe)` survive into the run log —
   they are the discriminator between a failing arm and a clean one.
4. The watcher's relayed read is answered nothing. Measured:
   `{:current_function, {:io, :execute_request, 3}}` 1.5 s after the break, still
   there after the close.
5. The end-of-file the same close produced reaches a device nobody is reading. The
   VM stays up and the SDK reconnects on its ladder for ever.

A live orphan caught mid-change confirmed every part from outside: `fd 1` a pipe
whose reader was gone, `fd 0` a pipe whose **writer** was gone — end-of-file sitting
there available — and `fd 2` still the run's log file, which is why its
`SIGTERM received - shutting down` line landed there when it was finally killed.

## The trap: a bare script answers a different question

In a plain script the group leader **is** `user`, so its death answers the blocked
read `{:error, :terminated}` and the `{:error, _}` branch is reached. That is real,
reproducible (`scratchpad/ki134/e3.sh`), and **not what the runner does**. Taking it
for the runner's behaviour cost this change a whole pass: a fix to that branch
alone shipped, its test passed, and the next e2e arm still left an orphan. Any probe
of this watcher must arm it from an `Application.start/2` — `:application.load/1`
with an inline module is enough — or it is testing a different topology.

## Half one: read the device, not the relay

`Process.group_leader(self(), Process.whereis(:user))` before the loop puts the
watcher's requests on the process that can die, so the monitor is on the right one.
Guarded with `if device = …`: a VM with no registered `user` behaves as before
rather than raising inside a spawned process — a watcher that dies silently is the
defect, not a fix for it.

## Half two: `{:error, _}` still cannot simply become `:eof`

The branch's comment was right and stays right. The device also dies as an ordinary
part of VM shutdown — applications stop before `kernel`'s `user` — so a failed boot
or a graceful stop produces the same `{:error, :terminated}`. Halting there
truncates the report and replaces the VM's chosen exit status with `0`, which is how
a failed boot comes to look like a clean exit.

`:init.get_status/0` separates them, measured in this app's own VM under `mix run`:

| moment | `:init.get_status()` |
| --- | --- |
| inside the boot script, where `Application.start/2` runs | `{:starting, :started}` |
| idle under `--no-halt`, the runner's steady state | `{:started, :started}` |
| 5 ms after `System.stop/1`, and onwards | `{:stopping, :started}` |

`match?({:started, _}, :init.get_status())` is true exactly when a dead device means
the port went away. The `:starting` window is covered for free: a boot that breaks
its own output does not halt itself.

## Both halves are load-bearing, and the test says so

Removing either one alone leaves the first test red with `no matching message after
30000ms` — the VM never exits. Pristine HEAD is red the same way. Only both
together are green.

## Two claims of the entry, and two alternatives, that did not survive

- **"Every multi-module elixir run"** — measured ~1 in 6 across two workers
  (once in seven arms, once in five, once in six).
- **"SIGTERM-resistant"** — the live specimen ended **250 ms** after a SIGTERM.
  Earlier sightings' `SIGKILL` raced a shutdown already under way.
- **A bare VM's children read `:eof` and halt**, with and without stdout noise. The
  noise there was drained by the parent, so the pipe never broke; noise alone is not
  the trigger, a *broken pipe* is.
- **Two ports spawned back to back do not share stdin/stdout** — only descriptor 2
  — so "the next module's spawn holds the previous pipe open" is refuted.

## The test's four other traps

- **`setup_all` compiles the test app.** Otherwise the compiler's own `Compiling 1
  file (.ex)` line takes the `EPIPE` first, `mix` dies with exit 1 and the test
  reads that instead of the watcher's verdict. The child runs `--no-compile`.
- **The break waits for `:init.notify_when_started/1`.** Run inside the boot script
  it is `{:starting, :started}`, the fix correctly does not halt, and the test fails
  for the wrong reason. The notification is an event, so no sleep and no poll.
- **A pipeline's exit status is its last command's**, so `head`'s `0` is what the
  port reports. The shell writes the VM's own status to a file (`echo $?`) and the
  port's `:exit_status` is only the settle.
- **A red run leaves a VM that will not die.** The child writes its OS pid and
  `on_exit` kills it; `:stderr_to_stdout` on the port keeps the child's EPIPE report
  out of the suite's output.

The second test is not a duplicate: it drives `System.stop(3)` before the break and
asserts the exit status is `3`. Under the unconditional `System.halt(0)` shape that
Rust, Java and Python can afford it reads `0`.

## Counting arms

A post-arm `ps` for `mix run --no-halt` matches **any** survivor, including one an
earlier arm left. Three consecutive "orphan" lines in this change's first pass were
one pid with a growing elapsed time. Kill what the check finds, or the rate is
wrong.

## What this did not touch

`RunnerServer.reconnect/2`'s synchronous `Process.sleep` retry inside `handle_info`
was the entry's fourth-sighting candidate. It explains how long a *supervised* stop
takes, not how a process survives with no supervisor stop at all — the orphan's
`mix` parent was gone and nothing had asked it to stop. The `shutdown: 10_000`
budget it interacts with is `carry-state-persistence-in-elixir`'s 5 s serialise plus
2 s ack; unchanged here.
