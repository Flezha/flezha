# SignalHandler owns SIGTERM (`fix-signal-handler-boot-race`)

The change is named after a boot-order race that **does not exist**. Read the
first section before re-investigating anything here.

## The misdiagnosis, and how to avoid repeating it

The symptom was a WARN on every single boot:

```
[shutdown] os.set_signal unavailable: {:error, :badarg}; falling back to
release-mode SIGTERM
```

The original diagnosis was "the signal service is not ready yet; retry on a
bounded ladder". It was wrong, and the evidence for it was a probe that only
ever tested **sigterm**:
`mix run -e 'Process.sleep(1500); :os.set_signal(:sigterm, :handle)'` prints
`:ok`, which looks like "the call works once boot settles".

Sampling both signals every 10 ms alongside application boot — 531 samples over
6 s — produces exactly one transition row, because nothing ever changes:

```
elapsed_ms | sigterm | sigint            | erl_signal_server alive
         0 | :ok     | {:error, :badarg} | true
first sample where BOTH succeed: nil
```

**`sigint` is not a settable signal.** ERTS reserves SIGINT for the break
handler; it is absent from the BIF's accepted list (OTP 29 `os:set_signal/2`):

```erlang
Signal :: 'sighup'  | 'sigquit' | 'sigabrt' | 'sigalrm' | 'sigterm'
        | 'sigusr1' | 'sigusr2' | 'sigchld' | 'sigstop' | 'sigtstp'
        | 'sigcont' | 'sigwinch' | 'siginfo',
```

`badarg` under every break-handler flag: `+B`, `+Bi`, `+Bd`, `+Bc`,
`-noshell`, `-noinput`. A retry ladder would have burned itself out on every
boot and latched `:degraded` anyway.

The old code ran both installs in one `try`, so the *successful* sigterm
install was discarded by the *impossible* sigint one.

## Kernel's handler and ours are mutually exclusive owners of SIGTERM

`erl_signal_server` is a `gen_event`; kernel adds `erl_signal_handler`, whose
`handle_event(sigterm, _)` calls `init:stop()`. Anything we add is *additional*,
and gen_event notifies every handler.

Measured, both arrangements:

- **Both registered** — ours is prepended and runs first, but kernel's
  `init:stop()` follows **~1 ms later**, so the shutdown is driven by
  `Flezha.Application.prep_stop/1` and recorded as `reason: :prep_stop`. The
  spec requires `%{reason: :sigterm}`. There is no way to win that race.
- **Kernel's deleted** — nothing calls `init:stop()` at all; the VM sat alive
  45 s after SIGTERM.

So we take ownership. Two consequences fall straight out of that:

**Deleting kernel's handler orphans `sigquit` and `sigusr1`**, which it also
owned (`sigquit` → `halt()`, `sigusr1` → `halt("Received SIGUSR1")`).
`SignalHandler` reproduces both. Only SIGTERM's disposition is deliberately
changed.

**`Relay` is added before kernel's is removed.** A failed registration then
leaves kernel's handler owning the signal instead of leaving the node with
neither — `:ok = add_sup_handler(...)` raising means the `delete_handler` below
it never runs. There is no ordering that ends with no handler at all.

## `Relay.terminate/2` runs INSIDE the manager — do not call back into it

The handler restores kernel's handler when it is removed, so a `SignalHandler`
crash degrades to `init:stop()` rather than to a node that ignores SIGTERM.
The obvious implementation deadlocks:

```elixir
def terminate(_reason, {_owner, manager}) do
  unless :erl_signal_handler in :gen_event.which_handlers(manager) do  # DEADLOCK
```

`terminate/2` is executed *by the manager process*, and both `which_handlers/1`
and `add_handler/3` are synchronous calls **to that same process**. The manager
blocks on itself until the call times out, which stalls handler removal. Hand
the restore to a spawned process. This cost real debugging time: it presented
as three unrelated-looking test failures where the handler "was never removed".

The handler is registered with `add_sup_handler/3`, so a crashed owner removes
it automatically instead of leaving it forwarding to a dead pid.

## `config/runtime.exs` silently overrode `config/test.exs`

`config/test.exs` sets `graceful_shutdown_enabled: false` with a comment
explaining why. `config/runtime.exs` then set it unconditionally to `true`,
and runtime config wins — so it had been on in the suite all along. That was
harmless only because the handler installed nothing. The moment dispatch is
live, it hands the **test suite's own BEAM's SIGTERM** to the controller.
The default is now `config_env() != :test`; an explicit env var still wins
everywhere.

## `signal_handler_status` is pulled, not pushed

`SignalHandler` is child #1 and `Coordinator` starts later, so the old
`report_signal_handler_status/1` **cast** was sent to a process that did not
exist yet and was swallowed by the caller's own `catch :exit, _ -> :ok`. The
field read `:ok` on boots that had degraded — the inverse of its contract.
`Coordinator.status/0` now reads it from the handler on demand, which deleted
the cast, the struct field, the `handle_cast` clause and the
`__reset_for_tests__` carry-over.

## Known follow-up: `[shutdown]` phase lines are invisible

The primary Logger level is `:warning` (`config/config.exs`), and every
`[shutdown] phase=…` / `[shutdown] received …` line is `Logger.info`. In dev and
prod they reach neither the console nor the file backend. **A graceful shutdown
that ran silently and one that never ran look identical in the logs.** Verifying
the drain requires attaching to `[:flezha, :shutdown, …]` telemetry instead —
that is how the `docker stop -t 90` evidence for this change was obtained. Not
fixed here; raising the primary level or promoting those lines is its own
change.

## Verification that matters

`docker stop -t 90` against the harness controller, with telemetry printed to
stderr so no log level can filter it:

```
>>> handlers=[Flezha.Shutdown.SignalHandler.Relay, :prim_tty_sighandler]
>>> signal_handler_status=:ok
>>> PID1=1
stop returned after 2s   exit_code=0
>>> TELEMETRY :requested       %{reason: :sigterm}
>>> TELEMETRY :phase_started   %{phase: :quiesce}
>>> TELEMETRY :phase_completed %{phase: :quiesce}
>>> TELEMETRY :phase_started   %{phase: :drain}
>>> TELEMETRY :phase_completed %{phase: :drain}
>>> TELEMETRY :phase_started   %{phase: :finalise}
>>> TELEMETRY :phase_completed %{phase: :finalise}
```

`PID1=1` is the part that matters: the BEAM holds PID 1 through
`scripts/harness-boot.sh`, so Docker's SIGTERM reaches it directly.

The trailing `:init.stop()` re-enters `prep_stop/1`, whose
`Coordinator.shutdown(:prep_stop)` lands on the coordinator's existing
`phase: :done` short-circuit — one `:requested`, one phase sequence, no second
drain. The handler's own `:requested` emit was deleted: `Coordinator.shutdown/2`
already emits it from the same reason, so with the dispatch alive every
signalled shutdown reported the event twice.

Tests that end the VM (`:init.stop()`, `halt(143)`) run in a throwaway BEAM
under `@describetag :system`; unit tests point `:signal_server` at a private
`gen_event` manager so nothing mutates the suite VM's signal handling.
