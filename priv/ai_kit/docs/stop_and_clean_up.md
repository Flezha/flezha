# Stop and clean up

On this page you stop what the last two pages started, in reverse order: the
plugins, the relay, the runner, then the controller. Do it before the
tradedesk tutorial, which starts a controller of its own with authorization on.

> #### What you need {: .tip}
>
> [Up and running](up_and_running.md) and [Connect a plugin](connect_a_plugin.md)
> done first, with everything they started still running.

## Stop the plugins and the relay

> Disable both plugins and stop `my_echo`.
> {: .step}

```sh
$ ./bin/flz plugin disable writer
```

```output
Plugin writer disabled.
```

```sh
$ ./bin/flz plugin disable reader
```

```output
Plugin reader disabled.
```

```sh
$ ./bin/flz relay stop my_echo
```

```output
Stopping relay...
Relay has been stopped
Relay stopped successfully.
```

Disabling a plugin stops it and pauses its bindings: a relay still bound to it
reads healthy, and `flz relay diagnose` names the plugin as disabled. An unbound
plugin stays enabled. With the relay stopped, nothing is left in the namespace:

```sh
$ ./bin/flz plugin list
```

```output
No plugins enabled.
```

```sh
$ ./bin/flz relay list
```

```output
Fetching relays for namespace 'default'...
No relays found for namespace 'default'.
```

These are the only steps that forget anything. The controller keeps enabled
plugins and started relays in a durable registry, so what you skip comes back
on its own the next time the controller and a runner start.

## Stop the runner

> Stop your runner: press Ctrl-C in its terminal.
> {: .step}

With `my_echo` stopped, nothing runs on the runner, so nothing waits for it.
A runner that stops while a relay still runs on it does not take the relay
with it: the controller binds the relay to the next runner that registers its
factory, and it carries on from the state the controller kept
([Durability](../topics/core/durability.md)).

## Stop the controller

> Stop the controller with the tab that matches how you started it.
> {: .step}

<!-- tabs-open -->

### Binaries

```sh
$ ./bin/flz controller stop
```

```output
Stopping flezha controller...
[shutdown] phase=quiesce started
[shutdown] phase=quiesce completed duration_ms=1
[shutdown] phase=drain started
[shutdown] phase=drain completed duration_ms=3
[shutdown] phase=finalise started
[shutdown] phase=finalise completed duration_ms=2
[shutdown] complete duration_ms=7 outcome=clean
```

### Docker

```sh
$ docker stop flezha-controller
```

`docker stop` runs the same `flz controller stop` inside the container.

<!-- tabs-close -->

The stop runs in three phases:

1. **Quiesce.** The controller takes on nothing new: a relay start is refused,
   and so is a new runner connection.
2. **Drain.** Each relay it still runs moves to another controller. With no
   other controller, nothing moves, and the next start brings the relays back
   from disk.
3. **Finalise.** Pending disk writes are flushed and the connections close.

So nothing is lost; a kill instead costs at most the last few milliseconds,
recovered on the next start.
[Deploying a controller](../topics/deploying/deploying_a_controller.md#graceful-shutdown)
covers shutdown in full, and [Running and testing](../topics/core/running_and_testing.md)
what comes back after a restart.

## Clean up

The controller's data stays on disk. To start the next one empty, clear it
once the controller is stopped:

<!-- tabs-open -->

### Binaries

See what would go first. `--dry-run` touches nothing:

```sh
$ ./bin/flz reset --dry-run
```

```output
would clear ./storage/app/state/                             (12 files, 4.3 MiB)
would clear ./storage/app/process_hub/                       (2 files, 16.0 KiB)
would keep  ./storage/app/certs/                             (use --include-certs to clear)
would keep  ./storage/app/dlq/                               (use --include-dlq to clear)
would keep  ./storage/app/schedules/                         (use --include-schedules to clear)
would keep  ./storage/app/auth/                              (use --include-auth to clear)
would keep  ./storage/app/audit/                             (use --include-audit to clear)
would clear ./storage/app/relay_history/                     (3 files, 12.4 KiB)
would keep  ./storage/app/plugins/                           (use --include-plugins to clear)
would keep  socket directory                                 (use --include-sockets to clear)
would keep  ./storage/logs/                                  (not reset's to clear)
reset complete in 0 ms
```

Then clear it, and remove the two files the file plugins used:

```sh
$ ./bin/flz reset --yes
$ rm storage/app/files/echo-in.txt storage/app/files/echo-out.txt
```

By default `flz reset` clears each relay's saved state and the registry of
started relays and enabled plugins. Tokens, schedules, installed plugins and
the rest stay unless an `--include-` flag names them, and it clears nothing
while a controller is running
([Start from a clean slate](../topics/core/running_and_testing.md#start-from-a-clean-slate-flz-reset)).
The files the file plugins wrote are yours, so it leaves them.

### Docker

```sh
$ docker rm flezha-controller
$ docker volume rm flezha_data
```

The volume holds everything the controller kept, the two files included.

<!-- tabs-close -->
