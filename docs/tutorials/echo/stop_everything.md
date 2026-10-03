# Stop everything

On this page you stop what this tutorial started, in reverse order: the
plugins, the operator, the runner, then the controller. Do it before the
tradedesk tutorial, which starts a controller of its own with authorization on.

> Disable both plugins and stop `my_echo`.
> {: .step}

```sh
$ ./bin/flz plugin disable writer
$ ./bin/flz plugin disable reader
$ ./bin/flz operator stop my_echo
```

These are the only steps that forget anything. The controller keeps enabled
plugins and started operators in a durable registry, so what you skip comes back
on its own the next time the controller and a runner start.

> Stop your runner: press Ctrl-C in its terminal.
> {: .step}

> Stop the controller with the tab that matches how you started it.
> {: .step}

<!-- tabs-open -->

### Binaries

```sh
$ ./bin/flz controller stop
```

```output
[shutdown] complete duration_ms=8 outcome=clean
```

### Docker

```sh
$ docker stop flezha-controller
```

<!-- tabs-close -->

`flz controller stop` drains the controller and writes its last changes before
it exits, so nothing is lost; a kill costs at most the last few milliseconds,
recovered on the next start.
[Deploying a controller](../../topics/basics/deploying_a_controller.md) covers
shutdown in full, and [Running operators](../../topics/basics/running_operators.md)
what comes back after a restart.

## Next

That is the echo. [The tradedesk](../tradedesk/tradedesk.md) is the next
tutorial.
