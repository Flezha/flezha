# Stop everything

> #### Before you start {: .info}
>
> This is the last page of Part 1, and it stops what the four before it
> started. Do it before Part 2, which starts a controller of its own with
> authorization on.

Stop things in the reverse order you started them: the plugin, the operative,
the runner, then the controller.

## The plugin and the operative

> Detach the file sink and stop `my_echo`.
> {: .step}

```sh
$ flz plugin disable writer
$ flz operative stop my_echo
```

If you did not turn `reader` off in
[Turn one off](sources_and_sinks.md#turn-one-off), disable it too:

```sh
$ flz plugin disable reader
```

**These are the only steps that forget anything.** The controller keeps enabled
plugins and started operatives in a durable registry, so if you skip them, they
come back on their own the next time the controller and your runner start. Run
them when you want that next start to be empty.

## The runner

> Stop your runner: press Ctrl-C in the terminal it is running in.
> {: .step}

An in-node Elixir runner has nothing to stop: it stops with the controller.

## The controller

> #### Why stop it instead of killing it {: .info}
>
> `flz controller stop` lets the controller finish its work properly. It stops
> taking new work, hands its operatives over to another controller while they
> keep running, if you have more than one, and writes its last changes to disk
> before it exits. No state is lost.
>
> Killing it is not a disaster either. The controller writes every change to
> disk as it goes, syncing every few milliseconds, and the next start picks up
> from there on its own. What a kill can cost is those last few milliseconds: an
> operative may come back a step behind and send a few outputs a second time.
> An operative that declares effectively-once delivery does not have that gap,
> because its outputs wait until the change behind them is on disk.

> Stop the controller with the tab below that matches the way you started it.
> {: .step}

<!-- tabs-open -->

### Binaries

```sh
$ flz controller stop
```

It quiesces, drains and finalises, and says so:

```
[shutdown] complete duration_ms=8 outcome=clean
```

Ctrl-C in `attached` mode stops it too, but at once: nothing is drained, and the
next start recovers from disk as it would after a crash.

That verb is authorization gated, so with enforcement on it refuses until you
have claimed an admin. `flzvm stop` needs only the cookie and works either way,
which is what to reach for on a controller you cannot authorize against.

### Docker

```sh
$ docker stop flezha-controller
```

### Inside an Elixir application

The controller stops with your application.

<!-- tabs-close -->

## Starting again

Start the controller and then your runner, the way
[`controller_deployment.md`](controller_deployment.md) and
[`runner_deployment.md`](runner_deployment.md) showed. Any operative or plugin
you did not stop comes back on its own.

## Next

That is Part 1. [`guides.md`](guides.md) lists what Part 2 covers,
starting at [`namespaces.md`](tradedesk/namespaces.md).
