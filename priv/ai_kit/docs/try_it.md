# Try it

Before you build anything, see Flezha work: a controller, a runner that can run
an `echo` relay, and a message that goes in and comes back. Everything runs
in Docker, so all this page needs is Docker with Compose and the Flezha
repository.

## Run the echo

From the root of the repository, bring up the controller and the runner, start
a relay from the `echo` factory, and send it a message:

```sh
$ cd sdk/python/examples/echo_deployment
$ docker compose up -d --wait
$ docker compose exec controller /flezha/bin/flz relay start echo --id my_echo
$ docker compose exec controller /flezha/bin/flz relay send my_echo --text 'hello world' --await 2000
```

```output
Delivered to 'my_echo'.
out	echo: hello world
```

The first line says the controller accepted the message. The second is what
came back: the channel it left on, a tab, and what the relay sent out.

The runner here is written in Python, but it could be written in any of the
seven languages without the controller or the commands changing.
[`sdk/python/examples/echo_deployment`](https://github.com/flezha/flezha_python_sdk/tree/main/examples/echo_deployment)
is the whole deployment.

It is a demo, not a deployment: it makes every choice for you. [Up and
running](../tutorials/echo/first_steps.md) builds the same `echo` by hand, one piece at a time, with a controller
of its own, so take this one down first:

```sh
$ docker compose down
```

## Or without a controller

[Local mode](../topics/basics/local_mode.md) runs a relay inside your own
process, with no controller and no Docker. Nothing in it is durable, so it is
the way to try a relay and to test one, not to run it.

## Next

[Up and running](../tutorials/echo/first_steps.md) builds the same echo by hand,
page by page. The [tutorials](tutorials/tutorials.html) then build larger
examples on it.
