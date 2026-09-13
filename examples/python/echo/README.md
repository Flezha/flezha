# Echo: the smallest complete deployment

A controller, one runner offering an `echo` operative, and a message that goes
in and comes back. Three files: the example itself is `echo.py`, and it is
about forty lines including the comments.

This is the same `echo` the guides build by hand in
[`docs/guides/runner_deployment.md`](../../../docs/guides/runner_deployment.md).
Running it here first is the fast way to see the pieces move; the guide is
where you learn what each one is.

## With Docker

```sh
docker compose up -d --wait
```

That builds the runner image and waits for the controller to report ready.
Then start an operative from the declaration the runner offers, and send it
something:

```sh
docker compose exec controller /flezha/bin/flz operative start my_echo echo
docker compose exec controller /flezha/bin/flz operative send my_echo --text hello --await 2000
```

```
Delivered to 'my_echo'.
out	echo: hello
```

The first line is the delivery verdict, which only says the payload was
accepted. The second is the output: the channel name, a tab, and what the
procedure emitted.

To watch instead of asking, in one terminal:

```sh
docker compose exec controller /flezha/bin/flz operative listen my_echo
```

and send from another. When you are done:

```sh
docker compose down -v
```

**The first build compiles the SDK's native extension from source, so it takes
a few minutes.** The `flezha` package is not on PyPI yet; once it is, the
build stage in `Dockerfile` is deleted and replaced by one `pip install` line,
and the first run becomes immediate.

## Without Docker

Against a controller you are already running, with the Python SDK built
(`scripts/build-python-sdk.sh`):

```sh
sdk/python/.venv/bin/python examples/python/echo/echo.py
```

Then the same `flz operative start` and `flz operative send` as above.

**Do not background it with its standard input closed.** The SDK treats end of
file on standard input as a shutdown request, which is how a supervising
harness stops a runner, so a runner started with no stdin exits immediately.
That is also why the compose file sets `stdin_open: true`.

## What the three parts are

| File | Part | What it does |
|---|---|---|
| `echo.py` → `echo` | the procedure | your code: one message in, one message out |
| `echo.py` → `echo_operative` | the operative | the graph: `in` feeds the procedure, output lands on `out` |
| `echo.py` → `main` | the runner | this process: offers the operative and waits |

The procedure never says where its output goes and the operative holds no
logic. That split is what lets the graph be rewired without touching the code
that runs.
