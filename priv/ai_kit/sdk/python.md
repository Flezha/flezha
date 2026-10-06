# The python SDK's echo example

Every file of `sdk/python/examples/echo`, the runner Up and running builds. Its dependency file names the SDK package and the version this release pins.

## examples/echo/README.md

````markdown
# Echo: the Python hello world

The Python hello world, as a standalone project. Nothing here depends on the
Flezha source tree: `requirements.txt` names one released version of `flezha`,
the way your own project would. It needs Python 3.11 or later.

Three files, one for each part the guides describe:

| File | Part | What it does |
|---|---|---|
| `echo_tasklet.py` | the tasklet | the processing stage: prefixes each message |
| `echo_relay.py` | the relay | the graph: `in` feeds the tasklet, output lands on `out` |
| `runner.py` | the runner | this process: registers the echo factory and waits |

## Run it

You need a controller running and reachable first
([`docs/guides/controller_deployment.md`](../../../../docs/guides/controller_deployment.md)).
From this folder:

```sh
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python runner.py
```

Then, from another terminal:

```sh
flz relay start echo --id my_echo
flz relay send my_echo --text 'hello world' --await 2000
```

```
Delivered to 'my_echo'.
out	echo: hello world
```

It keeps running until a SIGTERM, Ctrl-C, or its standard input closing, so do
not start it with standard input closed. The namespace is the first argument
and defaults to `default`: `.venv/bin/python runner.py my_app`.

## Building from a checkout

The package is not on PyPI yet. From the package root, build a wheel for
this platform once:

```sh
pip install "maturin>=1.9,<2.0"
maturin build --release --out dist --features python-extension,python-abi3
```

`maturin build` compiles the SDK's native library, so it needs a Rust
toolchain; `scripts/build-python-sdk.sh` names what else it looks for. Then
install the wheel into this folder's environment instead of the
`requirements.txt` line:

```sh
cd examples/echo
python3 -m venv .venv
.venv/bin/pip install ../../dist/flezha-0.1.0-*.whl
```

Once the package is published, the step goes away.

## Where to go next

[`docs/guides/runner_deployment.md`](../../../../docs/guides/runner_deployment.md)
walks through what each of the three parts is and why the split is the way it
is. [`docs/sdk/python_sdk.md`](../../../../docs/sdk/python_sdk.md) covers the
rest of the SDK. [`examples/echo_deployment`](../echo_deployment)
starts a controller and this runner together with Docker.
````

## examples/echo/echo_relay.py

````python
from echo_tasklet import Echo

import flezha


# The deployable unit. A relay declares the graph a message travels: the
# entry channel it arrives on, the steps it passes through, and the channel its
# result leaves on.
#
# The graph is checked as a whole when the runner declares it, so one that
# cannot work is refused at startup. The entry names the text codec, so the
# tasklet is handed a str, and the output names none, so a str result leaves
# as its own bytes.
@flezha.relay("echo")  # the name `flz relay start` takes
def echo_relay(c: flezha.Channels) -> None:
    (
        c.input("in", codec="text")  # messages arrive on the entry channel "in"
        .into(Echo)  # each one runs through the echo tasklet
        .output("out")  # the result leaves on "out", for a plugin to pick up
    )
````

## examples/echo/echo_tasklet.py

````python
import flezha


# A processing stage of the relay's graph, run once per message. This one
# prefixes the input and emits it through the context, the one way a value
# leaves a tasklet.
#
# `Ctx[None, str]` names no state, so it keeps nothing between messages and
# journals nothing, and a str output. Where the output goes is not decided here:
# the relay decides that, so the graph can be rewired without touching this
# class.
@flezha.tasklet(name="echo")  # state is saved under this name, so choose it once
class Echo:
    def run(self, value: str, ctx: flezha.Ctx[None, str]) -> None:
        ctx.emit(f"echo: {value}")  # where it goes is the relay's business
````

## examples/echo/requirements.txt

````
flezha==0.1.0
````

## examples/echo/runner.py

````python
"""Starts a runner: a process that connects to the controller and serves your
relays.

Declare every relay you want served before `start`. One runner can serve
several, and `register_factory` is the same call for the steps kind.
"""

import sys

from echo_relay import echo_relay

import flezha


def main() -> None:
    namespace = sys.argv[1] if len(sys.argv) > 1 else "default"
    flezha.configure_logging("info")

    # the relays this runner serves
    runner = flezha.create_runner(namespace).register_factory(echo_relay)

    # `run()` is `start()` followed by `await_termination()`. Splitting them
    # leaves room for the calls below, and still blocks until shutdown.
    runner.start()  # connects to the controller

    # Everything `flz relay` and `flz plugin` do is also a method of
    # `control()`, and `send` hands a message to a relay. Uncomment to drive
    # this namespace from the application instead of from a terminal:
    #
    # control = runner.control()
    # control.start_relay("echo", id="my_echo")
    # control.enable_plugin("writer", "relay_file_output", opts={"path": "echo-out.txt"})
    # control.bind_plugin("writer", "my_echo", hooks=[["on_output", {}]])
    # runner.send("my_echo", "hello world", "in", msg_id="hello-1")
    #
    # docs/topics/advanced/sending_from_runner_code.md covers sending. An enforced
    # controller refuses all of it until `flz role bind runner@default app default`.

    # Blocks until shutdown: a SIGTERM, or standard input closing.
    runner.await_termination()


if __name__ == "__main__":
    main()
````
