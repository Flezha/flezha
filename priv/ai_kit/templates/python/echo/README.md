# Echo: the Python hello world

The Python hello world, as a standalone project. Nothing here depends on the
Flezha source tree: `requirements.txt` names one released version of `flezha`,
the way your own project would. It needs Python 3.11 or later.

Three files, one for each of the [building blocks](https://github.com/flezha/flezha/blob/main/docs/public/topics/core/building_blocks.md):

| File | Part | What it does |
|---|---|---|
| `echo_tasklet.py` | the tasklet | the processing stage: prefixes each message |
| `echo_relay.py` | the relay | the graph: `in` feeds the tasklet, output lands on `out` |
| `runner.py` | the runner | this process: registers the echo factory and waits |

## Run it

You need a controller running and reachable first
([Up and running](https://github.com/flezha/flezha/blob/main/docs/public/getting_started/up_and_running.md)).
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

To run the example against a Flezha checkout instead of the published package,
build a wheel for this platform once, from the package root:

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
.venv/bin/pip install ../../dist/flezha-0.1.1-*.whl
```

## Where to go next

[Building blocks](https://github.com/flezha/flezha/blob/main/docs/public/topics/core/building_blocks.md)
walks through what each of the three parts is and why the split is the way it
is. The [Python SDK guide](https://github.com/flezha/flezha/blob/main/docs/internal/sdk/python_sdk.md) covers the
rest of the SDK. [`examples/echo_deployment`](../echo_deployment)
starts a controller and this runner together with Docker.
