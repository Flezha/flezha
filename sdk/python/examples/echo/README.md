# Echo: the Python hello world

The Python hello world, as a standalone project. Nothing here depends on the
Flezha source tree: `requirements.txt` names one released version of `flezha`,
the way your own project would. It needs Python 3.11 or later.

Three files, one for each part the guides describe:

| File | Part | What it does |
|---|---|---|
| `echo_procedure.py` | the procedure | the processing stage: prefixes each message |
| `echo_operative.py` | the operative | the graph: `in` feeds the procedure, output lands on `out` |
| `runner.py` | the runner | this process: offers the operative and waits |

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
flz operative start my_echo echo
flz operative send my_echo --text 'hello world' --await 2000
```

```
Delivered to 'my_echo'.
out	echo: hello world
```

It keeps running until a SIGTERM, Ctrl-C, or its standard input closing, so do
not start it with standard input closed. The namespace is the first argument
and defaults to `default`: `.venv/bin/python runner.py my_app`.

## Building from a checkout

The package is not on PyPI yet. From the repository root, build a wheel for
this platform once:

```sh
cd sdk/python
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
rest of the SDK. [`examples/python/echo`](../../../../examples/python/echo)
starts a controller and this runner together with Docker.
