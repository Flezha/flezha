# Starter: a relay that counts visits per page

A small, complete Flezha project in Python, to grow into your own: one relay,
one tasklet that keeps state, and a test. `requirements.txt` names the SDK by
its released version, the way your own project does. It needs Python 3.11 or
later.

| File | What it is |
|---|---|
| `count.py` | the tasklet `count`: keeps a page's total in state, changes it only through the `Counted` change, and answers the new count |
| `starter_relay.py` | the relay `starter`: the entry channel `hit`, described and keyed by page, into `count`, out on `counts` |
| `runner.py` | the runner: serves the relay until shutdown |
| `test_starter.py` | the test: four hits with no controller, and the replay check |

## Run it

You need a controller running and reachable, as
[Start the controller](https://github.com/flezha/flezha/blob/main/docs/public/getting_started/up_and_running.md#start-the-controller)
starts one. From this folder, install the SDK, run the test and start the
runner:

```sh
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python -m unittest
.venv/bin/python runner.py
```

Then, from another terminal, start the relay and call it:

```sh
flz relay start starter --id starter
flz relay call starter --channel hit --arg page=home
```

```
{"page":"home","total":1}
```

Each call for the same page answers the next total. `flz` is the release's
`bin/flz`. The runner keeps running until a SIGTERM, Ctrl-C or its standard
input closing. The namespace is its first argument and defaults to `default`.

## Make it yours

Nothing inside the files is renamed for you. `starter` is the name
`flz relay start` takes. `count` is the name the tasklet's state is saved under,
so choose it before the first run.

## Building from a checkout

To run the project against a Flezha checkout instead of the published package,
build a wheel for this platform once in the checkout's `sdk/python` (it needs a
Rust toolchain):

```sh
pip install "maturin>=1.9,<2.0"
maturin build --release --out dist --features python-extension,python-abi3
```

Then install that wheel into this folder's environment instead of the
`requirements.txt` line, with the checkout's absolute path:

```sh
.venv/bin/pip install /path/to/flezha/sdk/python/dist/flezha-0.1.1-*.whl
```
