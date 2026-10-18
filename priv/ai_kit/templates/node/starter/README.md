# Starter: a relay that counts visits per page

A small, complete Flezha project in Node, to grow into your own: one relay, one
tasklet that keeps state, and a test. `package.json` names the SDK by its
released version, the way your own project does.

| File | What it is |
|---|---|
| `count.mjs` | the tasklet `count`: keeps a page's total in state, changes it only through the change `run` records, and answers the new count |
| `starter_relay.mjs` | the relay `starter`: the entry channel `hit`, described and keyed by page, into `count`, out on `counts` |
| `runner.mjs` | the runner: serves the relay until shutdown |
| `count.test.mjs` | the test: four hits with no controller, and the replay check |

## Run it

You need a controller running and reachable, as
[Start the controller](https://github.com/flezha/flezha/blob/main/docs/public/getting_started/up_and_running.md#start-the-controller)
starts one. From this folder, install the SDK, run the test and start the
runner:

```sh
npm install
npm test
node runner.mjs
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
Start it from this folder: it names its relay by a module path.

## Make it yours

Nothing inside the files is renamed for you. Change the `name` in
`package.json`. `starter` is the name `flz relay start` takes. `count` is the
name the tasklet's state is saved under, so choose it before the first run.

## Before the SDK is published

`@flezha/sdk` is not on npm yet. From the root of a Flezha checkout, build it
once:

```sh
scripts/build-node-sdk.sh --release
```

Then, instead of `npm install`, install it from the checkout, with its absolute
path:

```sh
npm install --no-save /path/to/flezha/sdk/node
```
