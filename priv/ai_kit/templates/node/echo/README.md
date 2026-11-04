# Echo: the Node hello world

The Node hello world, as a standalone project. Nothing here depends on the
Flezha source tree: `package.json` names one released version of `@flezha/sdk`,
the way your own project would.

Three files, one for each of the [building blocks](https://github.com/flezha/flezha/blob/main/docs/public/topics/core/building_blocks.md):

| File | Part | What it does |
|---|---|---|
| `echo_tasklet.mjs` | the tasklet | the processing stage: prefixes each message |
| `echo_relay.mjs` | the relay | the graph: `in` feeds the tasklet, output lands on `out` |
| `runner.mjs` | the runner | this process: registers the echo factory and waits |

## Run it

You need a controller running and reachable first
([Up and running](https://github.com/flezha/flezha/blob/main/docs/public/getting_started/up_and_running.md)).
From this folder:

```sh
npm install
node runner.mjs
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
and defaults to `default`: `node runner.mjs my_app`.

Start it from this folder. The runner names its relay by module specifier,
`./echo_relay.mjs`, and that resolves against the working directory.

## Building from a checkout

To run the example against a Flezha checkout instead of the published package, build
the SDK from the repository root and link it into this folder once:

```sh
scripts/build-node-sdk.sh --release
(cd sdk/node && npm link)
(cd sdk/node/examples/echo && npm link @flezha/sdk)
```

`npm install` in this folder then keeps the link, and `package.json` stays as it
is.

## Where to go next

[Building blocks](https://github.com/flezha/flezha/blob/main/docs/public/topics/core/building_blocks.md)
walks through what each of the three parts is and why the split is the way it
is. The [Node SDK guide](https://github.com/flezha/flezha/blob/main/docs/internal/sdk/node_sdk.md) covers the rest of
the SDK.
