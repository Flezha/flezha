# Echo: the Node hello world

The Node hello world, as a standalone project. Nothing here depends on the
Flezha source tree: `package.json` names one released version of `@flezha/sdk`,
the way your own project would.

Three files, one for each part the guides describe:

| File | Part | What it does |
|---|---|---|
| `echo_procedure.mjs` | the procedure | the processing stage: prefixes each message |
| `echo_operative.mjs` | the operative | the graph: `in` feeds the procedure, output lands on `out` |
| `runner.mjs` | the runner | this process: offers the operative and waits |

## Run it

You need a controller running and reachable first
([`docs/guides/controller_deployment.md`](../../../../docs/guides/controller_deployment.md)).
From this folder:

```sh
npm install
node runner.mjs
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
and defaults to `default`: `node runner.mjs my_app`.

Start it from this folder. The runner names its operative by module specifier,
`./echo_operative.mjs`, and that resolves against the working directory.

## Building from a checkout

`@flezha/sdk` is not on npm yet. From the repository root, build the SDK and
link it into this folder once:

```sh
scripts/build-node-sdk.sh --release
(cd sdk/node && npm link)
(cd sdk/node/examples/echo && npm link @flezha/sdk)
```

`npm install` in this folder then keeps the link, and `package.json` stays as it
is. Once the package is published, the step goes away.

## Where to go next

[`docs/guides/runner_deployment.md`](../../../../docs/guides/runner_deployment.md)
walks through what each of the three parts is and why the split is the way it
is. [`docs/sdk/node_sdk.md`](../../../../docs/sdk/node_sdk.md) covers the rest of
the SDK.
