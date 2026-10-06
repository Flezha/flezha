# The node SDK's echo example

Every file of `sdk/node/examples/echo`, the runner Up and running builds. Its dependency file names the SDK package and the version this release pins.

## examples/echo/README.md

````markdown
# Echo: the Node hello world

The Node hello world, as a standalone project. Nothing here depends on the
Flezha source tree: `package.json` names one released version of `@flezha/sdk`,
the way your own project would.

Three files, one for each part the guides describe:

| File | Part | What it does |
|---|---|---|
| `echo_tasklet.mjs` | the tasklet | the processing stage: prefixes each message |
| `echo_relay.mjs` | the relay | the graph: `in` feeds the tasklet, output lands on `out` |
| `runner.mjs` | the runner | this process: registers the echo factory and waits |

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
````

## examples/echo/echo_relay.mjs

````javascript
import { defineRelay } from '@flezha/sdk';

import { EchoTasklet } from './echo_tasklet.mjs';

/**
 * The deployable unit. A relay declares the graph a message travels: the
 * entry channel it arrives on, the steps it passes through, and the channel its
 * result leaves on.
 *
 * `factoryId` is the relay's stable identity, and the name
 * `flz relay start` takes. The graph is checked as a whole when the runner
 * declares it, so one that cannot work is refused at startup.
 *
 * The entry names the text codec, so what `flz relay send --text` sends
 * arrives as a string. The output names no codec, and an output sends a string as its
 * UTF-8 bytes, so the output is the text itself.
 */
export default defineRelay({
  factoryId: 'echo', // the name `flz relay start` takes
  channels: (c) => {
    c.input('in', { codec: 'text' }) // messages arrive on the entry channel "in"
      .into(EchoTasklet) // each one runs through the echo tasklet
      .output('out'); // the result leaves on "out", for a plugin to pick up
  },
});
````

## examples/echo/echo_tasklet.mjs

````javascript
import { defineTasklet } from '@flezha/sdk';

/**
 * A processing stage of the relay's graph, run once per message. This one
 * prefixes the input and emits the result through its context, `ctx.emit`.
 *
 * `name` is the tasklet's stable identity. State is filed under it, so
 * renaming it later declares a different tasklet rather than renaming this
 * one. With no `initState` and no `applyChange`, nothing is kept between
 * messages and nothing is journaled.
 *
 * Where the result goes is not decided here. The relay decides that, so the
 * graph can be rewired without touching this tasklet.
 */
export const EchoTasklet = defineTasklet({
  name: 'echo', // state is saved under this name, so choose it once
  run: (input, ctx) => ctx.emit(`echo: ${input}`), // where it goes is the relay's business
});
````

## examples/echo/package.json

````json
{
  "name": "flezha-echo-example",
  "version": "0.1.0",
  "private": true,
  "type": "module",
  "description": "One message in, one message out: a tasklet, a relay and a runner.",
  "scripts": {
    "start": "node runner.mjs"
  },
  "dependencies": {
    "@flezha/sdk": "0.1.0"
  }
}
````

## examples/echo/runner.mjs

````javascript
import { createRunner } from '@flezha/sdk';

/**
 * Starts a runner: a process that connects to the controller and serves your
 * relays.
 *
 * A relay is registered by module specifier rather than as an imported
 * value, because every dispatch loop imports the module itself. That is what
 * lets `.workers(n)` move dispatch onto worker threads without a rewrite.
 * Registering therefore imports, which is asynchronous, so each one is its own
 * `await` and a module that fails to load says so at the line that names it.
 */
const namespace = process.argv[2] ?? 'default';

const runner = createRunner(namespace);
await runner.registerFactory('./echo_relay.mjs'); // the relays this runner serves

// Resolves once the runner is connected and serving. The process then keeps
// running until a SIGTERM, or until standard input closes.
await runner.run(); // connects to the controller

// Everything `flz relay` and `flz plugin` do is also a method of
// `control()`, and `send` hands a message to a relay. Uncomment to drive
// this namespace from the application instead of from a terminal:
//
// const control = runner.control();
// await control.startRelay('echo', { id: 'my_echo' });
// await control.enablePlugin('writer', 'relay_file_output', { opts: { path: 'echo-out.txt' } });
// await control.bindPlugin('writer', 'my_echo', { hooks: [['on_output', {}]] });
// await runner.send('my_echo', 'hello world', 'in', { msgId: 'hello-1' });
//
// docs/topics/advanced/sending_from_runner_code.md covers sending. An enforced
// controller refuses all of it until `flz role bind runner@default app default`.
````
