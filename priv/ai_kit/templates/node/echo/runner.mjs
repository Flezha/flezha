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

// `run()` is `start()` followed by `awaitTermination()`. Splitting them leaves
// room for the calls below, and still serves until a SIGTERM or until
// standard input closes.
await runner.start(); // connects to the controller

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
// docs/public/topics/core/sending_and_calling.md covers sending. An enforced
// controller refuses all of it until `flz role bind runner@default app default`.

// Resolves at shutdown: a SIGTERM, or standard input closing.
await runner.awaitTermination();
