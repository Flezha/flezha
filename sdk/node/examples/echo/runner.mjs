import { createRunner } from '@flezha/sdk';

/**
 * Starts a runner: a process that connects to the controller and serves your
 * operatives.
 *
 * An operative is registered by module specifier rather than as an imported
 * value, because every dispatch loop imports the module itself. That is what
 * lets `.workers(n)` move dispatch onto worker threads without a rewrite.
 */
const namespace = process.argv[2] ?? 'default';

const runner = createRunner(namespace).defineStream('./echo_operative.mjs');

// Resolves once the runner is connected and serving. The process then keeps
// running until a SIGTERM, or until standard input closes.
await runner.run();

// Everything `flz operative` and `flz plugin` do is also a method here.
// Uncomment to drive this namespace from the application instead of from a
// terminal:
//
// const admin = runner.admin();
// await admin.startOperative({ id: 'my_echo', factory_id: 'echo' });
// await admin.enablePlugin('writer', {
//   id: 'writer',
//   provider: 'operative_file_sink',
//   operative_id: 'my_echo',
//   opts: { path: 'echo-out.txt' },
//   hooks: [['on_output', {}]],
// });
// await admin.send('my_echo', 'hello world', 'in');
//
// docs/sdk/admin.md has the whole surface. An enforced controller refuses all
// of it until `flz role bind runner@default app default`.
