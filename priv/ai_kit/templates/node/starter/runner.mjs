import { createRunner } from '@flezha/sdk';

/**
 * The runner: connects to the controller and serves the starter relay until
 * a SIGTERM or its standard input closing. A relay is registered by module
 * specifier, resolved against the working directory.
 */
const runner = createRunner(process.argv[2] ?? 'default');
await runner.registerFactory('./starter_relay.mjs');
await runner.run();
