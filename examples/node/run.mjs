// Run the example.
//
//   node run.mjs                        the main event loop dispatches
//   FLEZHA_NODE_WORKERS=4 node run.mjs  four worker threads dispatch
//
// Nothing in `session.mjs` changes between the two: the handler module is
// registered by specifier, so a dispatch loop imports it itself, and selecting
// which loop takes an activation is the only difference.

import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import { consoleLogger, createRunner } from '@flezha/sdk';

const here = dirname(fileURLToPath(import.meta.url));
const workers = Number(process.env.FLEZHA_NODE_WORKERS ?? 0) || 0;

const runner = createRunner(process.argv[2] || 'default')
  .logger(consoleLogger('info'))
  .defineStream(resolve(here, 'session.mjs'))
  .workers(workers);

await runner.run();
console.log(`sessions up — ${workers === 0 ? 'main-loop' : `${workers}-worker`} dispatch`);
