#!/usr/bin/env node
// The Node SDK's e2e test app.
//
// Registers the reference fixture twins and runs until shutdown. The namespace
// is argv[1] (default "default"), matching rust_runner / java_runner /
// elixir_runner. Dispatch topology is an axis of the run rather than a property
// of this file: FLEZHA_NODE_WORKERS selects the single-loop default or a
// worker-thread pool, and the fixtures are identical under both.

import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import { consoleLogger, createRunner } from '@flezha/sdk';

import { armEntryIdleProbe } from './fixtures.mjs';

const namespace = process.argv[2] || 'default';
const fixtures = resolve(dirname(fileURLToPath(import.meta.url)), 'fixtures.mjs');
const workers = Number(process.env.FLEZHA_NODE_WORKERS ?? 0) || 0;

const runner = createRunner(namespace)
  .logger(consoleLogger(process.env.FLEZHA_LOG_LEVEL ?? 'info'))
  .defineStream(fixtures)
  .workers(workers);

process.on('unhandledRejection', (reason) => {
  console.error(`[node_runner] unhandled rejection: ${reason}`);
});

// The harness stops a runner by closing its pipes. A write that lands after
// that raises EPIPE on the stream, and an unhandled stream error would turn a
// clean drain into a crash the run reports as a runner failure.
for (const stream of [process.stdout, process.stderr]) {
  stream.on('error', () => {});
}

// A runner-level registration, not a declaration: armed here, beside the
// fixtures the chain above registers by specifier.
armEntryIdleProbe(runner);

await runner.run();

console.log(
  `[node_runner] namespace=${namespace} transport=${process.env.FLEZHA_TRANSPORT ?? 'uds'} ` +
    `dispatch=${workers === 0 ? 'main-loop' : `${workers} workers`}`,
);
