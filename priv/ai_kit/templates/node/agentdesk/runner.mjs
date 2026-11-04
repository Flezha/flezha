import { createRunner } from '@flezha/sdk';

/**
 * Starts the desk's runner: the process that connects to the controller and
 * serves agentdesk's relays, in the `agents` namespace. A relay is
 * registered by module specifier, which resolves against the working
 * directory, so start the runner from this folder.
 */
const runner = createRunner('agents');
await runner.registerFactory('./agent_relay.mjs');
await runner.registerFactory('./gateway_relay.mjs');

await runner.run();
