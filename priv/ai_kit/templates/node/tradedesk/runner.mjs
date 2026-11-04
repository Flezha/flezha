import { createRunner } from '@flezha/sdk';

/**
 * Starts the desk's runner: the process that connects to the controller and
 * serves tradedesk's relays.
 *
 * The namespace is the first argument and defaults to `trading`. Under an
 * enforced controller the runner proves its namespace with that namespace's
 * key, read from `FLEZHA_NAMESPACE_SECRET`.
 *
 * Registering a factory starts nothing: it offers the controller relays of
 * that type, which are started from it later. `registerFactory` is the same
 * call for both kinds, the stream that tracks prices and the steps relay
 * that places one order. A relay is registered by module specifier, which
 * resolves against the working directory, so start the runner from this
 * folder.
 */
const runner = createRunner(process.argv[2] ?? 'trading');
await runner.registerFactory('./trade_watcher_relay.mjs');
await runner.registerFactory('./order_executor_relay.mjs');

// Serves until a SIGTERM, Ctrl-C, or its standard input closing.
await runner.run();
