<?php

declare(strict_types=1);

/**
 * The PHP SDK's e2e test app.
 *
 * Registers the reference fixture twins and runs until shutdown. The namespace
 * is $argv[1] (default "default"), matching rust_runner / java_runner /
 * node_runner / ruby_runner / elixir_runner. The executor is an axis of the
 * run rather than a property of this file: FLEZHA_PHP_EXECUTOR selects the
 * synchronous loop (the default) or the Swoole thread pool, and the fixtures
 * are identical under both.
 *
 * The script must be re-enterable: a `SwooleExecutor` worker thread bootstraps
 * it to rebuild the PHP-side registry, and `Runner::run()` attaches to the
 * pool's context rather than creating a second one.
 *
 * Which is also why it carries no `#!` line. A shebang is stripped only from
 * the script PHP was invoked with; a worker `require`s this file, where the
 * line is raw output before `<?php` and `declare(strict_types=1)` is then no
 * longer the first statement — a fatal error, in a fresh interpreter, that
 * kills every worker before it polls once.
 */

require __DIR__ . '/../../../sdk/php/vendor/autoload.php';
require __DIR__ . '/fixtures.php';
require __DIR__ . '/fixtures_state.php';
require __DIR__ . '/fixtures_probe.php';
require __DIR__ . '/fixtures_workflow.php';
require __DIR__ . '/fixtures_workflow_loop.php';

use Flezha\Flezha;
use Flezha\Logger;
use Flezha\TestApp\EntryIdleProbeCounters;
use Flezha\TestApp\Fixtures;
use Flezha\TestApp\WorkflowFixtures;

$namespace = $argv[1] ?? 'default';

$runner = Flezha::createRunner($namespace)
    ->logger(new Logger((string) (getenv('FLEZHA_LOG_LEVEL') ?: 'info')));

foreach (Fixtures::all() as $operative) {
    $runner->defineStream($operative);
}
// The same registration, under the name that says which kind it is.
foreach (WorkflowFixtures::all() as $operative) {
    $runner->defineWorkflow($operative);
}
// A runner-level registration, not a declaration: armed beside the fixtures.
EntryIdleProbeCounters::arm($runner);

// The configuration this run is under, before it starts: readiness is the
// SDK's own startup line, which reports the executor.
fwrite(STDERR, sprintf(
    "[php_runner] namespace=%s transport=%s executor=%s\n",
    $namespace,
    getenv('FLEZHA_TRANSPORT') ?: 'uds',
    getenv('FLEZHA_PHP_EXECUTOR') ?: 'sync',
));

$runner->run();
