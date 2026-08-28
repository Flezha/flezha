<?php

declare(strict_types=1);

/**
 * The entry script of one `Swoole\Thread` of {@see \Flezha\Executor\SwooleExecutor}'s pool.
 *
 * A thread bootstraps a fresh interpreter from a script path plus scalar
 * arguments, so this is where the crossing happens: the address of the context
 * the main thread created, the budget, and the atomics the pool shares. It
 * rebuilds the PHP-side registry by requiring the runner's own entry script —
 * which is why that script must be re-enterable — and `Runner::run()` sees the
 * attachment left here and serves rather than creating a second context.
 *
 * Part of the Swoole adapter, so it names Swoole types; nothing outside the
 * adapter's own files does. Never autoloaded: it is only ever a thread's
 * script path.
 */
[$autoload, $entry, $address, $budget, $stopping, $claimed, $observed, $bodyNanos]
    = Swoole\Thread::getArguments();

require $autoload;

Flezha\Runner::attachInThread($address, new Flezha\Executor\SwooleLoopState(
    $budget === 0 ? null : $budget,
    $stopping,
    $claimed,
    $observed,
    $bodyNanos,
));

require $entry;
