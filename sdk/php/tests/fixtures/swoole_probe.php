<?php

declare(strict_types=1);

/**
 * A `Swoole\Thread` body that attaches to a context another thread created and
 * reports what it saw through the shared atomics.
 *
 * The whole of the thread-specific surface is the per-thread FFI handle
 * ({@see \Flezha\Ffi\Binding}), so this is what proves it is genuinely
 * per-thread rather than a process-wide singleton that happens to work: a
 * fresh interpreter with no handle at all resolves its own and reaches the
 * same core.
 */
[$autoload, $address, $schema, $claims, $budget] = Swoole\Thread::getArguments();

require $autoload;

$core = Flezha\Ffi\Core::attach($address);
$schema->set($core->coreSchemaVersion());

$state = new Flezha\Executor\SwooleLoopState($budget, new Swoole\Thread\Atomic(0), $claims);
for ($i = 0; $i < 10; $i++) {
    $state->claim();
}

// Deliberately NOT freed: a thread that attached never frees what it borrowed,
// and the creating thread frees it only after every worker is joined.
