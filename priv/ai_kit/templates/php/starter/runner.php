<?php

declare(strict_types=1);

/**
 * The runner: connects to the controller and serves the starter relay until a
 * SIGTERM or its standard input closing.
 */

require __DIR__ . '/vendor/autoload.php';

use Flezha\Examples\Starter\StarterRelay;
use Flezha\Flezha;

Flezha::createRunner($argv[1] ?? 'default')
    ->registerFactory(StarterRelay::class)
    ->run();
