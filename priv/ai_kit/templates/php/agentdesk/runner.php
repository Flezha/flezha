<?php

declare(strict_types=1);

/**
 * Starts the desk's runner: the process that connects to the controller and serves agentdesk's
 * relays, in the `agents` namespace.
 */

require __DIR__ . '/vendor/autoload.php';

use Flezha\Examples\Agentdesk\AgentRelay;
use Flezha\Examples\Agentdesk\GatewayRelay;
use Flezha\Flezha;

$runner = Flezha::createRunner('agents')
    ->registerFactory(AgentRelay::class)
    ->registerFactory(GatewayRelay::class)
    ->start();

$runner->awaitTermination();
