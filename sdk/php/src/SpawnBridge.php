<?php

declare(strict_types=1);

namespace Flezha;

/**
 * The one effect a `Ctx` cannot compute: asking the controller to spawn or
 * deliver to a workflow instance.
 *
 * An interface rather than the core itself, so the invocation context depends
 * on what it needs rather than on the FFI layer — which is also what lets the
 * test harness drive the real `Ctx` and the real dispatch path without a
 * controller behind them.
 */
interface SpawnBridge
{
    /** Answers the controller's outcome (`spawned` | `delivered` | ...). */
    public function spawnWorkflow(
        string $instanceId,
        string $factoryId,
        string $payload,
        string $ifRunning,
    ): string;
}
