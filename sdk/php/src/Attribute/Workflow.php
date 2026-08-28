<?php

declare(strict_types=1);

namespace Flezha\Attribute;

use Attribute;
use Flezha\LoopMode;
use Flezha\RollbackMode;

/**
 * The workflow driver's settings for one class. Read only from a
 * {@see \Flezha\WorkflowOperative}; a stream class that carries it is refused.
 */
#[Attribute(Attribute::TARGET_CLASS)]
final class Workflow
{
    public function __construct(
        public readonly LoopMode $loopMode = LoopMode::Once,
        public readonly RollbackMode $rollbackMode = RollbackMode::AllOrNothing,
        public readonly ?int $retryAttempts = null,
        public readonly ?int $retryBackoffMs = null,
        public readonly ?int $barrierRetryAttempts = null,
        public readonly ?int $maxDurationMs = null,
        public readonly ?int $wakeAfterMs = null,
        public readonly bool $syncDurability = false,
    ) {
    }
}
