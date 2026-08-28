<?php

declare(strict_types=1);

namespace Flezha\Graph;

use Flezha\AckGranularity;
use Flezha\Attribute\Workflow;
use Flezha\BeyondWindow;
use Flezha\CheckpointOutputs;

/** Everything the graph knows about one declared class. */
final class ClassSpec
{
    /**
     * @param list<ProcNode>    $procedures
     * @param list<ChannelSpec> $channels
     * @param (callable(mixed, \Flezha\WorkflowCtx):mixed)|null $run
     */
    public function __construct(
        public readonly string $factoryId,
        /** `stream` | `workflow` */
        public readonly string $kind,
        public readonly bool $persistent,
        public readonly bool $effectivelyOnce,
        public readonly AckGranularity $ackGranularity,
        public readonly ?int $checkpointEvery,
        public readonly CheckpointOutputs $checkpointOutputs,
        public readonly ?int $checkpointMaxDelayMs,
        public readonly ?int $correctionWindowMs,
        public readonly BeyondWindow $beyondWindowPolicy,
        public readonly ?int $compactAfterBytes,
        public readonly bool $syncDurability,
        public readonly array $procedures,
        public readonly array $channels,
        public readonly ?Workflow $workflow = null,
        public $run = null,
    ) {
    }
}
