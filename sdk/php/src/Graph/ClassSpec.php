<?php

declare(strict_types=1);

namespace Flezha\Graph;

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
        /** The author's explicit `checkpointAck()`; the granularity is derived. */
        public readonly bool $checkpointAck,
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

    /**
     * Whether this class's producers release their values untransformed.
     *
     * A structured workflow's stage releases ONE value per emit and the driver
     * fans it out to every consumer itself, so each consumer edge — its
     * transforms and its key — belongs to the consumer. The producer and the
     * consumer read this one predicate, so they cannot disagree about which
     * side applies the edge.
     */
    public function structuredWorkflow(): bool
    {
        return $this->kind === 'workflow' && $this->channels !== [];
    }
}
