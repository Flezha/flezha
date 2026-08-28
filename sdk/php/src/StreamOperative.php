<?php

declare(strict_types=1);

namespace Flezha;

use Flezha\Attribute\Operative as OperativeAttribute;
use Flezha\Graph\ClassSpec;

/** A stream operative: fed by the controller through its entry channels. */
abstract class StreamOperative extends Operative
{
    protected static function buildSpec(OperativeAttribute $declaration): ClassSpec
    {
        $builder = static::builder($declaration);

        return new ClassSpec(
            factoryId: $declaration->factoryId,
            kind: 'stream',
            persistent: $declaration->persistent,
            effectivelyOnce: $builder->effectivelyOnce,
            ackGranularity: $builder->ackGranularity,
            checkpointEvery: $builder->checkpointEvery,
            checkpointOutputs: $builder->checkpointOutputs,
            checkpointMaxDelayMs: $builder->checkpointMaxDelayMs,
            correctionWindowMs: $builder->correctionWindowMs,
            beyondWindowPolicy: $builder->beyondWindowPolicy,
            compactAfterBytes: $builder->compactAfterBytes,
            syncDurability: $builder->syncDurability,
            procedures: $builder->procedures,
            channels: $builder->channels,
        );
    }
}
