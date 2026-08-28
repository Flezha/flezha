<?php

declare(strict_types=1);

namespace Flezha;

/** When a checkpoint-ack class's outputs leave the runner. */
enum CheckpointOutputs: string
{
    case AsProduced = 'as_produced';
    case AtCheckpoint = 'at_checkpoint';
}
