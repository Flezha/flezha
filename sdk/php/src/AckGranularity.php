<?php

declare(strict_types=1);

namespace Flezha;

/** Where an effectively-once class's consumed offset advances. */
enum AckGranularity: string
{
    case PerInput = 'per_input';
    case Checkpoint = 'checkpoint';
}
