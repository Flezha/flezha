<?php

declare(strict_types=1);

namespace Flezha;

/** How many times a workflow instance's chain runs. */
enum LoopMode: string
{
    case Once = 'once';
    case PerInput = 'per_input';
    case Chained = 'chained';
}
