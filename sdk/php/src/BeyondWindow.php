<?php

declare(strict_types=1);

namespace Flezha;

/** What happens to an input that arrives after its correction window closed. */
enum BeyondWindow: string
{
    case ProcessAsArrival = 'process_as_arrival';
    case Drop = 'drop';
}
