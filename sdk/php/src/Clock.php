<?php

declare(strict_types=1);

namespace Flezha;

/**
 * Which clock the core evaluates per input, and therefore what `Ctx::now()`
 * serves. Evaluated once per invocation, never read from ambient time.
 */
enum Clock: string
{
    /** The runner's wall clock at delivery. */
    case Wall = 'wall';
    /** The entity's durable change sequence. */
    case Logical = 'logical';
    /** The time the procedure's own `eventTime()` extracts from the input. */
    case EventTime = 'event_time';
}
