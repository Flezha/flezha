<?php

declare(strict_types=1);

namespace Flezha;

/**
 * A fired timer, as `onTimer` receives it. `Ctx::now()` inside the hook equals
 * `scheduledAt`, so a chained `timerAfter` anchors on the firing that woke it
 * and is replay-stable.
 */
final class Timer
{
    public function __construct(
        /** `event_time` | `processing_time` */
        public readonly string $kind,
        public readonly int $scheduledAt,
    ) {
    }
}
