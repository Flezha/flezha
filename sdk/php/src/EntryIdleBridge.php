<?php

declare(strict_types=1);

namespace Flezha;

/**
 * The core calls idle observation makes: the two clock reads and the three
 * watch calls, all of which take no context. {@see Ffi\Core} implements it; a
 * test scripts it.
 */
interface EntryIdleBridge
{
    /** The core's monotonic clock, in nanoseconds since a process origin. */
    public function monotonicNanos(): int;

    /** When entry channel `$channel` last received a frame, on that clock; 0 when never. */
    public function entryLastDeliveryNanos(string $channel): int;

    /** Register an episode latch over `$channel` in the core; the id, or 0 when the name is refused. */
    public function idleWatchNew(string $channel, int $thresholdNanos): int;

    /**
     * Advance a watch's latch to now.
     *
     * @return array{int, int} the ABI's status and the episode's gap in nanoseconds
     */
    public function idleWatchPoll(int $id): array;

    /** Drop a watch. */
    public function idleWatchFree(int $id): void;
}
