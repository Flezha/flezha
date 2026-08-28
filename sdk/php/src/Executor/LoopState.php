<?php

declare(strict_types=1);

namespace Flezha\Executor;

/**
 * What every loop of one runner shares: whether to stop, how much of the
 * activation budget is left, and the two numbers the shutdown line reports.
 *
 * One collaborator rather than three callables, because all four questions
 * have the same answer to the same problem — under `SyncExecutor` the state is
 * three integers on one thread, and under `SwooleExecutor` it is the same
 * three in shared atomics. The loop asks; it never knows which.
 */
interface LoopState
{
    /** Whether every loop should leave after its current activation. */
    public function stopping(): bool;

    /** Ask every loop to stop. */
    public function stop(): void;

    /**
     * Claim one activation against the budget.
     *
     * `false` means the budget is spent and this loop should shut the runner
     * down cleanly — counted across every loop, so N threads share one budget.
     */
    public function claim(): bool;

    /** Record one activation's procedure-body time, in nanoseconds. */
    public function observe(int $bodyNanos): void;

    /**
     * `[activations, mean body nanoseconds]` — the sizing arithmetic, measured
     * rather than assumed.
     *
     * @return array{int, int}
     */
    public function report(): array;
}
