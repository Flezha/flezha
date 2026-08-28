<?php

declare(strict_types=1);

namespace Flezha\Executor;

use Flezha\Runner;

/**
 * The concurrency seam: how many loops run, and nothing else.
 *
 * Two methods, and deliberately not `submit(callable $activation)`. A
 * `Swoole\Thread` is constructed from a script path plus scalar arguments and
 * bootstraps a fresh interpreter — closures, objects and resources do not
 * cross the boundary — so anything shaped as "hand this callable to a thread"
 * has exactly one possible implementation, the inline one. Loop multiplicity
 * is expressible under every PHP threading model; work distribution is not.
 *
 * **Safety comes from the core, not the adapter.** `PollCore` holds per-entity
 * exclusivity and will never hand out two activations for the same entity
 * concurrently, however many threads poll it, so no executor can produce a
 * race in user state. An executor can affect throughput and latency; it cannot
 * affect correctness. That is what makes it safe to ship two and to let a user
 * write a third.
 */
interface Executor
{
    /** A name for the startup line. */
    public function name(): string;

    /** Drive the runner's poll loop to completion. MAY drive it on N threads. */
    public function run(Runner $runner): void;

    /**
     * Ask every loop to finish its current activation and return.
     *
     * Returns only once every loop has left user code: `flz_core_shutdown` must
     * not be called before it does.
     */
    public function stop(): void;

    /**
     * What every loop of this executor shared, for the runner's shutdown line;
     * null before {@see run()}.
     *
     * On the interface rather than on the adapter, so the runner reports the
     * sizing arithmetic without naming which executor produced it.
     */
    public function state(): ?LoopState;
}
