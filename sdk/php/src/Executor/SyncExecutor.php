<?php

declare(strict_types=1);

namespace Flezha\Executor;

use Flezha\Runner;

/**
 * The default executor: one loop, on the calling thread.
 *
 * Zero dependencies and no extension beyond `ext-ffi`, so it works on the
 * non-thread-safe build every distribution ships. It processes one activation
 * at a time, which is correct — not degraded — for a procedure body that is
 * CPU-bound or performs blocking I/O: the body runs, the activation completes,
 * and the next one is polled.
 *
 * The ceiling that follows is stated rather than discovered: throughput per
 * loop is roughly the inverse of body latency, and in-flight concurrency is
 * the loop count, which here is one per process. More cores come from more
 * processes; `SwooleExecutor` is what lifts a *single* operative past one.
 */
final class SyncExecutor implements Executor
{
    private ?LoopState $state = null;

    public function __construct(private readonly ?int $maxActivations = null)
    {
    }

    public function name(): string
    {
        return 'sync';
    }

    public function run(Runner $runner): void
    {
        $this->state = new LocalLoopState($this->maxActivations);
        $runner->serve($this->state);
    }

    public function stop(): void
    {
        // `run` is on this thread, so by the time anything can call `stop` the
        // loop is either between polls or inside a body. Setting the flag is
        // the whole of it: the loop leaves after the activation it holds.
        $this->state?->stop();
    }

    public function state(): ?LoopState
    {
        return $this->state;
    }
}
