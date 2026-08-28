<?php

declare(strict_types=1);

namespace Flezha\Executor;

/**
 * {@see LoopState} for a single-interpreter executor: three integers, no
 * synchronisation, because there is exactly one loop reading them.
 */
final class LocalLoopState implements LoopState
{
    private bool $stopping = false;
    private int $claimed = 0;
    private int $observed = 0;
    private int $bodyNanos = 0;

    /** @param int|null $budget activations before a clean shutdown; null = unlimited */
    public function __construct(private readonly ?int $budget = null)
    {
    }

    public function stopping(): bool
    {
        return $this->stopping;
    }

    public function stop(): void
    {
        $this->stopping = true;
    }

    public function claim(): bool
    {
        if ($this->budget === null) {
            return true;
        }

        return ++$this->claimed <= $this->budget;
    }

    public function observe(int $bodyNanos): void
    {
        $this->observed++;
        $this->bodyNanos += $bodyNanos;
    }

    public function report(): array
    {
        return [$this->observed, $this->observed === 0 ? 0 : intdiv($this->bodyNanos, $this->observed)];
    }
}
