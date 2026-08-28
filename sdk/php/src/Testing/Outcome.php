<?php

declare(strict_types=1);

namespace Flezha\Testing;

/** What one invocation produced, as the author's test reads it. */
final class Outcome
{
    /**
     * @param list<mixed>                 $outputs
     * @param list<mixed>                 $changes
     * @param list<array<string, mixed>>  $timers
     */
    public function __construct(
        public readonly array $outputs,
        public readonly array $changes,
        public readonly array $timers,
        public readonly mixed $returned,
    ) {
    }
}
