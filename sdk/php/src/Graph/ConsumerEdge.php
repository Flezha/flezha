<?php

declare(strict_types=1);

namespace Flezha\Graph;

/** One consumer of a channel, with the transforms and key of its own edge. */
final class ConsumerEdge
{
    /** @param list<callable(mixed):mixed> $transforms */
    public function __construct(
        public readonly int $procedure,
        public array $transforms = [],
        /** @var (callable(mixed):string)|null */
        public $keyBy = null,
    ) {
    }
}
