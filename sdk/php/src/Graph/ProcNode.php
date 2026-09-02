<?php

declare(strict_types=1);

namespace Flezha\Graph;

use Flezha\Definition;

/** A procedure as the graph holds it, with its position in the declaration. */
final class ProcNode
{
    /** @param class-string $class */
    public function __construct(
        public readonly int $index,
        public readonly string $name,
        public readonly string $class,
        public readonly Definition $definition,
        /** The one channel this node produces on, created on first wiring. */
        public ?ChannelSpec $output = null,
    ) {
    }
}
