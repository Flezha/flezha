<?php

declare(strict_types=1);

namespace Flezha;

/** One value a graph released on a declared sink, in local mode. */
final class SinkOutput
{
    /**
     * @param array<string, string> $metadata what a deployed run would have
     *                                        put on the wire, `flz.*` stamps included
     */
    public function __construct(
        public readonly string $channel,
        public readonly string $payload,
        public readonly array $metadata,
    ) {
    }
}
