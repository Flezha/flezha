<?php

declare(strict_types=1);

namespace Flezha;

/**
 * What a `flatMap` yields: several values where one arrived.
 *
 * Each is delivered independently — through the rest of the chain, and then to
 * the consumer — so `flatMap` followed by `keyBy` is how one input reaches
 * several entities.
 */
final class Fanned
{
    /** @param list<mixed> $values */
    public function __construct(public readonly array $values)
    {
    }
}
