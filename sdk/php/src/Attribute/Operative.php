<?php

declare(strict_types=1);

namespace Flezha\Attribute;

use Attribute;

/**
 * The factory id an operative class registers under, and whether its change
 * bytes reach the per-entity WAL.
 *
 * Deliberately not inherited: two operatives cannot share a factory id, and a
 * subclass silently taking its parent's would be refused by the core at
 * declare time rather than here. PHP attributes are not inherited by default,
 * which is the behaviour this wants.
 */
#[Attribute(Attribute::TARGET_CLASS)]
final class Operative
{
    public function __construct(
        public readonly string $factoryId,
        public readonly bool $persistent = true,
    ) {
    }
}
