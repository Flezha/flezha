<?php

declare(strict_types=1);

namespace Flezha;

/**
 * What a `filter` yields for a value it rejects.
 *
 * A dropped value is not `null` or `false` — both are values a handler may
 * legitimately carry — so the marker is an object nothing else produces. It
 * stops the transform chain: the item is consumed without reaching a handler,
 * and at the prepare crossing it answers no key, which is how the core learns
 * not to route it.
 */
final class Dropped
{
    private static ?self $instance = null;

    private function __construct()
    {
    }

    public static function get(): self
    {
        return self::$instance ??= new self();
    }

    public static function is(mixed $value): bool
    {
        return $value instanceof self;
    }
}
