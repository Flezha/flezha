<?php

declare(strict_types=1);

namespace Flezha;

/**
 * An edge's transform chain: `map`, `filter`, `flatMap`, composed at
 * declaration time and evaluated where the core routes outputs.
 *
 * One implementation for the three places a chain runs — an entry payload
 * before its handler, a payload at the prepare crossing, and an emit on its
 * way out — so a filter cannot mean one thing on one path and something else
 * elsewhere.
 *
 * The answer is a list because that is the shape all three operators share: a
 * dropped value is the empty list, a mapped one is a list of one, and a fan is
 * a list of many. A later link runs over every surviving value and its results
 * concatenate, so `flatMap` followed by `filter` yields exactly the survivors.
 */
final class Transforms
{
    private function __construct()
    {
    }

    /**
     * @param list<callable(mixed):mixed> $transforms
     * @return list<mixed>
     */
    public static function apply(array $transforms, mixed $value): array
    {
        $values = [$value];

        foreach ($transforms as $transform) {
            $next = [];
            foreach ($values as $current) {
                $yielded = $transform($current);
                if (Dropped::is($yielded)) {
                    continue;
                }
                if ($yielded instanceof Fanned) {
                    array_push($next, ...$yielded->values);
                } else {
                    $next[] = $yielded;
                }
            }
            if ($next === []) {
                return [];
            }
            $values = $next;
        }

        return $values;
    }
}
