<?php

declare(strict_types=1);

namespace Flezha\Tests;

/**
 * The nearest directory at or above one that carries a given file.
 *
 * Both of this suite's resolvers ask the same question: the contract set looks
 * for `test/fixtures/contract.txt`, the test app for its
 * `conformance.json`. Asking it by walking rather than by counting `..`
 * segments is what lets one answer serve both layouts this package lives in,
 * under `sdk/php` in the core's tree and at the root of its own repository.
 */
final class Ascent
{
    /** @return string|null the directory, or null when no ancestor carries it */
    public static function carrying(string $from, string $marker): ?string
    {
        for ($at = $from; ; $at = dirname($at)) {
            if (is_file($at . '/' . $marker)) {
                return $at;
            }
            if (dirname($at) === $at) {
                return null;
            }
        }
    }
}
