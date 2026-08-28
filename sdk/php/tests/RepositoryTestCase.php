<?php

declare(strict_types=1);

namespace Flezha\Tests;

use PHPUnit\Framework\TestCase;

/**
 * The repository this package is developed in, for the tests that read a
 * shared artefact — the schema fixtures, the declaration-key list.
 *
 * Skipped rather than failed when the package is installed on its own: an
 * artefact this package does not ship cannot be asserted against from a
 * `vendor/` directory, and pretending otherwise would make an install red for
 * a reason that is not the installer's.
 */
abstract class RepositoryTestCase extends TestCase
{
    protected function repositoryFile(string $relative): string
    {
        $path = dirname(__DIR__, 3) . '/' . $relative;
        if (!is_file($path)) {
            self::markTestSkipped("$relative is not present; this is an installed package, not a checkout");
        }

        return $path;
    }
}
