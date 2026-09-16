<?php

declare(strict_types=1);

namespace Flezha\Tests;

use PHPUnit\Framework\TestCase;

/**
 * The contract set this package is proven against: the shared fixtures, the
 * declaration-key list, the C ABI surface.
 *
 * `test/fixtures/contract.txt` names every member and the release publishes
 * them as one archive, so the set travels with the package rather than living
 * only in the repository the core is developed in. This resolves the directory
 * those repository-relative paths hang off, once, in the order every SDK uses:
 * `FLEZHA_CONTRACT_DIR`, else `contract/` at the package root, else the tree
 * this package sits in.
 *
 * A member that resolves nowhere FAILS rather than skips: the set ships with
 * every release, and a skip would let a fixture test stop running outside a
 * checkout without anyone noticing.
 */
abstract class RepositoryTestCase extends TestCase
{
    /** The member every candidate directory is recognised by. */
    private const MANIFEST = 'test/fixtures/contract.txt';

    private static ?string $contractRoot = null;

    protected function repositoryFile(string $relative): string
    {
        $path = self::contractRoot() . '/' . $relative;
        self::assertFileExists($path, sprintf(
            '%s is a member of the contract set and is not in %s',
            $relative,
            self::contractRoot(),
        ));

        return $path;
    }

    /** The directory the contract set's repository-relative paths hang off. */
    private static function contractRoot(): string
    {
        if (self::$contractRoot !== null) {
            return self::$contractRoot;
        }

        $package = dirname(__DIR__);
        $candidates = [
            getenv('FLEZHA_CONTRACT_DIR') ?: null,
            $package . '/contract',
            Ascent::carrying($package, self::MANIFEST),
        ];

        foreach ($candidates as $candidate) {
            if (is_string($candidate) && is_file($candidate . '/' . self::MANIFEST)) {
                return self::$contractRoot = $candidate;
            }
        }

        $unresolved = [
            '$FLEZHA_CONTRACT_DIR (unset)',
            '',
            sprintf('no directory above %s carries %s', $package, self::MANIFEST),
        ];
        self::fail(sprintf(
            "the contract set is nowhere this suite looked:\n  %s\n"
            . 'Unpack flezha-fixtures-<version>.tar.gz at %s/contract, or set FLEZHA_CONTRACT_DIR.',
            implode("\n  ", array_map(
                static fn (?string $candidate, string $why): string => $candidate ?? $why,
                $candidates,
                $unresolved,
            )),
            $package,
        ));
    }
}
