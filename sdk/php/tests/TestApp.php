<?php

declare(strict_types=1);

namespace Flezha\Tests;

/**
 * Where this package's conformance test app is.
 *
 * The app declares the fixture classes `conformance.json` names for each
 * capability, and the local-mode suite runs the same ones rather than a second
 * set declared beside them: a class that drifts from the one end-to-end drives
 * would prove nothing about the SDK under test.
 *
 * Two layouts, found rather than counted: `test_app/` beside the package once
 * this package has a repository of its own, and `test/apps/php_runner` in the
 * enclosing tree while it sits inside the core's.
 */
final class TestApp
{
    /** Where the app sits in the core's tree, relative to any ancestor. */
    public const IN_TREE = 'test/apps/php_runner';

    private static ?string $root = null;

    /** The test app's directory. */
    public static function root(): string
    {
        if (self::$root !== null) {
            return self::$root;
        }

        $package = dirname(__DIR__);
        $beside = $package . '/test_app';
        if (is_file($beside . '/conformance.json')) {
            return self::$root = $beside;
        }
        $tree = Ascent::carrying($package, self::IN_TREE . '/conformance.json');
        if ($tree !== null) {
            return self::$root = $tree . '/' . self::IN_TREE;
        }

        throw new \RuntimeException(sprintf(
            'no conformance test app beside %s (test_app/) or in any tree above it '
            . '(%s); the local-mode suite runs the classes it declares',
            $package,
            self::IN_TREE,
        ));
    }

    /** One file of the test app, loaded into this process. */
    public static function load(string $file): void
    {
        require_once self::root() . '/' . $file;
    }

    /**
     * The capability-to-fixture-class table `conformance.json` declares.
     *
     * @return array<string, list<string>>
     */
    public static function declaredFixtures(): array
    {
        $manifest = json_decode(
            (string) file_get_contents(self::root() . '/conformance.json'),
            true,
            flags: JSON_THROW_ON_ERROR,
        );

        return $manifest['fixtures'];
    }
}
