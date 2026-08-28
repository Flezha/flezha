<?php

declare(strict_types=1);

namespace Flezha\Tests;

use Flezha\Graph\HostClassDecl;
use Flezha\Graph\Validate;
use Flezha\Operative;
use PHPUnit\Framework\TestCase;
use ReflectionClass;

/**
 * Every shipped example declares a topology the core would accept.
 *
 * Examples are the first thing an author copies, so a stale one is a defect
 * that ships. Requiring and validating them here makes them provably correct
 * declarations rather than prose — and it costs nothing, because each file
 * only runs a runner when it is the invoked script.
 */
final class ExamplesTest extends TestCase
{
    /** @return iterable<string, array{string}> */
    public static function examples(): iterable
    {
        foreach (glob(dirname(__DIR__) . '/examples/*.php') ?: [] as $path) {
            yield basename($path) => [$path];
        }
    }

    /** @dataProvider examples */
    public function testAnExampleDeclaresATopologyTheCoreWouldAccept(string $path): void
    {
        $before = get_declared_classes();
        require_once $path;
        $declared = array_diff(get_declared_classes(), $before);

        $operatives = array_values(array_filter(
            $declared,
            static fn (string $class): bool => is_subclass_of($class, Operative::class)
                && !(new ReflectionClass($class))->isAbstract(),
        ));

        self::assertNotEmpty($operatives, "$path declares no operative");

        foreach ($operatives as $class) {
            $spec = $class::spec();
            Validate::spec($spec);

            $decl = HostClassDecl::of($spec);
            self::assertSame(HostClassDecl::SDK_LANG_PHP, $decl['sdk_lang']);
            self::assertNotSame('', $decl['factory_id']);
        }
    }
}
