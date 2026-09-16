<?php

declare(strict_types=1);

namespace Flezha\Tests;

use Flezha\Env;

/** This runtime's reading of the shared parse fixture. */
final class EnvTest extends RepositoryTestCase
{
    public function testEveryCaseParsesTheWayTheFixtureSays(): void
    {
        $seen = [];
        foreach ($this->cases() as [$kind, $raw, $expected]) {
            $seen[$kind] = true;
            $got = match ($kind) {
                'positive_int' => Env::parsePositive($raw),
                'ms' => Env::parseMs($raw),
                'int' => Env::parseInt($raw),
                'bool' => Env::parseBool($raw),
                default => self::fail("the fixture names a type this runtime has no rule for: {$kind}"),
            };
            $rendered = match (true) {
                $got === null => 'error',
                is_bool($got) => $got ? 'true' : 'false',
                default => (string) $got,
            };
            self::assertSame($expected, $rendered, "{$kind}|{$raw}");
        }
        $kinds = array_keys($seen);
        sort($kinds);
        self::assertSame(['bool', 'int', 'ms', 'positive_int'], $kinds);
    }

    public function testAnUnsetVariableLeavesTheDefaultStanding(): void
    {
        $absent = 'FLZ_TEST_ABSENT_VARIABLE';
        self::assertSame(7, Env::positive($absent, 7));
        self::assertTrue(Env::flag($absent, true));
    }

    /** @return list<array{0: string, 1: string, 2: string}> */
    private function cases(): array
    {
        $lines = file(
            $this->repositoryFile('test/fixtures/knob_parse_cases.txt'),
            FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES,
        );
        $cases = [];
        foreach ($lines as $line) {
            if (trim($line) === '' || str_starts_with($line, '#')) {
                continue;
            }
            [$kind, $raw, $expected] = explode('|', $line);
            $cases[] = [$kind, $raw === '<empty>' ? '' : $raw, $expected];
        }

        return $cases;
    }
}
