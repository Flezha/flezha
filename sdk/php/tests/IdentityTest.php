<?php

declare(strict_types=1);

namespace Flezha\Tests;

use Flezha\Identity;
use PHPUnit\Framework\TestCase;

/**
 * The replay-stable identifier, pinned to the value every other SDK derives.
 *
 * The vectors are the shared ones: the identity layout is length-prefixed
 * `instance | procedure | key` with big-endian lengths, and the identifier is
 * FNV-1a 128 over `identity | u64 BE change seq | u32 BE ordinal`. PHP has no
 * 128-bit integer, so this is the one algorithm in the package reimplemented
 * rather than delegated — which is exactly why it is pinned.
 */
final class IdentityTest extends TestCase
{
    private const IDENTITY_HEX = '04696e73740470726f6300026b31';

    public function testTheIdentityLayoutIsLengthPrefixed(): void
    {
        self::assertSame(self::IDENTITY_HEX, bin2hex(Identity::entity('inst', 'proc', 'k1')));
    }

    /** @return iterable<string, array{int, int, string}> */
    public static function vectors(): iterable
    {
        yield 'first call of a fresh entity' => [0, 0, 'ec2d8145d51f0ef2bc81a81ecef9711f'];
        yield 'at seq 7' => [7, 0, 'c9cada20361f0ef2bc819fa887d72efe'];
        yield 'second call at seq 7' => [7, 1, 'c9cada20371f0ef2bc819fa887d73039'];
        yield 'a large seq and ordinal' => [12_345, 42, 'e24bb5eab21f0ef2bc4a8ded9ae4b92a'];
    }

    /** @dataProvider vectors */
    public function testStableIdMatchesTheOtherSdks(int $changeSeq, int $ordinal, string $expected): void
    {
        self::assertSame(
            $expected,
            Identity::stableId((string) hex2bin(self::IDENTITY_HEX), $changeSeq, $ordinal),
        );
    }

    public function testAnIdentifierIsThirtyTwoLowercaseHexCharacters(): void
    {
        $id = Identity::stableId(Identity::entity('o', 'p', ''), 1, 0);

        self::assertMatchesRegularExpression('/^[0-9a-f]{32}$/', $id);
    }

    public function testAnOversizedComponentIsRefused(): void
    {
        $this->expectExceptionMessage('procedure id must be at most 255 bytes');

        Identity::entity(str_repeat('x', 256), 'p', '');
    }
}
