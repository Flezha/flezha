<?php

declare(strict_types=1);

namespace Flezha\Tests;

use Flezha\Codec\Postcard;
use Flezha\Codec\PostcardCursor;
use Flezha\Exception\DecodeException;
use PHPUnit\Framework\TestCase;

/** The postcard primitives, and the `u64` policy that is this host's own. */
final class PostcardTest extends TestCase
{
    /** @return iterable<string, array{int, string}> */
    public static function varints(): iterable
    {
        yield 'zero' => [0, '00'];
        yield 'one byte' => [127, '7f'];
        yield 'two bytes' => [128, '8001'];
        yield 'a thousand' => [1_000, 'e807'];
        yield 'a u32 ceiling' => [0xFFFFFFFF, 'ffffffff0f'];
        yield 'the largest PHP integer' => [PHP_INT_MAX, 'ffffffffffffffff7f'];
    }

    /** @dataProvider varints */
    public function testVarintsRoundTrip(int $value, string $hex): void
    {
        self::assertSame($hex, bin2hex(Postcard::varint($value)));
        self::assertSame($value, (new PostcardCursor((string) hex2bin($hex)))->varint('value'));
    }

    /**
     * PHP's integer is signed, so a `u64` with the high bit set has no
     * representation. Failing loudly is the policy: silently widening to a
     * float would corrupt an offset comparison rather than raise, and real
     * offsets, millisecond timestamps and change seqs do not approach 2^63.
     */
    public function testAU64AboveTheLargestIntegerIsRefusedByName(): void
    {
        // 2^63, one past PHP_INT_MAX.
        $bytes = (string) hex2bin('80808080808080808001');

        $this->expectException(DecodeException::class);
        $this->expectExceptionMessage('consumed offset carries a u64 above PHP_INT_MAX');

        (new PostcardCursor($bytes))->varint('consumed offset');
    }

    public function testABytesFieldPastTheBufferIsRefused(): void
    {
        $this->expectException(DecodeException::class);
        $this->expectExceptionMessage('payload claims 9 bytes, past the end of the buffer');

        (new PostcardCursor("\x09ab"))->bytes('payload');
    }

    public function testATruncatedReadNamesTheFieldItStoppedAt(): void
    {
        $this->expectException(DecodeException::class);
        $this->expectExceptionMessage('change seq ran past the end of the buffer');

        (new PostcardCursor(''))->varint('change seq');
    }

    public function testTrailingBytesAreRefused(): void
    {
        $cursor = new PostcardCursor("\x01\x02");
        $cursor->varint('first');

        $this->expectException(DecodeException::class);
        $this->expectExceptionMessage('left 1 unread byte(s)');

        $cursor->finish('the activation');
    }

    public function testMetadataIsAMapInBothDirections(): void
    {
        $encoded = Postcard::metadata(['flz.key' => 'k1', 'user' => 'v']);

        self::assertSame(
            ['flz.key' => 'k1', 'user' => 'v'],
            (new PostcardCursor($encoded))->metadata('metadata'),
        );
    }

    public function testASequenceCarriesItsLength(): void
    {
        $encoded = Postcard::seq(['a', 'bb'], Postcard::bytes(...));

        self::assertSame('020161026262', bin2hex($encoded));
    }

    public function testAnUnsignedEncoderRefusesANegativeValue(): void
    {
        $this->expectException(DecodeException::class);

        Postcard::varint(-1);
    }
}
