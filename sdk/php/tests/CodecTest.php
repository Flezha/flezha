<?php

declare(strict_types=1);

namespace Flezha\Tests;

use Flezha\Codable;
use Flezha\Codec\Json;
use Flezha\Codec\TypeCodec;
use Flezha\Exception\DeclarationException;
use Flezha\Exception\UnrepresentableValueException;
use PHPUnit\Framework\TestCase;

final class Totals implements Codable
{
    public function __construct(public int $count = 0, public string $label = '')
    {
    }

    public function toArray(): array
    {
        return ['count' => $this->count, 'label' => $this->label];
    }

    public static function fromArray(array $data): static
    {
        return new self((int) $data['count'], (string) $data['label']);
    }
}

final class NotCodable
{
}

/** The durable State/Change codec, and what it refuses to carry. */
final class CodecTest extends TestCase
{
    public function testAnAuthorDeclaredTypeRoundTripsThroughItsOwnArray(): void
    {
        $codec = new TypeCodec(Totals::class, 'state');
        $decoded = $codec->decode($codec->encode(new Totals(3, 'a')));

        self::assertEquals(new Totals(3, 'a'), $decoded);
    }

    /**
     * The whole reason the codec is not `serialize()`: the durable bytes carry
     * the array and nothing else, so a class rename leaves every record
     * already on disk readable.
     */
    public function testTheDurableBytesCarryNoClassName(): void
    {
        $bytes = (new TypeCodec(Totals::class, 'state'))->encode(new Totals(3, 'a'));

        self::assertSame('{"count":3,"label":"a"}', $bytes);
        self::assertStringNotContainsString('Totals', $bytes);
    }

    public function testATypeWithoutTheInterfaceIsRefusedAtDeclarationTime(): void
    {
        $this->expectException(DeclarationException::class);
        $this->expectExceptionMessage('does not implement Flezha\Codable');

        new TypeCodec(NotCodable::class, 'change');
    }

    public function testPlainValuesRoundTrip(): void
    {
        foreach ([null, true, 0, -1, 1.5, 'text', [], ['a' => [1, 2]], [1, 2, 3]] as $value) {
            self::assertSame($value, Json::decode(Json::encode($value)), var_export($value, true));
        }
    }

    public function testAFloatKeepsItsFraction(): void
    {
        self::assertSame('1.0', Json::encode(1.0));
    }

    /** @return iterable<string, array{mixed, string}> */
    public static function unrepresentable(): iterable
    {
        yield 'a plain object' => [['at' => new NotCodable()], 'at'];
        yield 'a byte string' => [['blob' => "\xff\xfe"], 'blob'];
        yield 'not a number' => [['ratio' => NAN], 'ratio'];
        yield 'nested' => [['a' => ['b' => [new NotCodable()]]], 'a.b.0'];
    }

    /**
     * Walked at registration rather than at the first restore, because a state
     * that cannot be encoded cannot be restored and the first anyone would
     * otherwise learn of it is a restart.
     *
     * @dataProvider unrepresentable
     */
    public function testAnUnrepresentableStateIsNamedByItsPath(mixed $sample, string $path): void
    {
        $this->expectException(UnrepresentableValueException::class);
        $this->expectExceptionMessage("Demo.$path cannot be encoded");

        Json::assertRepresentable('Demo', $sample);
    }

    public function testACodableInsideAStateIsRepresentable(): void
    {
        Json::assertRepresentable('Demo', ['totals' => new Totals(1, 'x')]);

        self::assertSame('{"totals":{"count":1,"label":"x"}}', Json::encode(['totals' => new Totals(1, 'x')]));
    }
}
