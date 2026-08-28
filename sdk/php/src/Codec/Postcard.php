<?php

declare(strict_types=1);

namespace Flezha\Codec;

use Flezha\Exception\DecodeException;

/**
 * The postcard primitives the activation schema is built from.
 *
 * Field order IS the wire contract (`flz_core/src/schema.rs`), so nothing here
 * is self-describing: a reader and a writer agree because they walk the same
 * shape in the same order. Encoding is a set of static functions returning
 * fragments; decoding is {@see PostcardCursor}, which holds the position.
 *
 * ## The `u64` policy
 *
 * `PHP_INT_MAX` is 2^63−1 and the schema is full of `u64` — offsets, change
 * seqs, timestamps, timer deadlines. PHP's integer is signed and silently
 * becomes a float on overflow, which would corrupt an offset comparison
 * rather than raise. So a varint whose value needs the 64th bit is a decode
 * error naming the field: real offsets, millisecond timestamps and seqs do
 * not approach 2^63, and a value that does is corruption.
 */
final class Postcard
{
    /** Longest LEB128 encoding of a 64-bit value. */
    private const MAX_VARINT_BYTES = 10;

    private function __construct()
    {
    }

    /** An unsigned integer, LEB128. */
    public static function varint(int $value): string
    {
        if ($value < 0) {
            throw new DecodeException(
                "cannot encode $value as an unsigned varint; the schema's integers are unsigned",
            );
        }

        $out = '';
        do {
            $byte = $value & 0x7F;
            $value >>= 7;
            $out .= chr($value === 0 ? $byte : ($byte | 0x80));
        } while ($value !== 0);

        return $out;
    }

    /** A length-prefixed byte string; also how a `String` is written. */
    public static function bytes(string $value): string
    {
        return self::varint(strlen($value)) . $value;
    }

    public static function bool(bool $value): string
    {
        return $value ? "\x01" : "\x00";
    }

    /** An enum discriminant: the variant index, in declaration order. */
    public static function variant(int $index): string
    {
        return self::varint($index);
    }

    /** `None`, or `Some` followed by the value the callback writes. */
    public static function option(?string $encoded): string
    {
        return $encoded === null ? "\x00" : "\x01" . $encoded;
    }

    /** An optional unsigned integer, the shape most of the schema's use. */
    public static function optionalVarint(?int $value): string
    {
        return self::option($value === null ? null : self::varint($value));
    }

    /**
     * A sequence: its length, then each element as `$encode` writes it.
     *
     * @template T
     * @param iterable<T>       $items
     * @param callable(T):string $encode
     */
    public static function seq(iterable $items, callable $encode): string
    {
        $out = [];
        foreach ($items as $item) {
            $out[] = $encode($item);
        }

        return self::varint(count($out)) . implode('', $out);
    }

    /**
     * `Vec<(String, String)>` — the metadata shape, written as a plain PHP
     * map so the authoring surface never sees a pair list.
     *
     * @param array<string, string> $pairs
     */
    public static function metadata(array $pairs): string
    {
        $out = '';
        foreach ($pairs as $key => $value) {
            $out .= self::bytes((string) $key) . self::bytes((string) $value);
        }

        return self::varint(count($pairs)) . $out;
    }

    /** The longest LEB128 encoding a well-formed 64-bit varint can have. */
    public static function maxVarintBytes(): int
    {
        return self::MAX_VARINT_BYTES;
    }
}
