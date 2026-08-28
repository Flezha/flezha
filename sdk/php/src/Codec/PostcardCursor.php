<?php

declare(strict_types=1);

namespace Flezha\Codec;

use Flezha\Exception\DecodeException;

/**
 * A position in a postcard buffer, and the reads the schema needs.
 *
 * Every read names the field it is reading, so a malformed buffer says where
 * it stopped making sense rather than "unexpected end of input".
 */
final class PostcardCursor
{
    private int $offset = 0;
    private readonly int $length;

    public function __construct(private readonly string $bytes)
    {
        $this->length = strlen($bytes);
    }

    /**
     * An unsigned LEB128 integer.
     *
     * Refuses a value that needs the 64th bit rather than returning a float:
     * see the `u64` policy on {@see Postcard}.
     */
    public function varint(string $field): int
    {
        $value = 0;
        $shift = 0;

        for ($read = 0; $read < Postcard::maxVarintBytes(); $read++) {
            $byte = $this->byte($field);
            $chunk = $byte & 0x7F;

            if ($shift >= 63) {
                if ($chunk !== 0) {
                    throw new DecodeException(sprintf(
                        '%s carries a u64 above PHP_INT_MAX (%d); PHP integers are signed, and a '
                        . 'silently widened offset would compare wrongly rather than fail',
                        $field,
                        PHP_INT_MAX,
                    ));
                }
            } else {
                $value |= $chunk << $shift;
            }

            $shift += 7;
            if (($byte & 0x80) === 0) {
                return $value;
            }
        }

        throw new DecodeException("$field is a varint longer than any 64-bit value can be");
    }

    /** A length-prefixed byte string; also how a `String` is read. */
    public function bytes(string $field): string
    {
        $length = $this->varint("$field length");
        if ($this->offset + $length > $this->length) {
            throw new DecodeException("$field claims $length bytes, past the end of the buffer");
        }
        $value = substr($this->bytes, $this->offset, $length);
        $this->offset += $length;

        return $value;
    }

    public function bool(string $field): bool
    {
        $byte = $this->byte($field);
        if ($byte > 1) {
            throw new DecodeException("$field is $byte, which is not a bool");
        }

        return $byte === 1;
    }

    /** An enum discriminant: the variant index, in declaration order. */
    public function variant(string $field): int
    {
        return $this->varint("$field variant");
    }

    /** Whether an `Option` is `Some`; the value follows when it is. */
    public function some(string $field): bool
    {
        return $this->bool("$field presence");
    }

    /** An optional unsigned integer, the shape most of the schema's use. */
    public function optionalVarint(string $field): ?int
    {
        return $this->some($field) ? $this->varint($field) : null;
    }

    /**
     * A sequence: its length, then each element as `$decode` reads it.
     *
     * @template T
     * @param callable(self):T $decode
     * @return list<T>
     */
    public function seq(string $field, callable $decode): array
    {
        $count = $this->varint("$field length");
        if ($count > $this->length - $this->offset && $count > $this->length) {
            throw new DecodeException("$field claims $count elements, more than the buffer holds");
        }

        $items = [];
        for ($index = 0; $index < $count; $index++) {
            $items[] = $decode($this);
        }

        return $items;
    }

    /**
     * `Vec<(String, String)>`, as the PHP map the authoring surface sees.
     *
     * @return array<string, string>
     */
    public function metadata(string $field): array
    {
        $pairs = [];
        foreach ($this->seq($field, fn (self $c): array => [$c->bytes("$field key"), $c->bytes("$field value")]) as $pair) {
            $pairs[$pair[0]] = $pair[1];
        }

        return $pairs;
    }

    /** Refuse trailing bytes: the shape was read, and there is more. */
    public function finish(string $what): void
    {
        if ($this->offset !== $this->length) {
            throw new DecodeException(sprintf(
                '%s left %d unread byte(s); the buffer does not match the schema this shim was built against',
                $what,
                $this->length - $this->offset,
            ));
        }
    }

    private function byte(string $field): int
    {
        if ($this->offset >= $this->length) {
            throw new DecodeException("$field ran past the end of the buffer");
        }

        return ord($this->bytes[$this->offset++]);
    }
}
