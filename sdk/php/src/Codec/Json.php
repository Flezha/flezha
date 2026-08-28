<?php

declare(strict_types=1);

namespace Flezha\Codec;

use Flezha\Codable;
use Flezha\Exception\UnrepresentableValueException;
use JsonException;

/**
 * The JSON codec for durable `State` and `Change` bodies.
 *
 * JSON, not PHP's `serialize()`: the durable bytes are read back after a
 * restart, a restore fold and a hot-swap migration, and `serialize()` embeds
 * fully-qualified class names and mangles private property names, so a class
 * rename breaks the restore of every record already written. It is also what
 * `flz state at` shows an operator, and what a migration handover carries —
 * so the durable form, the live read and the handover are one encoding rather
 * than three.
 *
 * The type mapping is the JSON one, stated rather than discovered:
 *
 * | PHP                  | JSON      | Note                                    |
 * | -------------------- | --------- | --------------------------------------- |
 * | `int`, `float`       | number    | `1.0` keeps its fraction                 |
 * | `string`             | string    | must be valid UTF-8; bytes need a codec  |
 * | `bool`, `null`       | as-is     |                                          |
 * | list array           | array     |                                          |
 * | map array            | object    | keys come back as strings                |
 * | {@see Codable}       | object    | through `toArray()` / `fromArray()`      |
 *
 * An object that is not `Codable` has no durable form, and saying so at
 * registration is the whole point of {@see assertRepresentable()}: the
 * alternative is learning it at the first restart.
 */
final class Json
{
    private const FLAGS = JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION | JSON_UNESCAPED_UNICODE;

    private function __construct()
    {
    }

    public static function encode(mixed $value): string
    {
        return json_encode(self::plain($value), self::FLAGS);
    }

    public static function decode(string $bytes): mixed
    {
        if ($bytes === '') {
            return null;
        }

        return json_decode($bytes, true, 512, JSON_THROW_ON_ERROR);
    }

    /**
     * A value the codec cannot carry, named by the declaring type and the path
     * to the offending field.
     *
     * Walked once at registration against what the procedure's initial state
     * produces, so an author learns at startup rather than at the first
     * restore that their state does not survive one.
     */
    public static function assertRepresentable(string $typeName, mixed $sample): void
    {
        self::walk($typeName, $sample, '');
    }

    /** Everything `Codable` collapsed to arrays, ready for `json_encode`. */
    private static function plain(mixed $value): mixed
    {
        if ($value instanceof Codable) {
            return self::plain($value->toArray());
        }
        if (is_array($value)) {
            return array_map(self::plain(...), $value);
        }

        return $value;
    }

    private static function walk(string $typeName, mixed $value, string $path): void
    {
        $where = $path === '' ? '<root>' : $path;

        if ($value instanceof Codable) {
            self::walk($typeName, $value->toArray(), $path);

            return;
        }
        if (is_array($value)) {
            foreach ($value as $key => $item) {
                self::walk($typeName, $item, $path === '' ? (string) $key : "$path.$key");
            }

            return;
        }
        if (is_object($value)) {
            throw new UnrepresentableValueException($typeName, $where, sprintf(
                'an instance of %s has no durable form; implement %s on it, or hold a plain '
                . 'array instead',
                $value::class,
                Codable::class,
            ));
        }
        if (is_resource($value)) {
            throw new UnrepresentableValueException(
                $typeName,
                $where,
                'a resource has no durable form; hold what it names instead',
            );
        }
        if (is_string($value) && !mb_check_encoding($value, 'UTF-8')) {
            throw new UnrepresentableValueException(
                $typeName,
                $where,
                'the string is not valid UTF-8, and JSON carries no byte string; base64 it, or '
                . 'supply a hand-written codec',
            );
        }
        if (is_float($value) && !is_finite($value)) {
            throw new UnrepresentableValueException(
                $typeName,
                $where,
                'JSON has no representation for NAN or INF',
            );
        }
    }

    /** Re-raise an encoding failure naming the type that could not be carried. */
    public static function guard(string $typeName, callable $encode): string
    {
        try {
            return $encode();
        } catch (JsonException $e) {
            throw new UnrepresentableValueException($typeName, '<root>', $e->getMessage());
        }
    }
}
