<?php

declare(strict_types=1);

namespace Flezha\Codec;

use Flezha\Codable;
use Flezha\Exception\DeclarationException;

/**
 * The codec for an author-declared State or Change class: `toArray()` on the
 * way out, `fromArray()` on the way back, over JSON.
 *
 * Built on the same seam a hand-written codec uses rather than beside it — the
 * dispatcher and the restore fold already ask that seam, and a second one
 * would be a second answer to one question.
 */
final class TypeCodec implements ValueCodec
{
    /** @param class-string $type */
    public function __construct(private readonly string $type, string $which)
    {
        if (!is_subclass_of($type, Codable::class)) {
            throw new DeclarationException($type, $which, sprintf(
                'is declared as the durable %s type but does not implement %s; a durable value '
                . 'is encoded as the array it answers and rebuilt from it',
                $which,
                Codable::class,
            ));
        }
    }

    public function encode(mixed $value): string
    {
        return Json::guard($this->type, fn (): string => Json::encode($value));
    }

    public function decode(string $bytes): mixed
    {
        $data = Json::decode($bytes);

        return $data === null ? null : $this->type::fromArray((array) $data);
    }
}
