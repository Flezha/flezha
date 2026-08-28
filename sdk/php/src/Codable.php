<?php

declare(strict_types=1);

namespace Flezha;

/**
 * A State or Change class that owns its own durable mapping.
 *
 * The pair is author-declared rather than reflective on purpose: the durable
 * bytes carry the array, never the class name or the property layout, so
 * renaming or moving the class leaves every record already on disk readable.
 * PHP's own `serialize()` does the opposite — it embeds the fully-qualified
 * name and mangles private property names — which turns a rename into a
 * data-loss-shaped failure with nothing to warn about it.
 */
interface Codable
{
    /** @return array<string, mixed> */
    public function toArray(): array;

    /** @param array<string, mixed> $data */
    public static function fromArray(array $data): static;
}
