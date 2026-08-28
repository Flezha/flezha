<?php

declare(strict_types=1);

namespace Flezha\Codec;

/**
 * How one declared State or Change type becomes durable bytes.
 *
 * The seam a hand-written codec replaces: the dispatcher and the restore fold
 * both ask it, so a class that declares one is encoded the same way live and
 * on replay.
 */
interface ValueCodec
{
    public function encode(mixed $value): string;

    public function decode(string $bytes): mixed;
}
