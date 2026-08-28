<?php

declare(strict_types=1);

namespace Flezha\Exception;

/**
 * A procedure error the retry ladder must not retry: the item parks
 * immediately, with no further attempts.
 */
final class NonRetryableException extends FlezhaException
{
    public function isNonRetryable(): bool
    {
        return true;
    }
}
