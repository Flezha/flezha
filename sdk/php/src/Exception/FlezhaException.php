<?php

declare(strict_types=1);

namespace Flezha\Exception;

use RuntimeException;

/**
 * The base of everything this SDK raises, so a host can catch one thing.
 */
class FlezhaException extends RuntimeException
{
    /**
     * Whether the retry ladder must not retry the item this error ended.
     *
     * Read by the dispatcher on every throwable, so an author can mark any
     * exception non-retryable by implementing this rather than by extending a
     * particular class.
     */
    public function isNonRetryable(): bool
    {
        return false;
    }
}
