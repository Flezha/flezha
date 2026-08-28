<?php

declare(strict_types=1);

namespace Flezha\Exception;

/**
 * A value the durable codec cannot represent, named by the declaring type and
 * the path to the offending field.
 */
final class UnrepresentableValueException extends FlezhaException
{
    public function __construct(string $typeName, string $path, string $reason)
    {
        parent::__construct(
            sprintf('%s.%s cannot be encoded for durable state: %s', $typeName, $path, $reason),
        );
    }
}
