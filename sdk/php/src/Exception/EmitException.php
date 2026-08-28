<?php

declare(strict_types=1);

namespace Flezha\Exception;

/**
 * An emit the declaration cannot carry — a channel the procedure does not
 * produce on, a reserved metadata key, a keyed channel with no key.
 */
final class EmitException extends FlezhaException
{
}
