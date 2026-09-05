<?php

declare(strict_types=1);

namespace Flezha\Exception;

/**
 * A registration refusal: names the offending class, the handler and the
 * defect.
 *
 * PHP has no compile step, so registration-time refusal replaces the Rust
 * builder's compile-time one and the message carries what a compiler would
 * have pointed at.
 */
final class DeclarationException extends FlezhaException
{
    /**
     * @param list<string> $refusals the core's own names for what it refused,
     *                               when the verdict came from it; empty for a
     *                               refusal this package made on its own surface
     */
    public function __construct(
        public readonly string $factoryId,
        public readonly ?string $handler,
        public readonly string $defect,
        public readonly array $refusals = [],
    ) {
        parent::__construct(
            $handler === null
                ? sprintf('%s: %s', $factoryId, $defect)
                : sprintf('%s: %s: %s', $factoryId, $handler, $defect),
        );
    }
}
