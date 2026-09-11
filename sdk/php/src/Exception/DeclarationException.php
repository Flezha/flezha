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
     * @param list<string> $refusals the core's vocabulary names for what was
     *                               refused: the core's verdict, or a refusal
     *                               this package makes under one of them; empty
     *                               for a refusal of this package's own surface
     */
    public function __construct(
        public readonly string $factoryId,
        public readonly ?string $handler,
        public readonly string $defect,
        public readonly array $refusals = [],
    ) {
        // A named refusal reads the way the core renders its verdict. The
        // core's own verdict arrives already rendered, header included.
        $header = sprintf('topology rejected [%s]:', implode(', ', $refusals));
        $text = $refusals === [] || str_starts_with($defect, $header)
            ? $defect
            : sprintf("%s\n  - %s", $header, $defect);
        parent::__construct(
            $handler === null
                ? sprintf('%s: %s', $factoryId, $text)
                : sprintf('%s: %s: %s', $factoryId, $handler, $text),
        );
    }
}
