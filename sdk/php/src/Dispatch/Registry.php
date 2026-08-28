<?php

declare(strict_types=1);

namespace Flezha\Dispatch;

use Flezha\Exception\FlezhaException;
use Flezha\Graph\ClassSpec;

/** The declared classes this runner serves. */
final class Registry
{
    /** @var array<string, RegisteredClass> */
    private array $classes = [];

    public function add(ClassSpec $spec): void
    {
        if (isset($this->classes[$spec->factoryId])) {
            throw new FlezhaException(sprintf(
                'duplicate factory id `%s`: a runner declares each factory once',
                $spec->factoryId,
            ));
        }

        $this->classes[$spec->factoryId] = new RegisteredClass($spec);
    }

    public function get(string $factoryId): ?RegisteredClass
    {
        return $this->classes[$factoryId] ?? null;
    }

    /** @return list<RegisteredClass> */
    public function all(): array
    {
        return array_values($this->classes);
    }
}
