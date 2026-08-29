<?php

declare(strict_types=1);

namespace Flezha;

use Flezha\Attribute\Operative as OperativeAttribute;
use Flezha\Exception\DeclarationException;
use Flezha\Graph\ClassSpec;
use ReflectionMethod;

/**
 * A workflow operative: a run-sugar body, or a structured chain over a
 * channels graph — one or the other, never both.
 */
abstract class WorkflowOperative extends Operative
{
    /**
     * A run-sugar body: one imperative function over the seed args and the
     * workflow context. Override it, or declare `channels()` instead.
     */
    public static function run(mixed $args, WorkflowCtx $ctx): mixed
    {
        return null;
    }

    protected static function buildSpec(OperativeAttribute $declaration): ClassSpec
    {
        $hasRun = (new ReflectionMethod(static::class, 'run'))
            ->getDeclaringClass()->getName() !== self::class;
        $hasChannels = (new ReflectionMethod(static::class, 'channels'))
            ->getDeclaringClass()->getName() !== Operative::class;

        if ($hasRun && $hasChannels) {
            throw new DeclarationException(
                $declaration->factoryId,
                null,
                'declares both a run body and a channels graph; a workflow is one or the other'
            );
        }
        if (!$hasRun && !$hasChannels) {
            throw new DeclarationException(
                $declaration->factoryId,
                null,
                'declares neither a run body nor a channels graph'
            );
        }

        $builder = static::builder($declaration);
        $workflow = static::workflowDeclaration();

        return static::assemble(
            $declaration,
            $builder,
            'workflow',
            $workflow->syncDurability || $builder->syncDurability,
            $workflow,
            $hasRun ? static::run(...) : null,
        );
    }
}
