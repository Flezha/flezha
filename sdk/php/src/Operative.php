<?php

declare(strict_types=1);

namespace Flezha;

use Flezha\Attribute\Operative as OperativeAttribute;
use Flezha\Attribute\Workflow as WorkflowAttribute;
use Flezha\Exception\DeclarationException;
use Flezha\Graph\Builder;
use Flezha\Graph\ClassSpec;
use ReflectionClass;

/**
 * The base an authored operative extends: an `#[Operative]` attribute for the
 * factory id, and `channels()` for the graph.
 */
abstract class Operative
{
    /** @var array<class-string, ClassSpec> */
    private static array $cache = [];

    /**
     * Declare the channels graph. A run-sugar workflow leaves it alone and
     * writes {@see WorkflowOperative::run()} instead.
     */
    public static function channels(Builder $c): void
    {
    }

    /** The resolved declaration, built once per class. */
    public static function spec(): ClassSpec
    {
        return self::$cache[static::class] ??= static::buildSpec(static::declaration());
    }

    /** The `#[Operative]` attribute, refusing a class that carries none. */
    protected static function declaration(): OperativeAttribute
    {
        // Not inherited on purpose: two operatives cannot share a factory id,
        // and PHP attributes are per class, which is the behaviour this wants.
        $attributes = (new ReflectionClass(static::class))->getAttributes(OperativeAttribute::class);
        if ($attributes === []) {
            throw new DeclarationException(static::class, null, sprintf(
                'declares no #[%s(factoryId: ...)]',
                OperativeAttribute::class,
            ));
        }

        return $attributes[0]->newInstance();
    }

    /** The `#[Workflow]` attribute, or its defaults. */
    protected static function workflowDeclaration(): WorkflowAttribute
    {
        $attributes = (new ReflectionClass(static::class))->getAttributes(WorkflowAttribute::class);

        return $attributes === [] ? new WorkflowAttribute() : $attributes[0]->newInstance();
    }

    protected static function builder(OperativeAttribute $declaration): Builder
    {
        $builder = new Builder($declaration->factoryId);
        static::channels($builder);

        return $builder;
    }

    abstract protected static function buildSpec(OperativeAttribute $declaration): ClassSpec;
}
