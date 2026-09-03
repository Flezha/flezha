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

    /**
     * The declaration both kinds carry, assembled once. A stream class names
     * only what the builder holds; a workflow adds its own attribute and, for
     * a run-sugar body, the callable.
     *
     * @param (callable(mixed, WorkflowCtx):mixed)|null $run
     */
    protected static function assemble(
        OperativeAttribute $declaration,
        Builder $builder,
        string $kind,
        bool $syncDurability,
        ?WorkflowAttribute $workflow = null,
        ?\Closure $run = null,
    ): ClassSpec {
        return new ClassSpec(
            factoryId: $declaration->factoryId,
            kind: $kind,
            persistent: $declaration->persistent,
            effectivelyOnce: $builder->effectivelyOnce,
            ackGranularity: $builder->ackGranularity,
            checkpointEvery: $builder->checkpointEvery,
            checkpointOutputs: $builder->checkpointOutputs,
            checkpointMaxDelayMs: $builder->checkpointMaxDelayMs,
            correctionWindowMs: $builder->correctionWindowMs,
            beyondWindowPolicy: $builder->beyondWindowPolicy,
            compactAfterBytes: $builder->compactAfterBytes,
            syncDurability: $syncDurability,
            procedures: $builder->procedures,
            channels: $builder->channels,
            workflow: $workflow,
            run: $run,
            chainIssues: $builder->chainIssues(),
        );
    }

    abstract protected static function buildSpec(OperativeAttribute $declaration): ClassSpec;
}
