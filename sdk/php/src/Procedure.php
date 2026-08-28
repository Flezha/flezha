<?php

declare(strict_types=1);

namespace Flezha;

use Flezha\Attribute\Proc;

/**
 * The base every authored procedure extends.
 *
 * A `#[Proc]` attribute carries the scalar declarations; the methods carry the
 * behaviour. Only {@see run()} is required — the rest are hooks with a default
 * that says "this procedure does not do that", and overriding one IS the
 * declaration that it does, so a declaration and its implementation cannot
 * disagree.
 *
 *     #[Proc(entityKeys: ['a', 'b'])]
 *     final class Totals extends Procedure
 *     {
 *         public function state(?string $entityKey): mixed { return ['sum' => 0]; }
 *         public function applyChange(mixed &$state, mixed $change): void { $state['sum'] += $change; }
 *         public function run(mixed $input, Ctx $ctx): mixed { $ctx->change((int) $input); return null; }
 *     }
 */
abstract class Procedure
{
    /** What one input does. The only method a procedure must write. */
    abstract public function run(mixed $input, Ctx $ctx): mixed;

    /**
     * An entity's state before anything has happened to it.
     *
     * Override to declare that this procedure is stateful. `$entityKey` is
     * null for a single-entity procedure and at the representability check.
     */
    public function state(?string $entityKey): mixed
    {
        return null;
    }

    /**
     * Fold one recorded change into the state, in place.
     *
     * The change log IS how state is rebuilt, so this must be pure: a live
     * fold and a replayed fold have to reach the same state, and anything read
     * here that is not `$state` or `$change` will differ between the two.
     */
    public function applyChange(mixed &$state, mixed $change): void
    {
    }

    /** A fired timer. Override to declare that this procedure registers them. */
    public function onTimer(Timer $timer, Ctx $ctx): void
    {
    }

    /**
     * Which of a static key set's entities an input belongs to. Override on a
     * procedure declaring `entityKeys`.
     *
     * @return list<string>
     */
    public function entitiesFor(mixed $input): array
    {
        return [];
    }

    /** The input's event time. Override with `Clock::EventTime`. */
    public function eventTime(mixed $input): int
    {
        return 0;
    }

    /**
     * Rebuild a State written at an older schema version.
     *
     * Override alongside `#[Proc(minReadableSchemaVersion: ...)]`: the core
     * refuses everything below that floor and hands the rest across tagged
     * with the version it was written at, so this decides nothing about
     * whether the record is readable — only how.
     */
    public function migrateState(int $fromVersion, string $bytes): mixed
    {
        return null;
    }

    /** The same for a recorded Change. */
    public function migrateChange(int $fromVersion, string $bytes): mixed
    {
        return null;
    }

    /**
     * Undo this procedure's committed effects, in a workflow chain the driver
     * is rolling back.
     *
     * @param list<mixed> $outputs the forward outputs the core recorded
     */
    public function compensate(array $outputs, Ctx $ctx): void
    {
    }
}
