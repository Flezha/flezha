<?php

declare(strict_types=1);

namespace Flezha\Graph;

use Flezha\AckGranularity;
use Flezha\CheckpointOutputs;
use Flezha\Clock;
use Flezha\Exception\DeclarationException;
use Flezha\Overflow;

/**
 * Registration-time refusals.
 *
 * PHP has no compile step, so everything the Rust builder refuses at compile
 * time is refused here instead, naming the class, the handler and the specific
 * defect — the same set, for the same reasons, as the Rust builder's
 * `TopologyIssue`, so an author moving between SDKs meets one vocabulary.
 *
 * The core validates again on its side. That is not redundancy: a hand-written
 * declaration reaching the ABI directly must not slip past, and an author who
 * declared through this surface deserves the error before any data moves.
 */
final class Validate
{
    public const MAX_MAILBOX_CAPACITY = 1_048_576;

    private function __construct()
    {
    }

    public static function spec(ClassSpec $spec): void
    {
        self::workflow($spec);
        self::tier($spec);
        self::window($spec);
        self::channels($spec);
        self::procedures($spec);
        self::keying($spec);
        self::topology($spec);
    }

    private static function refuse(ClassSpec $spec, ?string $handler, string $defect): never
    {
        throw new DeclarationException($spec->factoryId, $handler, $defect);
    }

    private static function channels(ClassSpec $spec): void
    {
        $names = [];

        foreach ($spec->channels as $channel) {
            if (isset($names[$channel->name])) {
                self::refuse($spec, $channel->name, 'this channel name is declared more than once');
            }
            $names[$channel->name] = true;
            self::channelWiring($spec, $channel);
            self::overflow($spec, $channel);

            if ($channel->kind === 'entry' && $channel->consumers === []) {
                self::refuse($spec, $channel->name, 'is an entry channel that names no consumer');
            }
            if (self::declaredKind($channel) === 'inter_proc' && $channel->consumers === []) {
                self::refuse(
                    $spec,
                    $channel->name,
                    'is an inter-procedure channel that names no consumer'
                );
            }
        }
    }

    /**
     * The kind the wire carries.
     *
     * Terminality is derived from the wiring, not from which method the author
     * called: a sink with nothing consuming behind it IS the plugin-bound
     * terminal, and a sink declared alongside consumers is the hybrid — one
     * channel the core journals as inter-procedure and whose values also reach
     * the plugin.
     */
    public static function declaredKind(ChannelSpec $channel): string
    {
        return $channel->sink !== null && $channel->consumers === [] ? 'plugin' : $channel->kind;
    }

    private static function channelWiring(ClassSpec $spec, ChannelSpec $channel): void
    {
        if ($channel->sink === null && $channel->keyed) {
            self::refuse(
                $spec,
                $channel->name,
                'is declared keyed but has no plugin wire; output identity stamps only exist '
                . 'on plugin-bound frames, so add a sink(...) or drop the keyed'
            );
        }

        if ($spec->effectivelyOnce && $channel->sink !== null && !$channel->keyed) {
            self::refuse(
                $spec,
                $channel->name,
                'has a plugin wire but is not declared keyed; the effectively-once tier '
                . 'deduplicates by output identity, so every plugin-bound channel of a class '
                . 'that declares effectivelyOnce must opt in with keyed'
            );
        }
    }

    private static function overflow(ClassSpec $spec, ChannelSpec $channel): void
    {
        if ($channel->overflow !== Overflow::DropNothing) {
            if ($spec->effectivelyOnce) {
                self::refuse($spec, $channel->name, sprintf(
                    'declares onFull(%s) on a class that declares effectivelyOnce; a shed '
                    . "input's offset is still advanced past by the tier's watermark, so the "
                    . 'loss is permanent and reported nowhere — declare one or the other, not both',
                    $channel->overflow->value,
                ));
            }
            if ($spec->correctionWindowMs !== null) {
                self::refuse($spec, $channel->name, sprintf(
                    'declares onFull(%s) on a class that declares a correction window; a '
                    . 'correction re-folds a replayed slice and its answer may not depend on '
                    . 'which items a full mailbox happened to keep',
                    $channel->overflow->value,
                ));
            }
        }

        if ($channel->retryMax !== 0) {
            return;
        }
        self::refuse(
            $spec,
            $channel->name,
            'declares retryMax(0); at least one attempt is required — raise a '
            . 'non-retryable error to park without retries'
        );
    }

    private static function keying(ClassSpec $spec): void
    {
        foreach ($spec->channels as $channel) {
            foreach ($channel->consumers as $edge) {
                $node = $spec->procedures[$edge->procedure];
                $mode = $node->definition->entityMode();

                if ($edge->keyBy !== null && $mode === 'static') {
                    self::refuse($spec, $channel->name, sprintf(
                        'declares keyBy into procedure `%s`, which declares a static key set; '
                        . 'static keying routes by entitiesFor and may name several entities, '
                        . 'edge keying names exactly one — declare one or the other, not both',
                        $node->name,
                    ));
                }
                if ($edge->keyBy === null && $mode === 'dynamic') {
                    self::refuse($spec, $channel->name, sprintf(
                        'feeds dynamic procedure `%s` without a keyBy; every edge into a dynamic '
                        . 'procedure must name the entity its inputs reach',
                        $node->name,
                    ));
                }
            }
        }
    }

    private static function procedures(ClassSpec $spec): void
    {
        foreach ($spec->procedures as $node) {
            self::clocks($spec, $node);
            self::entities($spec, $node);
            self::schema($spec, $node);
        }
    }

    private static function clocks(ClassSpec $spec, ProcNode $node): void
    {
        $definition = $node->definition;
        $clock = $definition->decl->clock;

        if ($clock === Clock::EventTime && !$definition->declares('eventTime')) {
            self::refuse(
                $spec,
                $node->name,
                'declares an event_time clock but no eventTime method; the core evaluates '
                . 'the declared clock per input and has nothing to evaluate'
            );
        }
        if ($clock !== Clock::EventTime && $definition->declares('eventTime')) {
            self::refuse($spec, $node->name, sprintf(
                'declares an eventTime method with clock `%s`; the extractor is only '
                . 'consulted by an event_time clock, so one of the two is not what was meant',
                $clock->value,
            ));
        }
        if ($clock === Clock::Logical && !$definition->declares('state')) {
            self::refuse(
                $spec,
                $node->name,
                'declares a logical clock but no state; logical time IS the durable change '
                . 'sequence, which never advances for a procedure that records no changes'
            );
        }
    }

    private static function entities(ClassSpec $spec, ProcNode $node): void
    {
        $definition = $node->definition;

        if ($definition->entityMode() === 'static' && !$definition->declares('entitiesFor')) {
            self::refuse(
                $spec,
                $node->name,
                'declares a static key set without an entitiesFor; static keying needs the '
                . 'method that says which declared entities an input belongs to'
            );
        }

        $capacity = $definition->decl->mailboxCapacity;
        if ($capacity !== null && ($capacity < 1 || $capacity > self::MAX_MAILBOX_CAPACITY)) {
            self::refuse($spec, $node->name, sprintf(
                'declares mailboxCapacity %d; capacity must be in 1..%d',
                $capacity,
                self::MAX_MAILBOX_CAPACITY,
            ));
        }
        if ($definition->decl->maxEntities === 0) {
            self::refuse(
                $spec,
                $node->name,
                'declares maxEntities 0; a dynamic procedure that may hold no entity can '
                . 'process nothing'
            );
        }
    }

    private static function schema(ClassSpec $spec, ProcNode $node): void
    {
        $definition = $node->definition;
        $decl = $definition->decl;
        $floor = $decl->minReadableSchemaVersion;
        if ($floor === null) {
            return;
        }

        if ($floor > $decl->schemaVersion) {
            self::refuse($spec, $node->name, sprintf(
                'declares minReadableSchemaVersion %d above its schemaVersion %d',
                $floor,
                $decl->schemaVersion,
            ));
        }
        if ($floor < $decl->schemaVersion
            && !$definition->declares('migrateState')
            && !$definition->declares('migrateChange')) {
            self::refuse($spec, $node->name, sprintf(
                'declares it reads back to schema version %d but supplies no migration; a '
                . 'floor without one fails at the first old record instead of at startup',
                $floor,
            ));
        }
    }

    private static function tier(ClassSpec $spec): void
    {
        if ($spec->effectivelyOnce && !$spec->persistent) {
            self::refuse(
                $spec,
                null,
                'declares effectivelyOnce without persistence: the tier IS durable state'
            );
        }
        if ($spec->ackGranularity === AckGranularity::Checkpoint && !$spec->effectivelyOnce) {
            self::refuse(
                $spec,
                null,
                'declares checkpoint acknowledgement without effectivelyOnce; the checkpoint is '
                . "where the tier's consumed offset advances, and a class without the tier has "
                . 'no durable offset to hold back'
            );
        }
        if ($spec->checkpointEvery === 0) {
            self::refuse(
                $spec,
                null,
                'declares checkpointEvery(0); an interval that closes every zero inputs is not '
                . 'an interval — omit it to close only on an explicit checkpoint'
            );
        }
        if ($spec->checkpointOutputs === CheckpointOutputs::AtCheckpoint
            && $spec->checkpointMaxDelayMs === null) {
            self::refuse(
                $spec,
                null,
                'declares outputsAtCheckpoint with no maximum delay; an interval that never '
                . 'closes would hold its outputs forever on a quiet entity'
            );
        }
    }

    private static function window(ClassSpec $spec): void
    {
        if ($spec->correctionWindowMs === null) {
            return;
        }
        if ($spec->correctionWindowMs === 0) {
            self::refuse(
                $spec,
                null,
                'declares a correction window of zero; a zero window can correct nothing'
            );
        }
        if (!$spec->effectivelyOnce) {
            self::refuse(
                $spec,
                null,
                'declares a correction window without effectivelyOnce; correction replays the '
                . 'input log against a rewound baseline, and only the tier journals inputs '
                . 'before forwarding them'
            );
        }
        foreach ($spec->procedures as $node) {
            if ($node->definition->decl->clock === Clock::EventTime) {
                return;
            }
        }
        self::refuse(
            $spec,
            null,
            'declares a correction window but no procedure declares an event_time clock; '
            . 'correction orders a replayed slice by event time'
        );
    }

    private static function topology(ClassSpec $spec): void
    {
        $entries = array_values(array_filter(
            $spec->channels,
            static fn (ChannelSpec $c): bool => $c->kind === 'entry',
        ));

        if ($spec->kind === 'stream' && $entries === []) {
            self::refuse(
                $spec,
                null,
                'declares no entry channel; a stream class is fed by the controller'
            );
        }
        if ($spec->procedures === []) {
            if ($spec->kind === 'stream') {
                self::refuse($spec, null, 'declares no procedure');
            }

            return;
        }

        self::dag($spec, $entries, self::successorsOf($spec));
    }

    /**
     * Which procedures each procedure feeds, by index.
     *
     * @return list<list<int>>
     */
    private static function successorsOf(ClassSpec $spec): array
    {
        $successors = array_fill(0, count($spec->procedures), []);
        foreach ($spec->channels as $channel) {
            if ($channel->kind !== 'inter_proc' || $channel->producer === null) {
                continue;
            }
            array_push($successors[$channel->producer], ...self::consumerIndices($channel));
        }

        return $successors;
    }

    /**
     * The procedure indices a channel feeds, in declaration order.
     *
     * @return list<int>
     */
    private static function consumerIndices(ChannelSpec $channel): array
    {
        return array_map(static fn (ConsumerEdge $e): int => $e->procedure, $channel->consumers);
    }

    /**
     * Every procedure reachable from an entry channel, and no cycle among them.
     *
     * @param list<ChannelSpec> $entries
     * @param list<list<int>>   $successors
     */
    private static function dag(ClassSpec $spec, array $entries, array $successors): void
    {
        $reachable = [];
        $frontier = [];
        foreach ($entries as $entry) {
            array_push($frontier, ...self::consumerIndices($entry));
        }
        while ($frontier !== []) {
            $node = array_pop($frontier);
            if (isset($reachable[$node])) {
                continue;
            }
            $reachable[$node] = true;
            array_push($frontier, ...$successors[$node]);
        }

        // A cycle is found before unreachability is reported: a node on a
        // cycle with no path from an entry is both, and the cycle is the more
        // useful answer.
        $colour = [];
        $visit = static function (int $index) use (&$visit, &$colour, $successors, $spec): void {
            $seen = $colour[$index] ?? null;
            if ($seen === 'black') {
                return;
            }
            if ($seen === 'grey') {
                self::refuse(
                    $spec,
                    $spec->procedures[$index]->name,
                    'is on a cycle; an operative topology is a DAG — a node\'s output may never '
                    . 'reach its own input'
                );
            }
            $colour[$index] = 'grey';
            foreach ($successors[$index] as $next) {
                $visit($next);
            }
            $colour[$index] = 'black';
        };
        foreach ($spec->procedures as $node) {
            $visit($node->index);
        }

        foreach ($spec->procedures as $node) {
            if (!isset($reachable[$node->index])) {
                self::refuse($spec, $node->name, 'is not reachable from any entry channel');
            }
        }
    }

    private static function workflow(ClassSpec $spec): void
    {
        if ($spec->kind !== 'workflow') {
            if ($spec->syncDurability) {
                self::refuse(
                    $spec,
                    null,
                    'declares syncDurability on a stream class; a stream class acknowledges at '
                    . 'input rates, and a remote round trip per acknowledgement is not a '
                    . 'guarantee it can keep'
                );
            }

            return;
        }

        if ($spec->correctionWindowMs !== null) {
            self::refuse($spec, null, 'is a workflow class and cannot declare a correction window');
        }
        if ($spec->run !== null && $spec->channels !== []) {
            self::refuse(
                $spec,
                null,
                'declares a run body and channels; a run-sugar workflow is one imperative body'
            );
        }
        if ($spec->syncDurability && !$spec->persistent) {
            self::refuse($spec, null, 'declares syncDurability without persistence');
        }
    }
}
