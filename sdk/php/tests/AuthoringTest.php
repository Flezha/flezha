<?php

declare(strict_types=1);

namespace Flezha\Tests;

use Flezha\AckGranularity;
use Flezha\Attribute\Operative;
use Flezha\Attribute\Proc;
use Flezha\CheckpointOutputs;
use Flezha\Clock;
use Flezha\Ctx;
use Flezha\Definition;
use Flezha\Exception\DeclarationException;
use Flezha\Graph\Builder;
use Flezha\Graph\Validate;
use Flezha\Overflow;
use Flezha\Procedure;
use Flezha\StreamOperative;
use Flezha\WorkflowOperative;
use PHPUnit\Framework\TestCase;

#[Proc(clock: Clock::EventTime)]
final class NoExtractor extends Procedure
{
    public function run(mixed $input, Ctx $ctx): mixed
    {
        return null;
    }
}

#[Proc(entityKeys: ['a'])]
final class NoEntitiesFor extends Procedure
{
    public function run(mixed $input, Ctx $ctx): mixed
    {
        return null;
    }
}

#[Proc(entityKeys: ['a'], dynamicEntities: true)]
final class BothModes extends Procedure
{
    public function run(mixed $input, Ctx $ctx): mixed
    {
        return null;
    }
}

#[Proc]
final class FoldWithoutState extends Procedure
{
    public function applyChange(mixed &$state, mixed $change): void
    {
    }

    public function run(mixed $input, Ctx $ctx): mixed
    {
        return null;
    }
}

/**
 * Everything a type system would have caught, refused at registration and
 * naming the class, the handler and the defect.
 *
 * The same set as the Rust builder's `TopologyIssue`, so an author moving
 * between SDKs meets one vocabulary rather than two.
 */
final class AuthoringTest extends TestCase
{
    /** @return iterable<string, array{callable, string}> */
    public static function refusals(): iterable
    {
        yield 'an event-time clock with no extractor' => [
            static fn (): mixed => self::validate(static function (Builder $c): void {
                $c->input('in')->into(NoExtractor::class)->sink('out');
            }),
            'declares an event_time clock but no eventTime method',
        ];
        yield 'a static key set with no entitiesFor' => [
            static fn (): mixed => self::validate(static function (Builder $c): void {
                $c->input('in')->into(NoEntitiesFor::class)->sink('out');
            }),
            'declares a static key set without an entitiesFor',
        ];
        yield 'both entity modes' => [
            static fn (): mixed => Definition::of(BothModes::class),
            'declares both a static key set and dynamic entities',
        ];
        yield 'a fold with nothing to fold into' => [
            static fn (): mixed => Definition::of(FoldWithoutState::class),
            'declares applyChange but no state',
        ];
        yield 'an entry channel with no consumer' => [
            static fn (): mixed => self::validate(static function (Builder $c): void {
                $c->input('in');
            }),
            'is an entry channel that names no consumer',
        ];
        yield 'a keyed channel with no plugin wire' => [
            static fn (): mixed => self::validate(static function (Builder $c): void {
                $c->input('in')->into(Echoer::class)->named('hop')->keyed()->into(Counter::class);
            }),
            'is declared keyed but has no plugin wire',
        ];
        yield 'a dynamic consumer with no key' => [
            static fn (): mixed => self::validate(static function (Builder $c): void {
                $c->input('in')->into(Keyed::class)->sink('out');
            }),
            'without a keyBy',
        ];
        yield 'shedding on an effectively-once class' => [
            static fn (): mixed => self::validate(static function (Builder $c): void {
                $c->effectivelyOnce();
                $c->input('in')->onFull(Overflow::DropOldest)->into(Echoer::class)->keyed()->sink('out');
            }),
            'declares onFull(drop_oldest) on a class that declares effectivelyOnce',
        ];
        yield 'an unkeyed plugin wire on an effectively-once class' => [
            static fn (): mixed => self::validate(static function (Builder $c): void {
                $c->effectivelyOnce();
                $c->input('in')->into(Echoer::class)->sink('out');
            }),
            'must opt in with keyed',
        ];
        yield 'a zero retry budget' => [
            static fn (): mixed => self::validate(static function (Builder $c): void {
                $c->input('in')->retryMax(0)->into(Echoer::class)->sink('out');
            }),
            'declares retryMax(0)',
        ];
        yield 'a correction window without the tier' => [
            static fn (): mixed => self::validate(static function (Builder $c): void {
                $c->correctionWindow(5_000);
                $c->input('in')->into(Echoer::class)->sink('out');
            }),
            'declares a correction window without effectivelyOnce',
        ];
        yield 'held outputs with no maximum delay' => [
            static fn (): mixed => self::validate(static function (Builder $c): void {
                $c->effectivelyOnce();
                $c->outputsAtCheckpoint();
                $c->input('in')->into(Echoer::class)->keyed()->sink('out');
            }),
            'declares outputsAtCheckpoint with no maximum delay',
        ];
        foreach ([
            'checkpointAck' => static fn (Builder $c): mixed => $c->checkpointAck(),
            'checkpointEvery' => static fn (Builder $c): mixed => $c->checkpointEvery(4),
            'outputsAtCheckpoint' => static fn (Builder $c): mixed => $c->outputsAtCheckpoint(250),
        ] as $option => $declare) {
            yield "$option without the tier" => [
                static fn (): mixed => self::validate(static function (Builder $c) use ($declare): void {
                    $declare($c);
                    $c->input('in')->into(Echoer::class)->keyed()->sink('out');
                }),
                'declares checkpoint acknowledgement without effectivelyOnce',
            ];
        }
        yield 'a branch with one arm' => [
            static fn (): mixed => self::validate(static function (Builder $c): void {
                $c->input('in')->into(Echoer::class)->branch(static fn ($e) => $e->sink('out'));
            }),
            'branch(...) requires at least two arms',
        ];
        yield 'a sink fed by an entry channel' => [
            static fn (): mixed => self::validate(static function (Builder $c): void {
                $c->input('in')->sink('out');
            }),
            'a plugin sink is fed by a procedure',
        ];
    }

    /** @dataProvider refusals */
    public function testTheDeclarationIsRefusedWithItsDefectNamed(callable $declare, string $defect): void
    {
        $this->expectException(DeclarationException::class);
        $this->expectExceptionMessage($defect);

        $declare();
    }

    public function testAnOperativeWithoutTheAttributeIsRefused(): void
    {
        $this->expectException(DeclarationException::class);
        $this->expectExceptionMessage('declares no #[Flezha\Attribute\Operative(factoryId: ...)]');

        Unattributed::spec();
    }

    public function testAWorkflowDeclaringNeitherFormIsRefused(): void
    {
        $this->expectException(DeclarationException::class);
        $this->expectExceptionMessage('declares neither a run body nor a channels graph');

        EmptyWorkflow::spec();
    }

    /**
     * Two unnamed mentions of one procedure are one node, so a diamond that
     * fans out and back in has one instance rather than two.
     */
    public function testOneProcedureMentionedTwiceIsOneNode(): void
    {
        $spec = Diamond::spec();
        Validate::spec($spec);

        self::assertSame(['echoer', 'counter'], array_map(
            static fn ($node): string => $node->name,
            $spec->procedures,
        ));
    }

    /**
     * `named()` is honoured on the inter-proc path and was dropped on the
     * plugin path, so `named('input')->sink('relay_out')` produced a channel
     * called `relay_out` — and the channel name is what a forwarded frame
     * carries and what selects the receiving operative's entry channel.
     */
    public function testANamedEdgeTerminatingOnASinkNamesTheChannelToo(): void
    {
        $spec = NamedSink::spec();
        Validate::spec($spec);

        self::assertSame(['in', 'input'], array_map(
            static fn ($channel): string => $channel->name,
            $spec->channels,
        ));
    }

    /**
     * Compatibility, not conformance: `sdk-graph-authoring` says an underived
     * name comes from the PRODUCER NODE, and these veneers use the sink's. That
     * second divergence is open and is not what this change moves — this test
     * pins that it moves nothing but the named case.
     */
    public function testAnUnnamedEdgeTerminatingOnASinkKeepsTheNameItHasToday(): void
    {
        $spec = UnnamedSink::spec();
        Validate::spec($spec);

        self::assertSame(['in', 'relay_out'], array_map(
            static fn ($channel): string => $channel->name,
            $spec->channels,
        ));
    }

    /** Validate a graph declared inline. */
    /**
     * PHP does not inherit attributes, and taking that literally would make
     * `class Sub extends Base {}` silently single-entity with the default
     * capacity and schema version 1 — wrong at runtime, reported nowhere.
     */
    public function testASubclassKeepsEveryDeclarationItsParentMade(): void
    {
        $definition = Definition::of(SilentSubclass::class);

        self::assertSame('static', $definition->entityMode());
        self::assertSame(['a', 'b'], $definition->decl->entityKeys);
        self::assertSame(8, $definition->decl->mailboxCapacity);
        self::assertSame(3, $definition->decl->schemaVersion);
    }

    /**
     * The one declaration that is NOT inherited: a node identity is per class,
     * and an inherited one would make two procedures claim one node.
     */
    public function testASubclassIsNamedAfterItselfRatherThanItsParent(): void
    {
        self::assertSame('declaring_base', Definition::of(DeclaringBase::class)->name);
        self::assertSame('silent_subclass', Definition::of(SilentSubclass::class)->name);
        self::assertSame('renamed', Definition::of(RenamingSubclass::class)->name);
    }

    /**
     * The common case, and the one whole-attribute inheritance gets wrong: a
     * subclass that renames the node and changes nothing else keeps the key
     * set, the capacity and the schema version its parent declared.
     */
    public function testRenamingASubclassDropsNothingElse(): void
    {
        $definition = Definition::of(RenamingSubclass::class);

        self::assertSame('static', $definition->entityMode());
        self::assertSame(['a', 'b'], $definition->decl->entityKeys);
        self::assertSame(8, $definition->decl->mailboxCapacity);
        self::assertSame(3, $definition->decl->schemaVersion);
    }

    public function testTheCheckpointOptionsComposeInAnyOrderAndNoneClearsAnother(): void
    {
        $builder = new Builder('compose');
        $builder->effectivelyOnce();
        $builder->outputsAtCheckpoint(250);
        $builder->checkpointAck();
        $builder->checkpointEvery(4);

        self::assertSame(AckGranularity::Checkpoint, $builder->ackGranularity);
        self::assertSame(4, $builder->checkpointEvery);
        self::assertSame(CheckpointOutputs::AtCheckpoint, $builder->checkpointOutputs);
        self::assertSame(250, $builder->checkpointMaxDelayMs);
    }

    private static function validate(callable $channels): mixed
    {
        $builder = new Builder('under_test');
        $channels($builder);

        Validate::spec(new \Flezha\Graph\ClassSpec(
            factoryId: 'under_test',
            kind: 'stream',
            persistent: true,
            effectivelyOnce: $builder->effectivelyOnce,
            ackGranularity: $builder->ackGranularity,
            checkpointEvery: $builder->checkpointEvery,
            checkpointOutputs: $builder->checkpointOutputs,
            checkpointMaxDelayMs: $builder->checkpointMaxDelayMs,
            correctionWindowMs: $builder->correctionWindowMs,
            beyondWindowPolicy: $builder->beyondWindowPolicy,
            compactAfterBytes: $builder->compactAfterBytes,
            syncDurability: $builder->syncDurability,
            procedures: $builder->procedures,
            channels: $builder->channels,
        ));

        return null;
    }
}

final class Unattributed extends StreamOperative
{
}

#[Operative(factoryId: 'empty_workflow')]
final class EmptyWorkflow extends WorkflowOperative
{
}

#[Operative(factoryId: 'named_sink')]
final class NamedSink extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('in')->into(Echoer::class)->named('input')->sink('relay_out');
    }
}

#[Operative(factoryId: 'unnamed_sink')]
final class UnnamedSink extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('in')->into(Counter::class)->sink('relay_out');
    }
}

#[Operative(factoryId: 'diamond')]
final class Diamond extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $edge = $c->input('in')->into(Echoer::class);
        $edge->named('left')->into(Counter::class);
        $edge->named('right')->sink('out');
    }
}

#[Proc(entityKeys: ['a', 'b'], mailboxCapacity: 8, schemaVersion: 3)]
class DeclaringBase extends Procedure
{
    public function entitiesFor(mixed $input): array
    {
        return ['a'];
    }

    public function run(mixed $input, Ctx $ctx): mixed
    {
        return $input;
    }
}

/** Declares nothing of its own: everything but the node name is the parent's. */
final class SilentSubclass extends DeclaringBase
{
}

#[Proc(name: 'renamed')]
final class RenamingSubclass extends DeclaringBase
{
}
