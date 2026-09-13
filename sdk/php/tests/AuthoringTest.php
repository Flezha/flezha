<?php

declare(strict_types=1);

namespace Flezha\Tests;

use Flezha\Attribute\Operative;
use Flezha\Attribute\Proc;
use Flezha\CheckpointOutputs;
use Flezha\Clock;
use Flezha\Ctx;
use Flezha\Definition;
use Flezha\Exception\DeclarationException;
use Flezha\Ffi\Core;
use Flezha\Graph\Builder;
use Flezha\Graph\ClassSpec;
use Flezha\Graph\Edge;
use Flezha\Graph\HostClassDecl;
use Flezha\Graph\Validate;
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
 * What this package's authoring surface answers, and what it hands the core.
 *
 * The topology itself is judged once, in `flz_core::host::register::check`, and
 * the reference refusals are pinned by `test/fixtures/reference_topologies.txt`
 * — not here. What is left here is the ergonomics: which shapes the fluent
 * builder BUILDS, the defects a record cannot carry (a PHP class that does not
 * define the method its declaration promises, a plugin sink on a channel no
 * procedure produces on), and that the core's verdict reaches an author as this
 * package's own refusal.
 */
final class AuthoringTest extends TestCase
{
    /** @return iterable<string, array{callable, string}> */
    public static function refusals(): iterable
    {
        // A clock is a promise about a method, and a record carries the clock
        // but not the class: only this package can see the method is missing.
        yield 'an event-time clock with no extractor' => [
            static fn (): mixed => self::validate(static function (Builder $c): void {
                $c->input('in')->into(NoExtractor::class)->sink();
            }),
            'declares an event_time clock but no eventTime method',
        ];
        yield 'both entity modes' => [
            static fn (): mixed => Definition::of(BothModes::class),
            'declares both a static key set and dynamic entities',
        ];
        yield 'a fold with nothing to fold into' => [
            static fn (): mixed => Definition::of(FoldWithoutState::class),
            'declares applyChange but no state',
        ];
        // The other refusal the builder keeps: a channel with no producer has
        // no value to serialize, and the core reads a sink as the terminal of
        // the channel it sits on — it cannot see this one is controller-fed.
        yield 'a sink fed by an entry channel' => [
            static fn (): mixed => self::validate(static function (Builder $c): void {
                $c->input('in')->sink();
            }),
            'a plugin sink is fed by a procedure',
        ];
        // The one refusal the builder raises under the core's vocabulary reads
        // the way the core's own verdicts do.
        yield 'two procedures claiming one node identity' => [
            static fn (): mixed => self::validate(static function (Builder $c): void {
                $in = $c->input('in');
                $in->into(Echoer::class, 'twin');
                $in->into(Keyed::class, 'twin');
            }),
            'topology rejected [NodeIdentityCollision]',
        ];
        // `Validate::spec` IS the submission: the record crosses the ABI and
        // the core's verdict comes back as this package's own refusal, in the
        // core's vocabulary.
        yield 'the core verdict, rendered here' => [
            static fn (): mixed => self::validate(static function (Builder $c): void {
                $c->input('in')
                    ->keyBy(static fn (mixed $v): string => 'a')
                    ->keyBy(static fn (mixed $v): string => 'b')
                    ->into(Keyed::class);
            }),
            'topology rejected [DuplicateKeyBy]',
        ];
    }

    /** @dataProvider refusals */
    public function testTheDeclarationIsRefusedWithItsDefectNamed(callable $declare, string $defect): void
    {
        $this->expectException(DeclarationException::class);
        $this->expectExceptionMessage($defect);

        $declare();
    }

    /**
     * And the same verdict as data: the core hands its refusal names out
     * beside the rendered message, so nothing here reads them out of prose.
     */
    public function testARefusedDeclarationCarriesTheCoresOwnNames(): void
    {
        try {
            self::validate(static function (Builder $c): void {
                $c->input('in')
                    ->keyBy(static fn (mixed $v): string => 'a')
                    ->keyBy(static fn (mixed $v): string => 'b')
                    ->into(Keyed::class);
            });
            self::fail('the core refuses two keys before one into');
        } catch (DeclarationException $e) {
            self::assertSame(['DuplicateKeyBy'], $e->refusals);
            self::assertStringStartsWith("under_test: topology rejected [DuplicateKeyBy]:\n  - ", $e->getMessage());
        }
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
        self::derived($spec);

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
        self::derived($spec);

        self::assertSame(['in', 'input'], array_map(
            static fn ($channel): string => $channel->name,
            $spec->channels,
        ));
    }

    /**
     * `sdk-graph-authoring`: the no-argument `sink()` names nothing, so the
     * channel keeps the PRODUCER NODE's name — the derivation a channel with
     * no sink at all would take.
     */
    public function testAnUnnamedEdgeTerminatingOnASinkTakesTheProducingNodesName(): void
    {
        $spec = UnnamedSink::spec();
        self::derived($spec);

        self::assertSame(['in', 'counter'], array_map(
            static fn ($channel): string => $channel->name,
            $spec->channels,
        ));
    }

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

    /**
     * The three checkpoint options each write their own raw flag now — the
     * granularity is the core's to imply — so none may clear another.
     */
    public function testTheCheckpointOptionsComposeInAnyOrderAndNoneClearsAnother(): void
    {
        $builder = new Builder('compose');
        $builder->effectivelyOnce();
        $builder->outputsAtCheckpoint(250);
        $builder->checkpointAck();
        $builder->checkpointEvery(4);

        self::assertTrue($builder->checkpointAck);
        self::assertSame(4, $builder->checkpointEvery);
        self::assertSame(CheckpointOutputs::AtCheckpoint, $builder->checkpointOutputs);
        self::assertSame(250, $builder->checkpointMaxDelayMs);
    }

    /**
     * Submit a graph declared inline, and answer the core's derived
     * declaration.
     *
     * A context of its own per call: `flz_core_check_class` registers nothing
     * and a context is a plain allocation, so a case pays one rather than
     * sharing a handle with the suites that make and free theirs.
     *
     * @return array<string, mixed>
     */
    private static function validate(callable $channels): array
    {
        $builder = new Builder('under_test');
        $channels($builder);
        $builder->seal();

        $core = Core::create();

        try {
            return Validate::spec(new ClassSpec(
                factoryId: 'under_test',
                kind: 'stream',
                persistent: true,
                effectivelyOnce: $builder->effectivelyOnce,
                checkpointAck: $builder->checkpointAck,
                checkpointEvery: $builder->checkpointEvery,
                checkpointOutputs: $builder->checkpointOutputs,
                checkpointMaxDelayMs: $builder->checkpointMaxDelayMs,
                correctionWindowMs: $builder->correctionWindowMs,
                beyondWindowPolicy: $builder->beyondWindowPolicy,
                compactAfterBytes: $builder->compactAfterBytes,
                syncDurability: $builder->syncDurability,
                procedures: $builder->procedures,
                channels: $builder->channels,
            ), $core);
        } finally {
            $core->free();
        }
    }

    /**
     * One declared class's verdict, with a context of its own.
     *
     * @return array<string, mixed>
     */
    private static function derived(ClassSpec $spec): array
    {
        $core = Core::create();

        try {
            return Validate::spec($spec, $core);
        } finally {
            $core->free();
        }
    }

    /** The sink names the channel; the no-argument form leaves it derived. */
    public function testTheSinkNamesTheChannelAndTheNoArgumentFormDoesNot(): void
    {
        self::assertSame(
            ['in', 'input'],
            array_column(self::derived(NamedSink::spec())['channels'], 'name'),
        );
        self::assertSame(
            ['in', 'counter'],
            array_column(self::derived(UnnamedSink::spec())['channels'], 'name'),
        );
    }

    /** A channel both `named()` and `sink()` name is refused, naming both. */
    public function testAChannelNamedTwiceIsRefusedNamingBoth(): void
    {
        $this->expectException(DeclarationException::class);
        $this->expectExceptionMessage('ChannelNamedTwice');

        self::derived(NamedTwice::spec());
    }

    /** A channel option with nothing wired after it still reaches the channel. */
    public function testAChannelOptionWithNothingWiredAfterItStillReachesTheChannel(): void
    {
        self::assertSame(
            ['in', 'renamed'],
            array_column(self::derived(TrailingNamed::spec())['channels'], 'name'),
        );
        $this->expectException(DeclarationException::class);
        $this->expectExceptionMessage('KeyedChannelWithoutPluginWire');

        self::derived(TrailingKeyed::spec());
    }

    /** Two channels naming one sink is a duplicate channel name. */
    public function testTwoChannelsNamingOneSinkIsRefused(): void
    {
        $this->expectException(DeclarationException::class);
        $this->expectExceptionMessage('DuplicateSinkName');

        self::derived(OneSinkTwoChannels::spec());
    }

    // -- one output channel per producing procedure --------------------------
    //
    // The model Rust and Java carry: a procedure node owns ONE output channel,
    // created on first wiring and reused after. A held edge wired twice appends
    // a second consumer to it — it does not declare a second channel claiming
    // the same producer, which the core resolves by first match
    // (`host/register.rs` `procedure_wiring`) and so silently truncates.

    /**
     * Read off the DERIVED declaration: the consumers, the plugin kind and the
     * sink name are what the core made of the chains this builder recorded.
     *
     * @return list<array{string, string, ?int, list<int>, ?string}>
     */
    private static function channelsOf(ClassSpec $spec): array
    {
        return array_map(
            static fn (array $c): array => [
                $c['name'],
                $c['kind'],
                $c['producer'],
                array_map(static fn (array $e): int => $e['procedure'], $c['consumers']),
                $c['sink'],
            ],
            self::derived($spec)['channels'],
        );
    }

    public function testAHeldEdgeWiredTwiceIsOneChannelWithTwoConsumers(): void
    {
        self::assertSame(
            [['in', 'entry', null, [0], null], ['counter', 'inter_proc', 0, [1, 2], null]],
            self::channelsOf(FanOut::spec()),
        );
    }

    /**
     * A re-declaration carrying neither transform nor key is the fan-in /
     * repeated-`into` case both references make a true no-op: one consumer
     * edge, not two, and not a refusal.
     */
    public function testABareReDeclaredEdgeIsANoOpNotASecondEdge(): void
    {
        $builder = null;
        self::validate(static function (Builder $c) use (&$builder): void {
            $edge = $c->input('in');
            $edge->into(Echoer::class);
            $edge->into(Echoer::class);
            $builder = $c;
        });

        self::assertSame([[0, 0]], array_map(
            static fn ($edge): array => [$edge->procedure, count($edge->transforms)],
            $builder->channels[0]->consumers,
        ));
    }

    public function testBranchAndTwoIntoStatementsBuildTheIdenticalDeclaration(): void
    {
        self::assertSame(self::channelsOf(FanOut::spec()), self::channelsOf(Branched::spec()));
    }

    public function testSinkReturnsTheEdgeSoAConsumerWiresBehindIt(): void
    {
        self::assertSame(
            [
                ['in', 'entry', null, [0], null],
                ['hybrid_indicator', 'inter_proc', 0, [1], 'hybrid_indicator'],
            ],
            self::channelsOf(Hybrid::spec()),
        );
    }

    /** `keyed()` reaches the channel it was declared on, and the record. */
    public function testKeyedReachesTheChannelOfAHybridSink(): void
    {
        $spec = KeyedHybrid::spec();
        self::derived($spec);

        self::assertTrue(HostClassDecl::of($spec)['channels'][1]['keyed']);
    }

    public function testATransformBeforeOneIntoReachesThatConsumerOnly(): void
    {
        $spec = PerEdgeTransform::spec();
        self::derived($spec);

        self::assertSame(
            [[1, 1], [2, 0]],
            array_map(
                static fn ($edge): array => [$edge->procedure, count($edge->transforms)],
                $spec->channels[1]->consumers,
            ),
        );
    }

    /**
     * The reference declares exactly this — `tier_commit_block_workflow` opens
     * with `c.effectively_once()` on a workflow class in Rust and Java — and
     * `Operative::classSpec` already reads the builder flag for both kinds, so
     * PHP needed no change for it. This pins that, and that the flag stays off
     * the wire: the core refuses `effectively_once` on a workflow class, and
     * Java's `HostClassDeclaration.forWorkflow` never writes it either.
     */
    public function testAWorkflowClassCarriesItsEffectivelyOnceDeclaration(): void
    {
        $spec = TierWorkflow::spec();
        self::derived($spec);

        self::assertTrue($spec->effectivelyOnce);
        self::assertArrayNotHasKey('effectively_once', HostClassDecl::of($spec));
    }
}

#[Operative(factoryId: 'fan_out')]
final class FanOut extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $edge = $c->input('in')->into(Counter::class);
        $edge->into(Echoer::class, 'b');
        $edge->into(Echoer::class, 'c');
    }
}

#[Operative(factoryId: 'branched')]
final class Branched extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('in')->into(Counter::class)->branch(
            static fn (Edge $e) => $e->into(Echoer::class, 'b'),
            static fn (Edge $e) => $e->into(Echoer::class, 'c'),
        );
    }
}

#[Operative(factoryId: 'hybrid')]
final class Hybrid extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('in')->into(Counter::class)
            ->sink('hybrid_indicator')->into(Echoer::class);
    }
}

#[Operative(factoryId: 'keyed_hybrid')]
final class KeyedHybrid extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('in')->into(Counter::class)->keyed()
            ->sink('mid')->into(Echoer::class);
    }
}

#[Operative(factoryId: 'per_edge_transform')]
final class PerEdgeTransform extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $edge = $c->input('in')->into(Counter::class);
        $edge->map(static fn (mixed $v): mixed => $v)->into(Echoer::class, 'b');
        $edge->into(Echoer::class, 'c');
    }
}

#[Operative(factoryId: 'tier_workflow')]
final class TierWorkflow extends WorkflowOperative
{
    public static function channels(Builder $c): void
    {
        $c->effectivelyOnce();
        $c->input('in')->into(Counter::class)->keyed()->sink('out');
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
        $c->input('in')->into(Echoer::class)->sink('input');
    }
}

#[Operative(factoryId: 'unnamed_sink')]
final class UnnamedSink extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('in')->into(Counter::class)->sink();
    }
}

#[Operative(factoryId: 'named_twice')]
final class NamedTwice extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('in')->into(Echoer::class)->named('a')->sink('b');
    }
}

#[Operative(factoryId: 'trailing_named')]
final class TrailingNamed extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('in')->into(Echoer::class)->named('renamed');
    }
}

#[Operative(factoryId: 'trailing_keyed')]
final class TrailingKeyed extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('in')->into(Echoer::class)->keyed();
    }
}

#[Operative(factoryId: 'one_sink_two_channels')]
final class OneSinkTwoChannels extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('in')->into(Echoer::class)->sink('shared')
            ->into(Counter::class)->sink('shared');
    }
}

#[Operative(factoryId: 'diamond')]
final class Diamond extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $edge = $c->input('in')->into(Echoer::class);
        $edge->named('left')->into(Counter::class);
        $edge->sink();
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
