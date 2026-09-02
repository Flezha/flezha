<?php

declare(strict_types=1);

namespace Flezha\Tests;

use Flezha\Attribute\Operative;
use Flezha\Attribute\Proc;
use Flezha\Attribute\Workflow;
use Flezha\BeyondWindow;
use Flezha\Clock;
use Flezha\Ctx;
use Flezha\Graph\Builder;
use Flezha\Graph\ClassSpec;
use Flezha\Graph\HostClassDecl;
use Flezha\LoopMode;
use Flezha\Procedure;
use Flezha\RollbackMode;
use Flezha\StreamOperative;
use Flezha\WorkflowCtx;
use Flezha\WorkflowOperative;

/**
 * Every `HostClassDecl` key the core learns, spelled by this builder.
 *
 * The fixture is the list of record, shared with the `flz_core` test that
 * asserts the core's own struct and with the Java, Node and Ruby builders'
 * tests. A key the core learns that PHP cannot spell fails here, which is the
 * point: the cross-SDK rule is that a declaration key lands in every SDK in
 * one change, and this is what makes forgetting one a red build rather than a
 * silently narrower SDK.
 */
final class DeclKeysTest extends RepositoryTestCase
{
    public function testTheBuilderSpellsEveryDeclaredKey(): void
    {
        $missing = array_diff($this->declared(), $this->spelled());

        self::assertSame([], array_values($missing), sprintf(
            'the PHP builder cannot spell %s; teach it, or the core learned a key this SDK will '
            . 'never send',
            implode(', ', $missing),
        ));
    }

    public function testTheBuilderInventsNoKeyTheCoreNeverLearned(): void
    {
        // `deny_unknown_fields` would refuse one at declare time, but at the
        // first controller connection rather than here.
        $extra = array_diff($this->spelled(), $this->declared());

        self::assertSame([], array_values($extra), sprintf(
            'the PHP builder emits %s, which the core never learned',
            implode(', ', $extra),
        ));
    }

    /** @return list<string> */
    private function spelled(): array
    {
        $stream = HostClassDecl::of(FullStream::spec());
        $workflow = HostClassDecl::of(FullWorkflow::spec());

        $keys = array_map(static fn (string $k): string => "class.$k", array_keys($stream));
        foreach (array_keys($workflow) as $key) {
            $keys[] = "class.$key";
        }
        foreach ($stream['channels'] as $channel) {
            foreach (array_keys($channel) as $key) {
                $keys[] = "channel.$key";
            }
        }
        foreach ($stream['procedures'] as $procedure) {
            foreach (array_keys($procedure) as $key) {
                $keys[] = "procedure.$key";
            }
        }
        foreach (array_keys($workflow['workflow']) as $key) {
            $keys[] = "workflow.$key";
        }

        $keys = array_values(array_unique($keys));
        sort($keys);

        return $keys;
    }

    /** @return list<string> */
    private function declared(): array
    {
        $lines = file($this->repositoryFile('test/fixtures/host_class_decl_keys.txt'), FILE_IGNORE_NEW_LINES) ?: [];
        $keys = array_values(array_filter(
            $lines,
            static fn (string $line): bool => $line !== '' && !str_starts_with($line, '#'),
        ));
        sort($keys);

        return $keys;
    }

    /**
     * The cross-language topology golden.
     *
     * `test/fixtures/reference_topologies.txt` is the Rust reference's channel
     * topology for two fixtures, transcribed by hand. Four SDKs implement one
     * design with no compiler between them, so the file is what keeps them
     * from drifting: this builder declares both shapes in PHP's spelling and
     * the lines must be the golden's, in order.
     */
    public function testTheHybridStreamTopologyIsTheReference(): void
    {
        self::assertSame(
            $this->golden('hybrid_channel'),
            $this->rendered('hybrid_channel', HybridChannelTopology::spec()),
        );
    }

    public function testTheStructuredWorkflowTopologyIsTheReference(): void
    {
        self::assertSame(
            $this->golden('commit_block_chain'),
            $this->rendered('commit_block_chain', CommitBlockChainTopology::spec()),
        );
    }

    /** @return list<string> */
    private function golden(string $topology): array
    {
        $lines = file($this->repositoryFile('test/fixtures/reference_topologies.txt'), FILE_IGNORE_NEW_LINES) ?: [];

        return array_values(array_filter(
            array_map(trim(...), $lines),
            static fn (string $line): bool => str_starts_with($line, "$topology |"),
        ));
    }

    /**
     * The declaration rendered in the golden's columns, one line per channel.
     *
     * @return list<string>
     */
    private function rendered(string $topology, ClassSpec $spec): array
    {
        $decl = HostClassDecl::of($spec);
        $lines = [];
        foreach ($decl['channels'] as $index => $channel) {
            $consumers = $channel['consumers'] ?? [];
            $lines[] = implode(' | ', [
                $topology,
                (string) $index,
                $channel['name'],
                $channel['kind'],
                isset($channel['producer']) ? (string) $channel['producer'] : '-',
                $consumers === [] ? '-' : implode(',', $consumers),
                $spec->channels[$index]->sink ?? '-',
            ]);
        }

        return $lines;
    }
}

/** A declaration populated far enough that every optional key is present. */
#[Proc(name: 'entry', clock: Clock::EventTime, mailboxCapacity: 8, schemaVersion: 3, minReadableSchemaVersion: 1)]
final class FullEntry extends Procedure
{
    public function state(?string $entityKey): mixed
    {
        return ['seen' => []];
    }

    public function applyChange(mixed &$state, mixed $change): void
    {
        $state['seen'][] = $change['push'];
    }

    public function eventTime(mixed $input): int
    {
        return (int) $input;
    }

    public function migrateState(int $fromVersion, string $bytes): mixed
    {
        return \Flezha\Codec\Json::decode($bytes);
    }

    public function run(mixed $input, Ctx $ctx): mixed
    {
        $ctx->change(['push' => $input]);

        return null;
    }
}

#[Proc(name: 'downstream', dynamicEntities: true, idleTtlMs: 1_000, maxEntities: 16)]
final class FullDownstream extends Procedure
{
    public function run(mixed $input, Ctx $ctx): mixed
    {
        return $input;
    }

    public function compensate(array $outputs, Ctx $ctx): void
    {
    }
}

#[Proc(name: 'fanned', entityKeys: ['a', 'b'])]
final class FullFanned extends Procedure
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

#[Operative(factoryId: 'decl_keys_stream')]
final class FullStream extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->effectivelyOnce();
        $c->checkpointEvery(4);
        $c->outputsAtCheckpoint(250);
        $c->correctionWindow(5_000);
        $c->beyondWindowPolicy(BeyondWindow::Drop);
        $c->compactAfterBytes(4_096);

        $edge = $c->input('input')->into(FullEntry::class);
        $edge->named('hop')->keyBy(static fn (mixed $v): string => (string) $v)
            ->retryMax(5)->retryBackoffMs(50)->into(FullDownstream::class);
        $edge->named('fan')->into(FullFanned::class);
        $c->input('second')->into(FullEntry::class)->named('sink')->keyed()->sink('out');
    }
}

#[Operative(factoryId: 'decl_keys_workflow')]
#[Workflow(
    loopMode: LoopMode::Chained,
    rollbackMode: RollbackMode::KeepProgress,
    retryAttempts: 4,
    retryBackoffMs: 100,
    barrierRetryAttempts: 2,
    maxDurationMs: 60_000,
    wakeAfterMs: 5_000,
    syncDurability: true,
)]
final class FullWorkflow extends WorkflowOperative
{
    public static function run(mixed $args, WorkflowCtx $ctx): mixed
    {
        return $args;
    }
}

#[Proc(name: 'hybrid_producer_procedure')]
final class HybridProducer extends Procedure
{
    public function run(mixed $input, Ctx $ctx): mixed
    {
        $ctx->emit($input);

        return null;
    }
}

#[Proc(name: 'hybrid_consumer_procedure')]
final class HybridConsumer extends Procedure
{
    public function run(mixed $input, Ctx $ctx): mixed
    {
        return null;
    }
}

/** The reference's stream fixture: one producing channel, a sink AND a consumer. */
#[Operative(factoryId: 'hybrid_channel')]
final class HybridChannelTopology extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('input')->into(HybridProducer::class)->named('hybrid_indicator')
            ->sink('hybrid_indicators', static fn (mixed $v): string => (string) $v)
            ->into(HybridConsumer::class);
    }
}

#[Proc(name: 'tier_commit_a')]
final class TierCommitA extends Procedure
{
    public function run(mixed $input, Ctx $ctx): mixed
    {
        $ctx->emit($input, 'job');

        return null;
    }
}

#[Proc(name: 'block_b')]
final class BlockB extends Procedure
{
    public function run(mixed $input, Ctx $ctx): mixed
    {
        return null;
    }
}

/** The reference's durable structured workflow: the same hybrid, mid-chain. */
#[Operative(factoryId: 'commit_block_chain')]
final class CommitBlockChainTopology extends WorkflowOperative
{
    public static function channels(Builder $c): void
    {
        $c->effectivelyOnce();
        $c->input('in')->into(TierCommitA::class)->named('mid')->keyed()
            ->sink('tier_sink', static fn (mixed $v): string => (string) $v)
            ->into(BlockB::class);
    }
}
