<?php

declare(strict_types=1);

namespace Flezha\Tests;

use Flezha\Dispatch\Dispatcher;
use Flezha\Dispatch\Registry;
use Flezha\Ffi\Core;
use Flezha\Graph\Validate;
use Flezha\Logger;
use PHPUnit\Framework\TestCase;

/**
 * Every activation kind, against a real core context.
 *
 * A real one rather than a double: the context is what `Ctx::spawnWorkflow`
 * and the completion path talk to, and a double would prove that the
 * dispatcher agrees with the double. Nothing here starts a runner, so no
 * controller connection is attempted.
 */
final class DispatchTest extends TestCase
{
    private static ?Core $core = null;
    private Dispatcher $dispatcher;
    private Registry $registry;

    public static function setUpBeforeClass(): void
    {
        self::$core = Core::create();
    }

    public static function tearDownAfterClass(): void
    {
        self::$core?->free();
        self::$core = null;
    }

    protected function setUp(): void
    {
        $this->registry = new Registry();
        foreach (
            [EchoOperative::class, CounterOperative::class, KeyedOperative::class, StampOperative::class,
                MixedStampOperative::class] as $class
        ) {
            $spec = $class::spec();
            Validate::spec($spec);
            $this->registry->add($spec);
        }
        $this->dispatcher = new Dispatcher($this->registry, self::core());
    }

    public function testAnInputReachesTheProcedureAndItsReturnIsEmitted(): void
    {
        $result = $this->work('echo_operative', 0, [self::input('"hello"')]);

        self::assertSame('work', $result['outcome']);
        self::assertSame('ok', $result['effects'][0]['verdict']);
        self::assertSame('"hello"', $result['effects'][0]['outputs'][0]['payload']);
        self::assertSame('plugin', $result['effects'][0]['outputs'][0]['target']);
        // The CHANNEL's name, which the fixture sets with `named('out')`, not
        // the sink's — it is what rides the frame as `channel` metadata.
        self::assertSame('out', $result['effects'][0]['outputs'][0]['plugin_channel']);
    }

    public function testARecordedChangeIsJournaledAndFoldedLive(): void
    {
        $result = $this->work('counter_operative', 0, [self::input('"a"'), self::input('"b"')]);

        self::assertCount(2, $result['effects']);
        self::assertSame(['{"push":"a"}'], $result['effects'][0]['changes']);
        self::assertSame(
            ['count' => 2, 'seen' => ['a', 'b']],
            $this->stateOf('counter_operative'),
        );
    }

    public function testAThrowingHandlerClimbsTheRetryLadder(): void
    {
        $result = $this->work('counter_operative', 0, [self::input('not json at all')]);

        self::assertSame('retryable', $result['effects'][0]['verdict']);
        self::assertStringContainsString('JsonException', $result['effects'][0]['message']);
    }

    public function testANonRetryableErrorParksImmediately(): void
    {
        $this->registry->add(PoisonOperative::spec());
        $result = $this->work('poison_operative', 0, [self::input('"poison"')]);

        self::assertSame('non_retryable', $result['effects'][0]['verdict']);
        self::assertStringContainsString('poisoned', $result['effects'][0]['message']);
    }

    /** Items after a failure are re-handed by the core, so the batch stops. */
    public function testTheBatchStopsAtTheFailedItem(): void
    {
        $this->registry->add(PoisonOperative::spec());
        $result = $this->work('poison_operative', 0, [
            self::input('"ok"'),
            self::input('"poison"'),
            self::input('"never reached"'),
        ]);

        self::assertCount(2, $result['effects']);
        self::assertSame('ok', $result['effects'][0]['verdict']);
        self::assertSame('non_retryable', $result['effects'][1]['verdict']);
    }

    public function testATimerFiringReachesOnTimerWithNowEqualToItsSchedule(): void
    {
        $result = $this->work('counter_operative', 0, [
            ['kind' => 'timer', 'timer_kind' => 'processing_time', 'scheduled_at' => 5_000],
        ]);

        self::assertSame(5_000, $result['effects'][0]['event_time']);
        self::assertSame(['{"push":"timer:5000"}'], $result['effects'][0]['changes']);
    }

    public function testARestoreFoldRebuildsStateBeforeInputsFlow(): void
    {
        $restored = $this->dispatcher->answer([
            'token' => 1,
            'factory' => 'counter_operative',
            'procedure' => 0,
            'operative' => 'op',
            'entity_key' => null,
            'change_seq' => 0,
            'kind' => 'restore_fold',
            'snapshot' => null,
            'changes' => [
                ['bytes' => '{"push":"x"}', 'from_version' => null],
                ['bytes' => '{"push":"y"}', 'from_version' => null],
            ],
        ]);

        self::assertSame('restored', $restored['outcome']);
        self::assertSame(['count' => 2, 'seen' => ['x', 'y']], $this->stateOf('counter_operative'));
    }

    public function testASnapshotRoundTripsThroughALaterRestore(): void
    {
        $this->work('counter_operative', 0, [self::input('"a"')]);
        $snapshot = $this->command('counter_operative', 'snapshot_request')['state'];

        $fresh = new Dispatcher($this->registry, self::core());
        $fresh->answer([
            'token' => 2,
            'factory' => 'counter_operative',
            'procedure' => 0,
            'operative' => 'op',
            'entity_key' => null,
            'change_seq' => 0,
            'kind' => 'restore_fold',
            'snapshot' => ['bytes' => $snapshot, 'from_version' => null],
            'changes' => [],
        ]);

        self::assertSame(['count' => 1, 'seen' => ['a']], $this->stateOf('counter_operative'));
    }

    public function testALiveStateReadAndAMigrationSerializeAnswerTheSameJson(): void
    {
        $this->work('counter_operative', 0, [self::input('"a"')]);

        $live = $this->command('counter_operative', 'live_state_read')['json'];
        $migration = $this->command('counter_operative', 'migration_serialize')['json'];

        self::assertSame('{"count":1,"seen":["a"]}', $live);
        self::assertSame($live, $migration);
    }

    public function testAnAppliedStateMaterializesTheMigratedValue(): void
    {
        $applied = $this->dispatcher->answer([
            'token' => 3,
            'factory' => 'counter_operative',
            'procedure' => 0,
            'operative' => 'op',
            'entity_key' => null,
            'change_seq' => 0,
            'kind' => 'apply_state',
            'state_json' => '{"count":9,"seen":["z"]}',
        ]);

        self::assertSame('state_applied', $applied['outcome']);
        self::assertSame(['count' => 9, 'seen' => ['z']], $this->stateOf('counter_operative'));
    }

    public function testThePrepareCrossingAnswersOneKeyPerPayload(): void
    {
        $prepared = $this->prepare(['"a:1"', '"b:2"']);

        self::assertSame([['keys' => ['a'], 'event_time' => null], ['keys' => ['b'], 'event_time' => null]], $prepared);
    }

    /** A filtered payload answers no key: the core never mints an entity. */
    public function testAFilteredPayloadAnswersNoKey(): void
    {
        self::assertSame([['keys' => []]], $this->prepare(['"drop"']));
    }

    /** One payload's refusal is isolated to that payload. */
    public function testAFailingPayloadDoesNotAffectTheOthers(): void
    {
        $prepared = $this->prepare(['"a:1"', 'not json', '"b:2"']);

        self::assertSame(['a'], $prepared[0]['keys']);
        self::assertArrayHasKey('error', $prepared[1]);
        self::assertSame(['b'], $prepared[2]['keys']);
    }

    /**
     * The SDK's half of a workflow chain: a stage runs like any other
     * procedure, and the driver's barrier is answered by the completion. The
     * driver itself — loop modes, suspension, rewind, the proc journal that
     * makes a resumed chain skip committed stages — is the core's, reached
     * through step activations, so this SDK implements none of it.
     */
    public function testAWorkflowStageRunsAsAnOrdinaryProcedure(): void
    {
        $this->registry->add(SagaOperative::spec());
        $result = $this->work('saga_operative', 0, [self::input('"reserve"')]);

        self::assertSame('ok', $result['effects'][0]['verdict']);
        $emitted = json_decode($result['effects'][0]['outputs'][0]['payload'], true);
        self::assertSame('reserve', $emitted['step']);
        self::assertMatchesRegularExpression('/^[0-9a-f]{32}$/', $emitted['receipt']);
    }

    /** An aborted chain compensates the stages that committed, in reverse. */
    public function testCompensationRunsOverTheRecordedForwardOutputs(): void
    {
        $this->registry->add(SagaOperative::spec());
        $result = $this->dispatcher->answer([
            'token' => 4,
            'factory' => 'saga_operative',
            'procedure' => 0,
            'operative' => 'op',
            'entity_key' => null,
            'change_seq' => 0,
            'kind' => 'compensate_run',
            'outputs' => ['{"step":"reserve","receipt":"abc"}'],
        ]);

        self::assertSame('work', $result['outcome']);
        self::assertSame(
            '{"undo":"abc"}',
            $result['effects'][0]['outputs'][0]['payload'],
        );
    }

    /** A class with no compensate hook says so rather than silently doing nothing. */
    public function testCompensationWithoutAHookIsRefused(): void
    {
        $result = $this->dispatcher->answer([
            'token' => 5,
            'factory' => 'echo_operative',
            'procedure' => 0,
            'operative' => 'op',
            'entity_key' => null,
            'change_seq' => 0,
            'kind' => 'compensate_run',
            'outputs' => [],
        ]);

        self::assertSame('failed', $result['outcome']);
        self::assertTrue($result['non_retryable']);
        self::assertStringContainsString('declares no compensate hook', $result['error']);
    }

    /** Run-sugar: the seed args cross once and the body answers the driver. */
    public function testARunSugarBodyCompletesWithItsResult(): void
    {
        $this->registry->add(RunSugarOperative::spec());
        $result = $this->dispatcher->answer([
            'token' => 6,
            'factory' => 'run_sugar_operative',
            'procedure' => 0,
            'operative' => 'instance-1',
            'entity_key' => null,
            'change_seq' => 0,
            'kind' => 'workflow_run',
            'args' => '{"order":7}',
        ]);

        self::assertSame('workflow_done', $result['outcome']);
        self::assertTrue($result['workflow_completed']);
        self::assertSame('{"seeded":{"order":7}}', $result['workflow_result']);
    }

    /** A class handed a body it does not declare fails non-retryably. */
    public function testAWorkflowBodyOnAClassThatDeclaresNoneIsRefused(): void
    {
        $result = $this->dispatcher->answer([
            'token' => 7,
            'factory' => 'echo_operative',
            'procedure' => 0,
            'operative' => 'instance-1',
            'entity_key' => null,
            'change_seq' => 0,
            'kind' => 'workflow_run',
            'args' => '',
        ]);

        self::assertSame('failed', $result['outcome']);
        self::assertStringContainsString('declares none', $result['error']);
    }

    public function testAnUnregisteredFactoryFailsNonRetryably(): void
    {
        $result = $this->work('never_declared', 0, [self::input('"x"')]);

        self::assertSame('failed', $result['outcome']);
        self::assertTrue($result['non_retryable']);
        self::assertStringContainsString('no class registered for factory', $result['error']);
    }

    /**
     * An activation that returns unanswered retires its entity for the life of
     * the process, so every path out of `dispatch` ends in a completion or an
     * abandon — and when both refuse, it says so rather than throwing into the
     * loop.
     */
    public function testAnUnanswerableActivationIsAbandonedAndReported(): void
    {
        $said = [];
        Logger::use(new class ($said) extends Logger {
            /** @param array<int, string> $said */
            public function __construct(public array &$said)
            {
                parent::__construct('debug');
            }

            protected function write(string $level, string $message): void
            {
                $this->said[] = "$level: $message";
            }
        });

        try {
            // The pinned work-activation fixture: a token the core never
            // handed out, so both `complete` and `abandon` refuse it.
            $this->dispatcher->dispatch((string) hex2bin(
                '2a046f702d310301016b0002000102aabb010a666c7a2e6f66667365740137010701e8070100d00f090166',
            ));
        } finally {
            Logger::use(new Logger('warn'));
        }

        self::assertStringContainsString('unknown activation token 42', implode("\n", $said));
        self::assertStringContainsString('abandoning activation 42 failed', implode("\n", $said));
    }

    // --- output identity stamps ---------------------------------------------
    //
    // One invocation, several emits: the per-emit ordinal is the half a
    // single-emit assertion cannot see, and the e2e golden checks stamp NAMES
    // only.

    public function testAKeyedPluginEmitCarriesTheStampsAndTheOrdinalAdvancesPerEmit(): void
    {
        $outputs = $this->stamped('"twice"')['effects'][0]['outputs'];

        self::assertCount(2, $outputs);
        self::assertSame(
            ['flz.entity' => 'op/stamp_emitter/ek', 'flz.seq' => '5.0', 'flz.key' => 'ek'],
            $outputs[0]['metadata'],
        );
        self::assertSame(
            ['flz.entity' => 'op/stamp_emitter/ek', 'flz.seq' => '5.1', 'flz.key' => 'ek'],
            $outputs[1]['metadata'],
        );
    }

    public function testANonKeyedPluginEmitCarriesNoStampsAndStillAdvancesTheOrdinal(): void
    {
        [$plain, $keyed] = $this->mixed('"mixed"')['effects'][0]['outputs'];

        self::assertSame('plain_out', $plain['plugin_channel']);
        self::assertSame([], $plain['metadata']);
        self::assertSame('keyed_out', $keyed['plugin_channel']);
        self::assertSame(
            [
                'user' => 'meta',
                'flz.entity' => 'op/stamp_emitter/ek',
                'flz.seq' => '5.1',
                'flz.key' => 'ek',
            ],
            $keyed['metadata'],
        );
    }

    public function testAFannedEmitAdvancesTheOrdinalOncePerProducedOutput(): void
    {
        $outputs = $this->mixed('"fanned"')['effects'][0]['outputs'];

        self::assertCount(2, $outputs);
        self::assertSame('5.0', $outputs[0]['metadata']['flz.seq']);
        self::assertSame('5.1', $outputs[1]['metadata']['flz.seq']);
    }

    public function testAFreshWorkItemRestartsTheEmitOrdinal(): void
    {
        $result = $this->work(
            'stamp_operative',
            0,
            [self::input('"twice"'), self::input('"twice"')],
            'ek',
            5,
        );

        $seqs = array_map(
            static fn (array $effect): array => array_map(
                static fn (array $output): string => $output['metadata']['flz.seq'],
                $effect['outputs'],
            ),
            $result['effects'],
        );
        self::assertSame([['5.0', '5.1'], ['5.0', '5.1']], $seqs);
    }

    public function testASpoofedStampIsRefusedBeforeAnyOutput(): void
    {
        $effect = $this->stamped('"spoof"')['effects'][0];

        self::assertSame('retryable', $effect['verdict']);
        self::assertArrayNotHasKey('outputs', $effect);
    }

    /** @return array<string, mixed> */
    private function stamped(string $payload): array
    {
        return $this->work('stamp_operative', 0, [self::input($payload)], 'ek', 5);
    }

    /** @return array<string, mixed> */
    private function mixed(string $payload): array
    {
        return $this->work('mixed_stamp_operative', 0, [self::input($payload)], 'ek', 5);
    }

    /** @param list<array<string, mixed>> $items @return array<string, mixed> */
    private function work(
        string $factory,
        int $procedure,
        array $items,
        ?string $entityKey = null,
        int $changeSeq = 0,
    ): array {
        return $this->dispatcher->answer([
            'token' => 1,
            'factory' => $factory,
            'procedure' => $procedure,
            'operative' => 'op',
            'entity_key' => $entityKey,
            'change_seq' => $changeSeq,
            'kind' => 'work',
            'items' => $items,
        ]);
    }

    /** @return array<string, mixed> */
    private function command(string $factory, string $kind): array
    {
        return $this->dispatcher->answer([
            'token' => 1,
            'factory' => $factory,
            'procedure' => 0,
            'operative' => 'op',
            'entity_key' => null,
            'change_seq' => 0,
            'kind' => $kind,
        ]);
    }

    /** @param list<string> $payloads @return list<array<string, mixed>> */
    private function prepare(array $payloads): array
    {
        return $this->dispatcher->answer([
            'token' => 1,
            'factory' => 'keyed_operative',
            'procedure' => 0,
            'operative' => 'op',
            'entity_key' => null,
            'change_seq' => 0,
            'kind' => 'key_request',
            'key_request' => [
                'channel' => 0,
                'payloads' => $payloads,
                'want_keys' => true,
                'want_event_time' => false,
            ],
        ])['prepared'];
    }

    private function stateOf(string $factory): mixed
    {
        $registered = $this->registry->get($factory);
        self::assertNotNull($registered);

        return $registered->stateOf($registered->spec->procedures[0], null);
    }

    /** @return array<string, mixed> */
    private static function input(string $payload, ?int $offset = null): array
    {
        return [
            'kind' => 'input',
            'channel' => 0,
            'payload' => $payload,
            'metadata' => [],
            'consumed_offset' => $offset,
            'replay' => false,
            'now' => 1_000,
        ];
    }

    private static function core(): Core
    {
        return self::$core ?? self::fail('the shared core context was not created');
    }
}
