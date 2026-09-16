<?php

declare(strict_types=1);

namespace Flezha\Tests;

use Flezha\Exception\FlezhaException;
use Flezha\LocalOperative;
use Flezha\SinkOutput;
use Flezha\TestApp\BoundedMailboxDropNothingOperative;
use Flezha\TestApp\DynamicEntityOperative;
use Flezha\TestApp\PartitionRelayOperative;
use Flezha\TestApp\SessionTimeoutOperative;
use Flezha\TestApp\SugarDoNothingOperative;
use Flezha\TestApp\TierStoreOperative;
use PHPUnit\Framework\TestCase;

/**
 * Local mode over the five capabilities the LANGUAGE LAYER owns, each driven
 * through the fixture class `test/apps/php_runner/conformance.json` names for
 * it: the same class the end-to-end scenario drives, so what passes here and
 * what passes there cannot be two different graphs.
 *
 * The other ten capabilities in the vocabulary are the controller's:
 * persistence, effectively-once, correction, partitioning, workflows,
 * liveness, restarts. Local mode refuses them by name rather than
 * approximating them.
 *
 * What each test can assert is what a run with no controller can see. A
 * durable write reaches a null sink, a parked payload has nowhere to park, and
 * a partition member's key range arrives with `AddOperative`, so the
 * assertions here are the output side of each capability, which is the half
 * the language layer produces.
 */
final class LocalModeCapabilitiesTest extends TestCase
{
    /** Long enough for a graph to settle, short enough that a wedge is a failure. */
    private const BUDGET_MS = 20_000;

    /** The capacity `BoundedMailbox` declares, which is what a feed fits into. */
    private const MAILBOX = 4;

    public static function setUpBeforeClass(): void
    {
        TestApp::load('fixtures.php');
        // `tier_store` lives beside the other persisted-state fixtures, and
        // reads the wire helpers `fixtures.php` declares.
        TestApp::load('fixtures_state.php');
    }

    public function testGraphBuilderBuildsAndRunsADeclaredGraphWithNoController(): void
    {
        $this->declares('graph_builder', 'sugar_do_nothing');

        $released = $this->release(
            SugarDoNothingOperative::class,
            'input',
            'sugar_do_nothing_echo',
            ['alpha', 'beta'],
        );

        self::assertSame(
            ['alpha', 'beta'],
            array_map(static fn (SinkOutput $out): string => $out->payload, $released),
            'the builder produced a graph the core ran, in order',
        );
    }

    public function testKeyedChannelsStampTheKeyTheClassResolved(): void
    {
        // `partition_relay` is the manifest's only keyed-channel class.
        // Partitioning itself is the controller's (a member's key-group range
        // arrives with `AddOperative`), so what is asserted here is the half
        // the language layer owns: the key it resolves and the stamps it puts
        // on the released value.
        $this->declares('keyed_channels', 'partition_relay');

        $released = $this->release(PartitionRelayOperative::class, 'in', 'input', ['a:1', 'b:1', 'a:2']);

        self::assertSame(
            ['a' => ['a:1', 'a:2'], 'b' => ['b:1']],
            $this->byKey($released),
            'every value reached the Entity its own key resolved to, in the order it was fed',
        );
        foreach ($released as $output) {
            self::assertSame(
                explode(':', $output->payload)[0],
                $output->metadata['flz.key'],
                'each release carries the key its own value resolved to',
            );
            foreach (['flz.entity', 'flz.seq'] as $stamp) {
                self::assertArrayHasKey($stamp, $output->metadata, "a keyed sink carries $stamp");
            }
        }
    }

    public function testADynamicEntityFilterRunsBeforeKeyingAndNothingElseIsLost(): void
    {
        // The class's sink is not keyed, so a released value carries no entity
        // stamp and the per-key minting is not visible on this channel. It is
        // asserted above, on `partition_relay`, whose procedure is dynamic too
        // and whose sink IS keyed. What this class shows that that one does
        // not is the declared filter: a value it rejects answers no key, so
        // the core is never asked to mint an Entity for it.
        $this->declares('dynamic_entity', 'dynamic_entity');

        $released = $this->release(
            DynamicEntityOperative::class,
            'input',
            'kafka',
            ['a:1', 'b:1', 'drop', 'a:2'],
        );

        self::assertSame(
            ['a' => ['a:1', 'a:2'], 'b' => ['b:1']],
            $this->byKey($released),
            'the filter dropped `drop` before it was keyed, and every other value reached its Entity',
        );
        foreach ($released as $output) {
            self::assertSame(
                ['channel' => 'kafka'],
                $output->metadata,
                'an unkeyed sink carries its channel and no identity stamps',
            );
        }
    }

    public function testARetryLadderMailboxThatDropsNothingLosesNoInput(): void
    {
        // `poison_probe` is the manifest's other class for this capability and
        // its poison path ends in the dead-letter queue, which is the
        // controller's. The ladder's other arm, a full mailbox under
        // `drop_nothing` where nothing is discarded, is entirely the language
        // layer's, and is what a run with no controller can see.
        $this->declares('retry_ladder', 'bounded_mailbox_drop_nothing');

        $inputs = array_map(static fn (int $at): string => "n$at", range(0, 23));
        $released = $this->release(
            BoundedMailboxDropNothingOperative::class,
            'drop_nothing_input',
            'drop_nothing_echo',
            $inputs,
            perDrive: self::MAILBOX,
        );

        self::assertSame(
            $inputs,
            array_map(static fn (SinkOutput $out): string => $out->payload, $released),
            'six mailboxfuls were fed and under `drop_nothing` all 24 came back, in order',
        );
    }

    public function testADeclaredTimerFiresLocallyWithItsScheduledTime(): void
    {
        $this->declares('entity_timers', 'session_timeout');

        // The class reads its own deadline from the environment, the way the
        // e2e sets it; 200 ms keeps the run short without changing what is
        // proved.
        $previous = getenv('TIMER_TEST_TIMEOUT_MS');
        putenv('TIMER_TEST_TIMEOUT_MS=200');

        try {
            $released = $this->release(
                SessionTimeoutOperative::class,
                'input',
                'kafka',
                ['open'],
            );
        } finally {
            putenv($previous === false ? 'TIMER_TEST_TIMEOUT_MS' : "TIMER_TEST_TIMEOUT_MS=$previous");
        }

        $payloads = array_map(static fn (SinkOutput $out): string => $out->payload, $released);
        $closed = array_values(array_filter($payloads, static fn (string $p): bool => str_starts_with($p, 'closed@')));
        self::assertCount(1, $closed, 'the timer fired exactly once: ' . json_encode($payloads));
        self::assertMatchesRegularExpression(
            '/^closed@\d+$/',
            $closed[0],
            'the firing carries the timestamp it was scheduled for',
        );
    }

    public function testAControllerCapabilityIsRefusedByNameNotApproximated(): void
    {
        // The line local mode draws: the ten the controller owns stay refused.
        // `tier_store` is effectively-once, which needs a durable store to
        // release from, so a local run says so instead of running the graph.
        $this->expectException(FlezhaException::class);
        $this->expectExceptionMessageMatches('/effectively_once.*requires a controller/s');
        LocalOperative::start(TierStoreOperative::class);
    }

    /**
     * Run one class over `$inputs` on `$channel`, and return what `$sink`
     * released.
     *
     * `$perDrive` caps how many values are fed before the loop is driven.
     * This host has one thread, so a feed that parks on a full mailbox parks
     * the only thread that could drain it; a class whose mailbox is smaller
     * than the feed names its capacity here.
     *
     * @param list<string> $inputs
     *
     * @return list<SinkOutput>
     */
    private function release(
        string $class,
        string $channel,
        string $sink,
        array $inputs,
        int $perDrive = 0,
    ): array {
        $released = [];
        $run = LocalOperative::start($class);

        try {
            $run->onSink($sink, static function (SinkOutput $out) use (&$released): void {
                $released[] = $out;
            });
            foreach ($perDrive > 0 ? array_chunk($inputs, $perDrive) : [$inputs] as $batch) {
                foreach ($batch as $input) {
                    $run->feed($channel, $input);
                }
                $run->runUntilIdle(self::BUDGET_MS);
            }
        } finally {
            $run->close();
        }

        return $released;
    }

    /**
     * The released payloads under the key each of them resolved to.
     *
     * One Entity drains at a time here, so releases arrive grouped by key
     * rather than interleaved as they were fed. What a keyed declaration
     * promises is the order within a key, which is what this compares.
     *
     * @param list<SinkOutput> $released
     *
     * @return array<string, list<string>>
     */
    private function byKey(array $released): array
    {
        $grouped = [];
        foreach ($released as $output) {
            $grouped[explode(':', $output->payload)[0]][] = $output->payload;
        }
        ksort($grouped);

        return $grouped;
    }

    /** The class under test is the one the manifest names for the capability. */
    private function declares(string $capability, string $factoryId): void
    {
        $declared = TestApp::declaredFixtures()[$capability] ?? [];
        self::assertContains($factoryId, $declared, sprintf(
            'conformance.json lists %s for %s, not %s; the local-mode run must drive a class the e2e drives',
            json_encode($declared),
            $capability,
            $factoryId,
        ));
    }
}
