<?php

declare(strict_types=1);

namespace Flezha\Tests;

use Flezha\EntryIdle;
use Flezha\EntryIdleBridge;
use Flezha\Exception\FlezhaException;
use Flezha\Executor\LocalLoopState;
use Flezha\IdleObserver;
use Flezha\Logger;
use Flezha\Runner;
use PHPUnit\Framework\TestCase;

/**
 * The observer's host half — registration, the tick, contained delivery and
 * the accessor's mapping — against a scripted core. The latch itself is the
 * core's and is proved there; what this proves is the wiring over it.
 */
final class EntryIdleTest extends TestCase
{
    public const GAP_NANOS = 1_500_000_000;

    protected function tearDown(): void
    {
        Logger::use(new Logger());
    }

    /**
     * A core whose polls answer a script — the ABI's statuses in order, then
     * none. Its clock advances two seconds per read, so every tick is due.
     *
     * @param list<int> $answers
     */
    private static function scripted(array $answers, int $clockStepNanos = 2_000_000_000): EntryIdleBridge
    {
        return new class ($answers, $clockStepNanos) implements EntryIdleBridge {
            /** @var list<int> */
            public array $freed = [];
            private int $clock = 0;
            private int $next = 0;

            /** @param list<int> $answers */
            public function __construct(public array $answers, private readonly int $step)
            {
            }

            public function monotonicNanos(): int
            {
                return $this->clock += $this->step;
            }

            public function entryLastDeliveryNanos(string $channel): int
            {
                return 0;
            }

            public function idleWatchNew(string $channel, int $thresholdNanos): int
            {
                return $channel === '' ? 0 : ++$this->next;
            }

            public function idleWatchPoll(int $id): array
            {
                $status = array_shift($this->answers) ?? 0;

                return [$status, $status > 0 ? EntryIdleTest::GAP_NANOS : 0];
            }

            public function idleWatchFree(int $id): void
            {
                $this->freed[] = $id;
            }
        };
    }

    public function testOneIdleAndOneResumedReachTheObserverInTheOrderTheCoreAnswers(): void
    {
        $watcher = new EntryIdle(self::scripted([0, 1, 0, 2]));
        $seen = [];
        $watcher->register('trades_in', 1_000, new class ($seen) implements IdleObserver {
            /** @param list<array{string, float}> $seen */
            public function __construct(private array &$seen)
            {
            }

            public function idle(float $gapMs): void
            {
                $this->seen[] = ['idle', $gapMs];
            }

            public function resumed(float $gapMs): void
            {
                $this->seen[] = ['resumed', $gapMs];
            }
        });
        for ($i = 0; $i < 4; $i++) {
            $watcher->tick();
        }

        self::assertSame([['idle', 1500.0], ['resumed', 1500.0]], $seen);
        $watcher->stop();
    }

    public function testACallableIsAnObserverWhoseResumeDoesNothing(): void
    {
        $watcher = new EntryIdle(self::scripted([1, 2, 1]));
        $gaps = [];
        $watcher->register('trades_in', 1_000, static function (float $gapMs) use (&$gaps): void {
            $gaps[] = $gapMs;
        });
        for ($i = 0; $i < 3; $i++) {
            $watcher->tick();
        }

        self::assertSame([1500.0, 1500.0], $gaps);
        $watcher->stop();
    }

    public function testAThrowingObserverIsLoggedAndTheNextEpisodeStillReachesIt(): void
    {
        $lines = [];
        Logger::use(new class ($lines) extends Logger {
            /** @param list<string> $lines */
            public function __construct(private array &$lines)
            {
                parent::__construct('warn');
            }

            protected function write(string $level, string $message): void
            {
                $this->lines[] = "$level: $message";
            }
        });
        $watcher = new EntryIdle(self::scripted([1, 2, 1]));
        $calls = 0;
        $watcher->register('book_in', 1_000, static function () use (&$calls): void {
            $calls++;

            throw new \RuntimeException('observer under test');
        });
        for ($i = 0; $i < 3; $i++) {
            $watcher->tick();
        }

        self::assertSame(2, $calls, 'the episode after the throw still reaches the observer');
        self::assertCount(2, $lines);
        self::assertSame(
            "warn: entry-idle observer threw on the idle episode of channel 'book_in': observer under test",
            $lines[0],
        );
        $watcher->stop();
    }

    public function testStopFreesEveryWatchTheCoreHandedOutAndIsIdempotent(): void
    {
        $core = self::scripted([]);
        $watcher = new EntryIdle($core);
        $watcher->register('a', 1_000, static fn (): null => null);
        $watcher->register('b', 2_000, static fn (): null => null);
        $watcher->stop();

        self::assertSame([1, 2], $core->freed);
        $watcher->stop();
        self::assertSame([1, 2], $core->freed);
    }

    public function testATickInsideThePeriodPollsNothing(): void
    {
        // A clock that does not advance: the first tick is due at once, the
        // second falls inside the period the threshold asked for.
        $core = self::scripted([1, 1], 0);
        $watcher = new EntryIdle($core);
        $episodes = 0;
        $watcher->register('a', 1_000, static function () use (&$episodes): void {
            $episodes++;
        });
        $watcher->tick();
        $watcher->tick();

        self::assertSame(1, $episodes, 'one poll per period, however often the loop comes round');
        self::assertSame([1], $core->answers, 'the second answer was never consumed');
        $watcher->stop();
    }

    public function testARefusedNameAndANonPositiveThresholdAreRefusedAtRegistration(): void
    {
        $watcher = new EntryIdle(self::scripted([]));

        try {
            $watcher->register('', 1_000, static fn (): null => null);
            self::fail('an id of 0 must be refused');
        } catch (FlezhaException $refusal) {
            self::assertStringContainsString('refused the channel name', $refusal->getMessage());
        }

        $this->expectException(\InvalidArgumentException::class);
        $watcher->register('x', 0, static fn (): null => null);
    }

    public function testTheTickIsAQuarterOfTheThresholdWithinItsBounds(): void
    {
        self::assertSame(1_000, EntryIdle::tickPeriodMs(60_000));
        self::assertSame(500, EntryIdle::tickPeriodMs(2_000));
        self::assertSame(250, EntryIdle::tickPeriodMs(10));
    }

    public function testTheAccessorIsAbsentBeforeTheFirstFrameAndAnInstantFromTheGapAfter(): void
    {
        $core = new class () implements EntryIdleBridge {
            public function monotonicNanos(): int
            {
                return 5_000_000_000;
            }

            public function entryLastDeliveryNanos(string $channel): int
            {
                return $channel === 'fed' ? 3_000_000_000 : 0;
            }

            public function idleWatchNew(string $channel, int $thresholdNanos): int
            {
                return 1;
            }

            public function idleWatchPoll(int $id): array
            {
                return [0, 0];
            }

            public function idleWatchFree(int $id): void
            {
            }
        };

        self::assertNull(EntryIdle::lastEntryDelivery($core, 'never'));
        $before = microtime(true);
        $at = EntryIdle::lastEntryDelivery($core, 'fed');
        $after = microtime(true);

        self::assertInstanceOf(\DateTimeImmutable::class, $at);
        $seconds = (float) $at->format('U.u');
        self::assertGreaterThanOrEqual($before - 2.0 - 0.001, $seconds);
        self::assertLessThanOrEqual($after - 2.0 + 0.001, $seconds);
    }

    /**
     * A worker interpreter's handle registers nothing: the entry script re-runs
     * there, and only the thread that started the runner owns its observers.
     */
    public function testAnAttachedHandleRegistersNothing(): void
    {
        $attachment = new \ReflectionProperty(Runner::class, 'attachment');
        $idle = new \ReflectionProperty(Runner::class, 'idle');
        $runner = new Runner('entry-idle-attached');
        $attachment->setValue(null, [1, new LocalLoopState()]);

        try {
            self::assertSame($runner, $runner->onEntryIdle('input', 1_000, static fn (): null => null));
            self::assertNull($idle->getValue($runner), 'an attached handle keeps no watcher');
        } finally {
            $attachment->setValue(null, null);
        }
    }

    public function testTheAccessorIsLegalBeforeTheRunnerStarts(): void
    {
        self::assertNull((new Runner('entry-idle-cold'))->lastEntryDelivery('never_declared'));
    }
}
