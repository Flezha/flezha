<?php

declare(strict_types=1);

namespace Flezha\Tests;

use Flezha\Attribute\Proc;
use Flezha\Ctx;
use Flezha\Procedure;
use Flezha\Testing\Harness;
use Flezha\Testing\ReplayMismatch;
use PHPUnit\Framework\TestCase;

#[Proc]
final class MutatesWithoutRecording extends Procedure
{
    public function state(?string $entityKey): mixed
    {
        return ['count' => 0];
    }

    public function applyChange(mixed &$state, mixed $change): void
    {
        $state['count'] += (int) $change;
    }

    public function run(mixed $input, Ctx $ctx): mixed
    {
        // The failure this SDK cannot make impossible: PHP has no cheap deep
        // read-only view, so the state is writable and the rule is a rule.
        $ctx->state['count'] += (int) $input;

        return null;
    }
}

#[Proc]
final class ImpureFold extends Procedure
{
    public function state(?string $entityKey): mixed
    {
        return ['calls' => 0];
    }

    public function applyChange(mixed &$state, mixed $change): void
    {
        $state['calls'] = random_int(1, 1_000_000);
    }

    public function run(mixed $input, Ctx $ctx): mixed
    {
        $ctx->change($input);

        return null;
    }
}

/**
 * The harness, and the guard that makes the mutate-only-through-changes rule
 * safe in a language with no cheap state guard.
 */
final class HarnessTest extends TestCase
{
    public function testAnInvocationReportsWhatItProduced(): void
    {
        $harness = new Harness(Counter::class);
        $outcome = $harness->feed('a');

        self::assertSame([['push' => 'a']], $outcome->changes);
        self::assertSame(['count' => 1, 'seen' => ['a']], $harness->state());
        self::assertSame([], $outcome->outputs);
    }

    public function testAReturnedValueIsEmittedExactlyAsTheDispatcherDoesIt(): void
    {
        $outcome = (new Harness(Echoer::class))->feed('hello');

        self::assertSame(['hello'], $outcome->outputs);
        self::assertSame('hello', $outcome->returned);
    }

    /** The harness reads its sink back as JSON, so a string is not reported as its bytes. */
    public function testAStringAndAnArrayAreReportedExactlyAsEmitted(): void
    {
        $harness = new Harness(Echoer::class);

        self::assertSame(['echo: hi'], $harness->feed('echo: hi')->outputs);
        self::assertSame([['said' => 'hi']], $harness->feed(['said' => 'hi'])->outputs);
    }

    public function testATimerFiresWithNowEqualToItsSchedule(): void
    {
        $harness = new Harness(Counter::class);
        $harness->fireTimer('processing_time', 4_200);

        self::assertSame(['count' => 1, 'seen' => ['timer:4200']], $harness->state());
    }

    public function testARestoreRebuildsTheStateAChangeLogDescribes(): void
    {
        $harness = new Harness(Counter::class);
        $harness->restore(changes: ['{"push":"x"}', '{"push":"y"}']);

        self::assertSame(['count' => 2, 'seen' => ['x', 'y']], $harness->state());
    }

    public function testTheReplayOfAWellBehavedProcedureAgrees(): void
    {
        self::assertTrue(Harness::replayEquals(Counter::class, ['a', 'b', 'c']));
    }

    /**
     * Both failures this catches are invisible until a restart: a handler that
     * mutated the state without recording a change, and a fold that is not
     * pure. The SDKs whose state is guarded can only catch the second.
     */
    public function testAnUnrecordedMutationIsCaught(): void
    {
        $this->expectException(ReplayMismatch::class);
        $this->expectExceptionMessage('mutated the state without recording a change');

        Harness::replayEquals(MutatesWithoutRecording::class, [1, 2]);
    }

    public function testAnImpureFoldIsCaught(): void
    {
        $this->expectException(ReplayMismatch::class);

        Harness::replayEquals(ImpureFold::class, [1, 2, 3]);
    }

    /**
     * Both timer kinds, and cancellation by the timestamp a registration
     * answered — the whole of the timer surface a procedure can reach.
     */
    public function testTimersAreRegisteredAndCancelledByTheirScheduledTime(): void
    {
        $outcome = (new Harness(Timing::class))->feed('42', now: 42);

        self::assertSame([
            ['kind' => 'processing_time', 'scheduled_at' => 1_042, 'register' => true],
            ['kind' => 'event_time', 'scheduled_at' => 5_042, 'register' => true],
            ['kind' => 'processing_time', 'scheduled_at' => 1_042, 'register' => false],
        ], $outcome->timers);
    }

    /** `now()` inside `onTimer` IS the scheduled time, so a chain is stable. */
    public function testATimerFiringServesItsScheduledTimeAsNow(): void
    {
        $harness = new Harness(Timing::class);
        $harness->fireTimer('event_time', 9_000);

        self::assertSame(
            [['kind' => 'event_time', 'at' => 9_000, 'now' => 9_000]],
            $harness->state()['fired'],
        );
    }

    public function testStableIdIsDistinctPerInvocationAndStableAcrossReplay(): void
    {
        $harness = new Harness(StableIdProbe::class, 'k');
        $first = $harness->feed('x')->outputs;
        $again = (new Harness(StableIdProbe::class, 'k'))->feed('x')->outputs;

        self::assertNotSame($first[0][0], $first[0][1], 'two calls in one invocation differ');
        self::assertSame($first, $again, 'the same invocation replayed derives the same ids');
    }
}

#[Proc]
final class StableIdProbe extends Procedure
{
    public function run(mixed $input, Ctx $ctx): mixed
    {
        return [$ctx->stableId(), $ctx->stableId()];
    }
}
