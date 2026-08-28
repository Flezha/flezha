<?php

declare(strict_types=1);

namespace Flezha\Tests;

use Flezha\Executor\SwooleExecutor;
use Flezha\Executor\SwooleLoopState;
use Flezha\Executor\Executors;
use Flezha\Ffi\Core;
use PHPUnit\Framework\TestCase;
use Swoole\Thread;
use Swoole\Thread\Atomic;

/**
 * The threaded executor, on a build that has it.
 *
 * Skipped elsewhere rather than deleted: the executor is an axis of the run,
 * so the same suite runs on both builds and this arm is what CI's ZTS job
 * exercises.
 */
final class SwooleExecutorTest extends TestCase
{
    protected function setUp(): void
    {
        if (!PHP_ZTS || !class_exists(Thread::class)) {
            self::markTestSkipped('this build has no Swoole\Thread; run it on the ZTS image');
        }
    }

    public function testTheAdapterConstructsOnASatisfyingBuild(): void
    {
        self::assertInstanceOf(SwooleExecutor::class, Executors::swoole());
    }

    /**
     * The context crosses as an integer and each interpreter resolves its own
     * FFI handle — the whole of the thread-specific surface, exercised on day
     * one rather than left for a future adapter.
     */
    public function testAWorkerThreadAttachesToTheContextByAddress(): void
    {
        $core = Core::create();
        $schema = new Atomic(0);
        $claims = new Atomic(0);

        try {
            $thread = new Thread(
                __DIR__ . '/fixtures/swoole_probe.php',
                dirname(__DIR__) . '/vendor/autoload.php',
                $core->address(),
                $schema,
                $claims,
                4,
            );
            $thread->join();

            self::assertSame(Core::SCHEMA_VERSION, $schema->get());
            // Ten claims against a budget of four, counted in the shared
            // atomic: one runner has one budget, however many threads spend it.
            self::assertSame(10, $claims->get());
        } finally {
            // The creating thread frees, and only after every worker joined.
            $core->free();
        }
    }

    public function testABudgetIsSharedAcrossEveryLoop(): void
    {
        $state = new SwooleLoopState(2);

        self::assertTrue($state->claim());
        self::assertTrue($state->claim());
        self::assertFalse($state->claim());
    }

    public function testTheSharedStateReportsTheMeanBodyTime(): void
    {
        $state = new SwooleLoopState();
        $state->observe(2_000_000);
        $state->observe(4_000_000);

        self::assertSame([2, 3_000_000], $state->report());
    }

    public function testStoppingIsVisibleToEveryThread(): void
    {
        $state = new SwooleLoopState();
        [$stopping] = $state->atomics();

        self::assertFalse($state->stopping());
        $state->stop();
        self::assertSame(1, $stopping->get());
        self::assertTrue((new SwooleLoopState(null, $stopping))->stopping());
    }
}
