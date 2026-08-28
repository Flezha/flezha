<?php

declare(strict_types=1);

namespace Flezha\Tests;

use Flezha\Exception\FlezhaException;
use Flezha\Executor\Executors;
use Flezha\Executor\LocalLoopState;
use Flezha\Executor\SyncExecutor;
use PHPUnit\Framework\TestCase;

/** The concurrency seam, and the budget and metrics every loop shares. */
final class ExecutorTest extends TestCase
{
    protected function tearDown(): void
    {
        putenv(Executors::ENV);
    }

    public function testSyncIsTheDefault(): void
    {
        self::assertInstanceOf(SyncExecutor::class, Executors::fromEnvironment());

        putenv(Executors::ENV . '=');
        self::assertInstanceOf(SyncExecutor::class, Executors::fromEnvironment());

        putenv(Executors::ENV . '=sync');
        self::assertInstanceOf(SyncExecutor::class, Executors::fromEnvironment());
    }

    public function testAnUnknownExecutorNamesTheOnesThatExist(): void
    {
        putenv(Executors::ENV . '=fibers');

        $this->expectException(FlezhaException::class);
        $this->expectExceptionMessage('this SDK ships `sync` (the default) and `swoole`');

        Executors::fromEnvironment();
    }

    /**
     * The wrong build is an instruction, not `Class "Swoole\Thread" not found`
     * deep in the boot path — and it is refused before the runner connects.
     */
    public function testSwooleOnAWrongBuildNamesWhatIsMissing(): void
    {
        if (PHP_ZTS && class_exists(\Swoole\Thread::class)) {
            self::markTestSkipped('this build satisfies the threaded executor; see SwooleExecutorTest');
        }

        $this->expectException(FlezhaException::class);
        $this->expectExceptionMessageMatches('/thread-safe \(ZTS\) PHP build|Swoole\\\\Thread/');

        Executors::swoole();
    }

    /**
     * Every arm of the threaded executor's requirement check, on one machine.
     *
     * `ffi.enable` is PHP_INI_SYSTEM, so a satisfying build cannot be made
     * unsatisfying at runtime — which is why the check is a pure function of
     * what it reads rather than of what it looks up.
     *
     * @return iterable<string, array{bool, bool, string, string|null}>
     */
    public static function requirements(): iterable
    {
        yield 'a non-thread-safe build' => [false, true, '1', 'thread-safe (ZTS) PHP build'];
        yield 'no thread API' => [true, false, '1', '--enable-swoole-thread'];
        yield 'FFI restricted to preload' => [true, true, 'preload', 'needs ffi.enable=true'];
        yield 'FFI off' => [true, true, '', 'needs ffi.enable=true'];
        yield 'everything present' => [true, true, 'true', null];
        yield 'everything present, numerically' => [true, true, '1', null];
    }

    /** @dataProvider requirements */
    public function testTheThreadedExecutorNamesWhatABuildIsMissing(
        bool $zts,
        bool $hasThread,
        string $ffiEnable,
        ?string $expected,
    ): void {
        $refusal = \Flezha\Executor\SwooleExecutor::requirementRefusal($zts, $hasThread, $ffiEnable);

        if ($expected === null) {
            self::assertNull($refusal);

            return;
        }
        self::assertNotNull($refusal);
        self::assertStringContainsString($expected, $refusal);
    }

    /** `removeType` is the inverse of `defineStream` / `defineWorkflow`. */
    public function testARemovedTypeIsNotDeclared(): void
    {
        $runner = \Flezha\Flezha::createRunner('test')
            ->defineStream(EchoOperative::class)
            ->defineWorkflow(CounterOperative::class);

        self::assertSame(
            ['echo_operative'],
            self::declaredFactories($runner->removeType(CounterOperative::class)),
        );
        self::assertSame([], self::declaredFactories($runner->removeType(EchoOperative::class)));
    }

    /** @return list<string> */
    private static function declaredFactories(\Flezha\Runner $runner): array
    {
        $classes = (new \ReflectionProperty(\Flezha\Runner::class, 'classes'))->getValue($runner);

        return array_map(static fn (string $class): string => $class::spec()->factoryId, $classes);
    }

    public function testAnUnlimitedBudgetNeverRefusesAClaim(): void
    {
        $state = new LocalLoopState();

        for ($i = 0; $i < 1_000; $i++) {
            self::assertTrue($state->claim());
        }
    }

    /**
     * The budget is what lets an external supervisor recycle a long-lived
     * process: the loop stops claiming, the runner shuts down cleanly, and the
     * activation it did not claim is handed back untouched.
     */
    public function testABudgetIsSpentExactlyOnce(): void
    {
        $state = new LocalLoopState(2);

        self::assertTrue($state->claim());
        self::assertTrue($state->claim());
        self::assertFalse($state->claim());
        self::assertFalse($state->claim());
    }

    public function testStoppingIsObservedByEveryLoop(): void
    {
        $state = new LocalLoopState();

        self::assertFalse($state->stopping());
        $state->stop();
        self::assertTrue($state->stopping());
    }

    /**
     * The sizing arithmetic is measured rather than assumed: throughput per
     * loop is roughly the inverse of body latency, so the runner reports the
     * mean it actually saw.
     */
    public function testTheReportIsTheMeanBodyTime(): void
    {
        $state = new LocalLoopState();

        self::assertSame([0, 0], $state->report());

        $state->observe(1_000_000);
        $state->observe(3_000_000);

        self::assertSame([2, 2_000_000], $state->report());
    }
}
