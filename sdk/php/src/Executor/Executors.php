<?php

declare(strict_types=1);

namespace Flezha\Executor;

use Flezha\Exception\FlezhaException;

/**
 * How a runner names its executor, including from an environment variable.
 *
 * The executor is an axis of the run rather than a property of a file:
 * `FLEZHA_PHP_EXECUTOR` (default `sync`) is what a test suite, an e2e arm and
 * a benchmark swap, so no fixture, test name or manifest entry exists in a
 * per-executor variant.
 *
 * `swoole()` is the only place outside the adapter's own files that mentions
 * it, and it mentions the class rather than any Swoole type — so PHP's
 * per-class autoloading leaves the adapter entirely inert until someone calls
 * this.
 */
final class Executors
{
    public const ENV = 'FLEZHA_PHP_EXECUTOR';

    private function __construct()
    {
    }

    /** One loop on the calling thread. The default, and dependency-free. */
    public static function sync(?int $maxActivations = null): Executor
    {
        return new SyncExecutor($maxActivations);
    }

    /**
     * A pool of `Swoole\Thread`s over one shared context.
     *
     * Checks its own requirements here rather than at install: the package
     * requires only php and ext-ffi, and a wrong build must produce an
     * instruction before the runner connects, not a fatal deep in the boot path.
     */
    public static function swoole(?int $maxThreads = null, ?int $maxActivations = null): Executor
    {
        return new SwooleExecutor($maxThreads, $maxActivations);
    }

    /**
     * The executor this run selects, defaulting to `sync`.
     *
     * @param int|null $maxActivations the runner's activation budget, if any
     */
    public static function fromEnvironment(?int $maxActivations = null): Executor
    {
        $name = strtolower(trim((string) (getenv(self::ENV) ?: 'sync')));

        return match ($name) {
            '', 'sync' => self::sync($maxActivations),
            'swoole' => self::swoole(null, $maxActivations),
            default => throw new FlezhaException(sprintf(
                '%s=%s names no executor; this SDK ships `sync` (the default) and `swoole`, and '
                . 'a host may pass one of its own to Runner::executor()',
                self::ENV,
                $name,
            )),
        };
    }
}
