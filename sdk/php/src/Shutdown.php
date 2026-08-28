<?php

declare(strict_types=1);

namespace Flezha;

/**
 * How a runner learns it should stop, and the two ways that are not the core's
 * own.
 *
 * Both are **polled**, not asynchronous: a non-thread-safe PHP has no second
 * thread to put a watcher on, and a signal handler that ran inside a procedure
 * body would stop the runner from underneath the activation it is holding. So
 * the poll timeout is the stated upper bound on how quickly either is
 * observed, rather than an accident.
 */
final class Shutdown
{
    /** @var resource|null standard input, while it is worth watching */
    private $stdin = null;

    private function __construct()
    {
    }

    /**
     * Install the observers and answer the handle that reads them.
     *
     * @param callable():void $stop what a signal or an end of file asks for
     */
    public static function watch(callable $stop): self
    {
        $shutdown = new self();

        if (function_exists('pcntl_signal')) {
            pcntl_async_signals(false);
            foreach ([SIGINT, SIGTERM] as $signal) {
                pcntl_signal($signal, static function () use ($stop): void {
                    $stop();
                });
            }
        } else {
            // Said out loud rather than degraded silently: an orchestrator
            // stops a runner with SIGTERM, and without pcntl PHP's default
            // action ends the process immediately — in-flight activations are
            // abandoned rather than drained. The core re-hands them on the
            // next bind, so nothing is lost, but the shutdown is abrupt where
            // it did not need to be.
            Logger::current()->warn(
                'ext-pcntl is not loaded, so SIGTERM will end this process immediately instead '
                . 'of draining what is in flight. It ships with PHP and is enabled on most '
                . 'distribution CLI builds; the official Docker images need '
                . '`docker-php-ext-install pcntl`. Closing standard input still drains.',
            );
        }

        // The e2e harness manages a runner as an Erlang Port and stops it by
        // closing the port — which is this process's standard input.
        if (!stream_isatty(STDIN)) {
            $shutdown->stdin = STDIN;
            stream_set_blocking(STDIN, false);
        }

        return $shutdown;
    }

    /** A handle that observes nothing, for a worker thread. */
    public static function unwatched(): self
    {
        return new self();
    }

    /**
     * Dispatch pending signals and check standard input.
     *
     * Answers whether standard input has reached end of file, which the caller
     * reports before stopping — the harness's stop is not an operator's, and
     * saying which one arrived is the difference between a clean drain and a
     * mystery.
     */
    public function observe(): bool
    {
        if (function_exists('pcntl_signal_dispatch')) {
            pcntl_signal_dispatch();
        }
        if ($this->stdin === null || !$this->atEof($this->stdin)) {
            return false;
        }

        // Once, and then stop looking: the descriptor stays readable.
        $this->stdin = null;

        return true;
    }

    /**
     * Whether standard input has genuinely reached end of file.
     *
     * A read **error** is not end of file, and conflating the two is how a
     * runner shuts itself down moments after connecting. Readable and empty is
     * end of file; readable with bytes is the harness writing to us, which is
     * not a shutdown at all.
     *
     * @param resource $stdin
     */
    private function atEof($stdin): bool
    {
        $read = [$stdin];
        $write = [];
        $except = [];

        if (@stream_select($read, $write, $except, 0) !== 1) {
            return false;
        }

        return fread($stdin, 4096) === '' && feof($stdin);
    }
}
