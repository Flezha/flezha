<?php

declare(strict_types=1);

namespace Flezha;

/**
 * The SDK's diagnostics, through a logger the host supplies.
 *
 * The default writes to standard error at `warn` and above, which is what the
 * other SDKs' defaults amount to: a runner that says nothing about a wedge is
 * the failure mode diagnostics exist to close, and one that narrates every
 * activation is noise. No logging dependency — a host that wants PSR-3, spans
 * or structured fields supplies its own.
 */
class Logger
{
    private const ORDER = ['debug' => 0, 'info' => 1, 'warn' => 2, 'error' => 3];

    private readonly int $level;

    /**
     * Per-interpreter, which on a ZTS build means per-thread: a worker thread
     * bootstraps its own and never shares one across a boundary that objects
     * cannot cross anyway.
     */
    private static ?self $current = null;

    public function __construct(string $level = 'warn')
    {
        $this->level = self::ORDER[strtolower($level)] ?? self::ORDER['warn'];
    }

    public static function current(): self
    {
        return self::$current ??= new self((string) (getenv('FLEZHA_LOG_LEVEL') ?: 'warn'));
    }

    public static function use(self $logger): void
    {
        self::$current = $logger;
    }

    public function debug(string $message): void
    {
        $this->write('debug', $message);
    }

    public function info(string $message): void
    {
        $this->write('info', $message);
    }

    public function warn(string $message): void
    {
        $this->write('warn', $message);
    }

    public function error(string $message): void
    {
        $this->write('error', $message);
    }

    protected function write(string $level, string $message): void
    {
        if (self::ORDER[$level] < $this->level) {
            return;
        }

        // A logger must never take the process down. The case that reaches
        // here is a closed standard stream during shutdown — the harness
        // closes the runner's pipes to stop it — where the write fails and an
        // unhandled failure would turn a clean drain into a crash.
        $stream = @fopen('php://stderr', 'wb');
        if ($stream !== false) {
            @fwrite($stream, "[flezha] $message\n");
            @fclose($stream);
        }
    }
}
