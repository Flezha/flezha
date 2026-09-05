<?php

declare(strict_types=1);

namespace Flezha\Ffi;

use FFI;
use FFI\CData;
use Flezha\Exception\FlezhaException;
use Flezha\EntryIdleBridge;
use Flezha\SpawnBridge;

/**
 * One `FlzContext`, and every ABI call a running host makes against it.
 *
 * The veneer adds no ABI surface: each method below forwards to exactly one
 * exported function. What it adds is the ownership rule, which is a host rule
 * rather than a core one — **the thread that created the context frees it, and
 * only after every worker has been joined; a thread that attached never
 * frees.** {@see attach()} is the only way to get an un-owning handle, and
 * {@see free()} is a no-op on one.
 */
final class Core implements SpawnBridge, EntryIdleBridge
{
    /**
     * The activation schema this package was built against, passed to
     * `flz_core_context_new` so a shim paired with a mismatched core is
     * refused before any controller connection.
     *
     * Moves in the same commit as `flz_core/src/schema.rs`; the fixture suite
     * is what makes forgetting it a red build.
     */
    public const SCHEMA_VERSION = 3;

    /**
     * The context's address, or null once it has been released.
     *
     * An integer rather than a `CData` pointer: that is what a thread
     * bootstrap argument can carry, and it is also the only shape with no
     * lifetime of its own — see the note on {@see Binding}'s declarations.
     */
    private ?int $ctx;

    private function __construct(
        private readonly FFI $ffi,
        ?int $ctx,
        private readonly bool $owned,
    ) {
        $this->ctx = $ctx;
    }

    /**
     * A handle with no context, for the entry points that take none: the two
     * clock reads and the three watch calls read process-global tables, which
     * is what lets a runner answer `lastEntryDelivery` and register an idle
     * watch before `start()` has made its context. Every context-bound call
     * refuses on it.
     */
    public static function contextFree(): self
    {
        return new self(Binding::current(), null, false);
    }

    /**
     * Create the process's context, negotiating the activation schema version
     * before any controller connection is attempted.
     */
    public static function create(): self
    {
        $ffi = Binding::current();
        $ctx = (int) $ffi->flz_core_context_new(self::SCHEMA_VERSION);
        if ($ctx === 0) {
            throw new FlezhaException(
                self::cstring($ffi->flz_core_init_error())
                    ?? 'flz_core refused this shim and reported no reason',
            );
        }

        return new self($ffi, $ctx, true);
    }

    /**
     * Attach to a context another thread created, by the address it published.
     *
     * `flz_core_declare_class` is per context, not per thread, so a worker
     * rebuilds only the PHP-side registry and reaches the same core through
     * this — and never frees it.
     */
    public static function attach(int $address): self
    {
        if ($address === 0) {
            throw new FlezhaException('cannot attach to a null context address');
        }

        return new self(Binding::current(), $address, false);
    }

    /**
     * The context's address, as an integer a thread bootstrap argument can
     * carry: `Swoole\Thread` takes scalars, never objects or resources.
     */
    public function address(): int
    {
        return $this->live();
    }

    /** Whether this handle created the context and therefore frees it. */
    public function ownsContext(): bool
    {
        return $this->owned;
    }

    /** The schema version the loaded library speaks. */
    public function coreSchemaVersion(): int
    {
        return (int) $this->ffi->flz_core_schema_version();
    }

    /** Register one operative class from its `HostClassDecl` JSON. */
    public function declareClass(string $json): void
    {
        $this->check($this->ffi->flz_core_declare_class($this->live(), $json, strlen($json)));
    }

    /**
     * Judge one class declaration without registering it, and answer the
     * DERIVED declaration as JSON: the consumers, channel kinds, sink names
     * and acknowledgement granularity the core read off the record's chains.
     *
     * Read-only, so a caller asks for the verdict its author would get with
     * nothing registered and no runner. A refused record throws the core's
     * rendered verdict, which {@see \Flezha\Graph\Validate} re-raises as
     * the package's own declaration refusal — and leaves the verdict the core
     * hands out on the same failing call in `$verdict`, as
     * `{"issues":[{"name","text"}]}`, so nothing has to parse that prose.
     *
     * @param string|null $verdict the refusal as data, on the throwing arm
     */
    public function checkClass(string $json, ?string &$verdict = null): string
    {
        $answer = null;
        $status = $this->outBuffer(
            fn (CData $out, CData $len): int => $this->ffi->flz_core_check_class(
                $this->live(),
                $json,
                strlen($json),
                FFI::addr($out),
                FFI::addr($len),
            ),
            $answer,
        );
        // `flz_core_check_class` answers FLZ_OK, not the poll status, and fills
        // the buffer on both arms: the derived declaration, or the verdict.
        $verdict = $status === Binding::OK ? null : $answer;
        $this->check($status);

        return (string) $answer;
    }

    /** Connect to the controller and start the runner loop. */
    public function start(string $namespace): void
    {
        $this->check($this->ffi->flz_core_start($this->live(), $namespace));
    }

    /**
     * Block for up to `$timeoutMs` for one activation.
     *
     * Answers the ABI's own three-way status; the encoded activation lands in
     * `$bytes` on {@see Binding::POLL_ACTIVATION}. Shaped like the C call
     * rather than wrapped, so the one place the three answers are interpreted
     * is the loop.
     */
    public function poll(int $timeoutMs, ?string &$bytes): int
    {
        $polled = null;
        $status = $this->outBuffer(
            fn (CData $out, CData $len): int => $this->ffi->flz_core_poll(
                $this->live(),
                $timeoutMs,
                FFI::addr($out),
                FFI::addr($len),
            ),
            $polled,
        );
        $bytes = $polled;
        if ($status === Binding::ERR) {
            $this->check($status);
        }

        return $status;
    }

    /**
     * Block until the core observes starvation: work queued with no thread
     * inside `poll`. Answers in `poll`'s three-way shape.
     */
    public function waitStarvation(int $timeoutMs): int
    {
        return $this->ffi->flz_core_wait_starvation($this->live(), $timeoutMs);
    }

    /** Complete an activation. Accepted from any thread. */
    public function complete(string $encoded): void
    {
        $this->check($this->ffi->flz_core_complete($this->live(), $encoded, strlen($encoded)));
    }

    /** Re-hand a polled activation's untouched items and release its entity. */
    public function abandon(int $token): void
    {
        $this->check($this->ffi->flz_core_abandon($this->live(), $token));
    }

    /** Ship every Entity's state to the controller, then wake every blocked poller and stop the runner loop. */
    public function shutdown(): void
    {
        $this->ffi->flz_core_shutdown($this->live());
    }

    /** Emit one output payload from a live workflow run-sugar body. */
    public function workflowEmit(string $instanceId, string $payload): void
    {
        $this->check(
            $this->ffi->flz_core_workflow_emit($this->live(), $instanceId, $payload, strlen($payload)),
        );
    }

    /** The next inbox item of a live run-sugar body, or null on timeout. */
    public function workflowRecv(string $instanceId, int $timeoutMs): ?string
    {
        $payload = null;
        $this->check($this->outBuffer(
            fn (CData $out, CData $len): int => $this->ffi->flz_core_workflow_recv(
                $this->live(),
                $instanceId,
                $timeoutMs,
                FFI::addr($out),
                FFI::addr($len),
            ),
            $payload,
        ));

        return $payload;
    }

    /** Whether the named body's cancellation token has tripped. */
    public function workflowCancelled(string $instanceId): bool
    {
        return $this->ffi->flz_core_workflow_cancelled($this->live(), $instanceId) === 1;
    }

    /** Spawn-or-deliver a workflow instance; answers the controller's outcome. */
    public function spawnWorkflow(
        string $instanceId,
        string $factoryId,
        string $payload,
        string $ifRunning,
    ): string {
        $outcome = null;
        $this->check($this->outBuffer(
            fn (CData $out, CData $len): int => $this->ffi->flz_core_spawn_workflow(
                $this->live(),
                $instanceId,
                $factoryId,
                $payload,
                strlen($payload),
                $ifRunning,
                FFI::addr($out),
                FFI::addr($len),
            ),
            $outcome,
        ));

        return (string) $outcome;
    }

    /** The core's monotonic clock, in nanoseconds since a process origin. */
    public function monotonicNanos(): int
    {
        return (int) $this->ffi->flz_core_monotonic_nanos();
    }

    /** When entry channel `$channel` last received a frame, on that clock. */
    public function entryLastDeliveryNanos(string $channel): int
    {
        return (int) $this->ffi->flz_core_entry_last_delivery_nanos($channel);
    }

    /**
     * Register an episode latch over entry channel `$channel` in the core; the
     * id, or 0 when the name is refused. The tick that advances it is the
     * runner's, between polls.
     */
    public function idleWatchNew(string $channel, int $thresholdNanos): int
    {
        return (int) $this->ffi->flz_core_idle_watch_new($channel, $thresholdNanos);
    }

    /**
     * Advance a watch's latch to now: `[status, gapNanos]`, the status one of
     * {@see Binding::IDLE_NONE}, {@see Binding::IDLE_IDLE},
     * {@see Binding::IDLE_RESUMED} or {@see Binding::ERR} for an unknown id.
     *
     * @return array{int, int}
     */
    public function idleWatchPoll(int $id): array
    {
        $gap = $this->ffi->new('uint64_t');
        $status = (int) $this->ffi->flz_core_idle_watch_poll($id, FFI::addr($gap));

        return [$status, (int) $gap->cdata];
    }

    /** Drop a watch. */
    public function idleWatchFree(int $id): void
    {
        $this->ffi->flz_core_idle_watch_free($id);
    }

    /**
     * Release the context. A no-op on an attached handle, which is what makes
     * the ownership rule structural rather than a caller's discipline.
     */
    public function free(): void
    {
        $ctx = $this->ctx;
        $this->ctx = null;
        if ($ctx !== null && $this->owned) {
            $this->ffi->flz_core_context_free($ctx);
        }
    }

    /**
     * Call an entry point that hands back a core-allocated buffer, and take it.
     *
     * One place, because all four have the same obligation and the same
     * shape: the core allocated the buffer and the core frees it, so forgetting
     * the free leaks per activation rather than visibly, and every one of them
     * writes through a `char **` / `size_t *` pair the caller must own.
     *
     * Taken whenever the core wrote one, never only on the status that usually
     * carries it: `flz_core_check_class` hands out its verdict on the FAILING
     * status, and a buffer a caller does not take is a buffer nobody frees.
     *
     * @param callable(CData, CData): int $call  fills the pair; answers the status
     * @param string|null                 $bytes the buffer, when there was one
     */
    private function outBuffer(callable $call, ?string &$bytes): int
    {
        $out = $this->ffi->new('char*');
        $len = $this->ffi->new('size_t');
        $bytes = null;

        $status = $call($out, $len);
        if (FFI::isNull($out)) {
            return $status;
        }

        $length = (int) $len->cdata;
        // `FFI::string` reads through the `char *` itself; the CData IS the
        // pointer, and a `->cdata` on it is a read of a field a pointer has no
        // more than a struct does.
        $bytes = $length === 0 ? '' : FFI::string($out, $length);
        $this->ffi->flz_core_buf_free($out, $length);

        return $status;
    }

    /** The context, refusing a call on a freed one rather than segfaulting. */
    private function live(): int
    {
        if ($this->ctx === null) {
            throw new FlezhaException('the flezha core context has been freed; this call is a defect');
        }

        return $this->ctx;
    }

    /** Turn an ABI failure into the reason the core recorded on the context. */
    private function check(int $status): void
    {
        if ($status !== Binding::ERR) {
            return;
        }
        throw new FlezhaException(
            self::cstring($this->ffi->flz_core_last_error($this->live()))
                ?? 'the flezha core refused a call and recorded no reason',
        );
    }

    /**
     * A borrowed `const char *` as a PHP string, or null when there is none.
     *
     * PHP's FFI converts a `char *` return to a string on its own, so what
     * arrives is a string, a null, or — on a build that hands back the pointer
     * — `CData`. One reader for all three, because getting it wrong turns
     * every core refusal into a `TypeError` that hides the refusal.
     */
    private static function cstring(mixed $value): ?string
    {
        if (is_string($value)) {
            return $value === '' ? null : $value;
        }
        if ($value instanceof CData && !FFI::isNull($value)) {
            return FFI::string($value);
        }

        return null;
    }
}
