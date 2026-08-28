<?php

declare(strict_types=1);

namespace Flezha;

use Flezha\Codec\Json;
use Flezha\Ffi\Core;

/**
 * The context a run-sugar workflow body receives.
 *
 * `recv()` takes a timeout and answers null on expiry rather than parking
 * indefinitely, so the body observes cancellation and a shutdown request
 * between attempts — which is what makes a run-sugar body stoppable without an
 * ABI addition.
 */
final class WorkflowCtx
{
    /** How long `recv()` parks when the caller names no timeout. */
    public const DEFAULT_RECV_TIMEOUT_MS = 1_000;

    private bool $completed = false;
    private mixed $result = null;

    public function __construct(
        public readonly string $instanceId,
        private readonly Core $core,
    ) {
    }

    /** Emit one output payload. Best-effort and at-least-once. */
    public function emit(mixed $value): void
    {
        $this->core->workflowEmit($this->instanceId, Json::encode($value));
    }

    /** The next inbox item, or null on timeout. */
    public function recv(int $timeoutMs = self::DEFAULT_RECV_TIMEOUT_MS): mixed
    {
        $payload = $this->core->workflowRecv($this->instanceId, $timeoutMs);

        return $payload === null ? null : Json::decode($payload);
    }

    /** Whether the body's cancellation token has tripped. */
    public function isCancelled(): bool
    {
        return $this->core->workflowCancelled($this->instanceId);
    }

    /** Set the terminal result; the body is completed once this is called. */
    public function complete(mixed $result = null): void
    {
        $this->completed = true;
        $this->result = $result;
    }

    public function isCompleted(): bool
    {
        return $this->completed;
    }

    public function result(): mixed
    {
        return $this->result;
    }

    public function now(): int
    {
        return $this->core->monotonicNanos();
    }
}
