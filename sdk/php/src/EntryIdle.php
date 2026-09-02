<?php

declare(strict_types=1);

namespace Flezha;

use Flezha\Exception\FlezhaException;
use Flezha\Ffi\Binding;

/**
 * Idle observation for entry channels: the policy half of the input-liveness
 * signal the core records.
 *
 * The core knows when a frame last arrived and owns the episode latch over it
 * (`flz_core::entry_input::IdleWatch`, reached by watch id); only the
 * application knows whether the silence since then is anomalous — a quiet
 * overnight market is not a dead feed. So the SDK ships no default threshold
 * and no default action: {@see Runner::onEntryIdle()} takes both from the
 * caller and reports episodes.
 *
 * The tick rides the poll loop of the thread that started the runner —
 * between polls under `SyncExecutor`, between starvation waits under
 * `SwooleExecutor` — because this host has no thread of its own to put it on.
 * A body that runs past the tick therefore delays the report by its length,
 * and never changes the count: the latch compares stamps, not clock readings.
 */
final class EntryIdle
{
    /**
     * Tick floor and ceiling in milliseconds, the same in every SDK. The floor
     * bounds how often a fleet of observers wakes; the ceiling bounds how late
     * an episode is reported.
     */
    public const TICK_MIN_MS = 250;
    public const TICK_MAX_MS = 1_000;

    /** @var list<array{string, int, \Closure, ?\Closure}> channel, watch id, idle, resumed */
    private array $watches = [];

    /** When the next evaluation is due on the core's clock; 0 means now. */
    private int $nextTickNanos = 0;

    private ?int $periodMs = null;

    public function __construct(private readonly EntryIdleBridge $bridge)
    {
    }

    /**
     * The tick a `$thresholdMs` asks for: a quarter of it, so an episode is
     * reported within 25% of the threshold, bounded at both ends.
     */
    public static function tickPeriodMs(int $thresholdMs): int
    {
        return max(self::TICK_MIN_MS, min(self::TICK_MAX_MS, intdiv($thresholdMs, 4)));
    }

    /**
     * When `$channel` last received a frame, or null before its first frame
     * (and for a name no declared operative has — both mean "nothing has
     * arrived here").
     *
     * The core's stamp is monotonic; the wall-clock instant is derived from
     * the gap, so a clock step between then and now cannot skew it.
     */
    public static function lastEntryDelivery(EntryIdleBridge $bridge, string $channel): ?\DateTimeImmutable
    {
        $last = $bridge->entryLastDeliveryNanos($channel);
        if ($last === 0) {
            return null;
        }
        $gapSeconds = max($bridge->monotonicNanos() - $last, 0) / 1e9;
        $at = \DateTimeImmutable::createFromFormat('U.u', sprintf('%.6F', microtime(true) - $gapSeconds));

        return $at === false ? null : $at;
    }

    /**
     * Register `$observer` against `$channel`. Legal before and after
     * `start()`: the core interns the observation by name, so a registration
     * made before the class is declared watches the same channel it later
     * gets. Several observers may watch one channel.
     */
    public function register(string $channel, int $thresholdMs, IdleObserver|callable $observer): void
    {
        if ($thresholdMs <= 0) {
            throw new \InvalidArgumentException(
                "onEntryIdle: the threshold must be a positive number of milliseconds, got $thresholdMs",
            );
        }
        $id = $this->bridge->idleWatchNew($channel, $thresholdMs * 1_000_000);
        if ($id === 0) {
            throw new FlezhaException("onEntryIdle: the core refused the channel name `$channel`");
        }
        [$idle, $resumed] = $observer instanceof IdleObserver
            ? [$observer->idle(...), $observer->resumed(...)]
            : [\Closure::fromCallable($observer), null];
        $this->watches[] = [$channel, $id, $idle, $resumed];

        $wanted = self::tickPeriodMs($thresholdMs);
        if ($this->periodMs === null || $wanted < $this->periodMs) {
            $this->periodMs = $wanted;
            // A shorter period is due at once rather than after the old one.
            $this->nextTickNanos = 0;
        }
    }

    /**
     * Evaluate every watch once the period has passed, and deliver the
     * episodes the core found. Called between polls by the thread that owns
     * the registrations; a call inside the period costs one clock read.
     */
    public function tick(): void
    {
        if ($this->watches === []) {
            return;
        }
        $now = $this->bridge->monotonicNanos();
        if ($now < $this->nextTickNanos) {
            return;
        }
        $this->nextTickNanos = $now + ($this->periodMs ?? self::TICK_MAX_MS) * 1_000_000;

        foreach ($this->watches as [$channel, $id, $idle, $resumed]) {
            [$status, $gapNanos] = $this->bridge->idleWatchPoll($id);
            if ($status === Binding::IDLE_IDLE) {
                $this->deliver($channel, 'idle', $idle, $gapNanos);
            } elseif ($status === Binding::IDLE_RESUMED && $resumed !== null) {
                $this->deliver($channel, 'resumed', $resumed, $gapNanos);
            }
        }
    }

    /** Drop every registration, in the core too. Idempotent. */
    public function stop(): void
    {
        foreach ($this->watches as [, $id]) {
            $this->bridge->idleWatchFree($id);
        }
        $this->watches = [];
        $this->periodMs = null;
        $this->nextTickNanos = 0;
    }

    /**
     * Invoke the observer, contained: application code runs inside the poll
     * loop, so a throw must become a log line rather than a dead loop.
     */
    private function deliver(string $channel, string $episode, \Closure $call, int $gapNanos): void
    {
        try {
            $call($gapNanos / 1e6);
        } catch (\Throwable $error) {
            Logger::current()->warn(sprintf(
                "entry-idle observer threw on the %s episode of channel '%s': %s",
                $episode,
                $channel,
                $error->getMessage(),
            ));
        }
    }
}
