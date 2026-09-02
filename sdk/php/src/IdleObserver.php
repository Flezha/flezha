<?php

declare(strict_types=1);

namespace Flezha;

/**
 * What an application does about an entry channel that has gone quiet.
 *
 * Registered per channel with a threshold; the SDK reports **episodes**, not
 * ticks: `idle` fires once when the silence first exceeds the threshold and
 * not again while it continues, and `resumed` fires once when a frame finally
 * arrives, re-arming the next episode. Silence before the very first frame is
 * never an episode — an operative waiting for its subscriptions to take is not
 * an incident.
 *
 * Both calls run between polls of the thread that started the runner. A throw
 * is caught and logged; delivery and later episodes are unaffected. A
 * `callable` passed to {@see Runner::onEntryIdle()} is an observer whose
 * `resumed` does nothing.
 */
interface IdleObserver
{
    /** The channel has been silent for `$gapMs`, which first exceeded the registered threshold. */
    public function idle(float $gapMs): void;

    /**
     * A frame arrived, ending an episode that had lasted `$gapMs`.
     *
     * `$gapMs` is measured to the newest frame the tick saw, so a burst that
     * resumes and continues within one tick reports the silence plus that
     * burst — bounded by the tick, i.e. at most a quarter of the threshold.
     */
    public function resumed(float $gapMs): void;
}
