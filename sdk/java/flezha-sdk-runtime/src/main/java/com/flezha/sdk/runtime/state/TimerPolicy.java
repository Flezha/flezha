package com.flezha.sdk.runtime.state;

import com.flezha.sdk.runtime.entity.TimerCounters;
import com.flezha.sdk.state.EventTimeFrom;
import com.flezha.sdk.state.TimerKind;
import javax.annotation.Nonnull;

/**
 * What a Procedure declares about its timers, resolved once per Procedure
 * rather than at every invocation: the bound, the names a refusal reports, and
 * whether its Clock can serve an event-time deadline at all. Carries the
 * counter set too, so opening a session costs no registry lookup.
 *
 * <p>Mirrors the Rust SDK's {@code TimerPolicy}.
 */
public record TimerPolicy(
        int maxPerEntity,
        @Nonnull String procedure,
        boolean eventTimeClock,
        @Nonnull String clock,
        @Nonnull TimerCounters counters) {

    @Nonnull
    public static TimerPolicy of(int maxPerEntity, @Nonnull String procedure, @Nonnull Class<?> clockType) {
        return new TimerPolicy(
                maxPerEntity,
                procedure,
                clockType == EventTimeFrom.class,
                clockType.getSimpleName(),
                TimerCounters.of(procedure));
    }

    /** Open a session over a COPY of {@code pending}. */
    @Nonnull
    public TimerSession session(@Nonnull PendingTimers pending, long now) {
        return new TimerSession(this, pending.copy(), now);
    }

    /**
     * Open a session seeded with the removal of the timer that is firing —
     * which is what makes that removal ride the SAME durable record as the
     * effects the firing goes on to produce.
     */
    @Nonnull
    public TimerSession firingSession(@Nonnull PendingTimers pending, long firedAt, @Nonnull TimerKind kind) {
        TimerSession session = session(pending, firedAt);
        session.openFiring(kind, firedAt);
        return session;
    }
}
