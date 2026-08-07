package com.flezha.sdk.runtime.state;

import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.runtime.entity.TimerCounters;
import com.flezha.sdk.state.TimerKind;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * The timer surface for ONE invocation, held by the dispatcher's
 * {@code ProcedureCtx}.
 *
 * <p>It works on a <b>copy</b> of the Entity's pending set: the retry ladder
 * re-runs a failed attempt, and a registration that attempt made must not
 * survive it. The dispatcher calls {@link #commit} only on the attempt that
 * completed — which is also where the counters move, so a refused-then-retried
 * invocation is counted once.
 *
 * <p>Opened through {@link TimerPolicy#session} / {@link TimerPolicy#firingSession},
 * which resolve everything a Procedure declares about its timers once.
 */
public final class TimerSession {

    private final PendingTimers pending;
    private final List<TimerOp> ops = new ArrayList<>();
    private final TimerPolicy policy;
    private final long now;
    private boolean firing;

    TimerSession(@Nonnull TimerPolicy policy, @Nonnull PendingTimers pending, long now) {
        this.policy = policy;
        this.pending = pending;
        this.now = now;
    }

    /**
     * Seed the session with the removal of the timer that is firing, so it rides
     * the SAME durable record as whatever {@code onTimer} goes on to apply and
     * emit. A crash between the two is therefore impossible: either both landed,
     * or the timer is still pending and the whole firing repeats.
     */
    void openFiring(@Nonnull TimerKind kind, long scheduledAt) {
        TimerOp op = TimerOp.cancel(kind, scheduledAt);
        pending.apply(op);
        ops.add(op);
        firing = true;
    }

    /** Register an event-time timer at an absolute timestamp. */
    public long registerAtEventTime(long scheduledAt) throws ProcedureException {
        if (!policy.eventTimeClock()) {
            policy.counters().refused().incrementAndGet();
            throw new ProcedureException.EventTimeTimerWithoutEventTimeClock(policy.procedure(), policy.clock());
        }
        return register(TimerKind.EVENT_TIME, scheduledAt);
    }

    /** Register a processing-time timer {@code delay} after {@code ctx.now()}. */
    public long registerAfter(@Nonnull Duration delay) throws ProcedureException {
        return register(TimerKind.PROCESSING_TIME, now + delay.toMillis());
    }

    private long register(TimerKind kind, long scheduledAt) throws ProcedureException {
        if (!pending.contains(kind, scheduledAt) && pending.size() >= policy.maxPerEntity()) {
            policy.counters().refused().incrementAndGet();
            throw new ProcedureException.TimerCapExceeded(policy.procedure(), policy.maxPerEntity());
        }
        TimerOp op = TimerOp.register(kind, scheduledAt);
        if (pending.apply(op)) {
            ops.add(op);
        }
        return scheduledAt;
    }

    /**
     * Cancel a pending timer by its scheduled timestamp. {@code false} when none
     * was pending — a cancellation nobody registered changes nothing.
     */
    public boolean cancel(@Nonnull TimerKind kind, long scheduledAt) {
        TimerOp op = TimerOp.cancel(kind, scheduledAt);
        boolean changed = pending.apply(op);
        if (changed) {
            ops.add(op);
        }
        return changed;
    }

    /**
     * Publish the session onto the Entity's live pending set and return the ops
     * to make durable (and to arm or disarm on the wheel).
     */
    @Nonnull
    public List<TimerOp> commit(@Nonnull PendingTimers live) {
        TimerCounters counters = policy.counters();
        for (int i = 0; i < ops.size(); i++) {
            TimerOp op = ops.get(i);
            if (op.register()) {
                counters.registered().incrementAndGet();
                counters.pending().incrementAndGet();
                continue;
            }
            counters.pending().decrementAndGet();
            if (firing && i == 0) {
                counters.fired().incrementAndGet();
                TimerCounters.noteFired();
            } else {
                counters.cancelled().incrementAndGet();
            }
        }
        live.replaceWith(pending);
        return ops;
    }
}
