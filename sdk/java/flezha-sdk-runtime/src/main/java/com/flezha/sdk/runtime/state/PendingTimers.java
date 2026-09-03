package com.flezha.sdk.runtime.state;

import com.flezha.sdk.state.TimerKind;
import java.util.NavigableSet;
import java.util.TreeSet;
import javax.annotation.Nonnull;

/**
 * An Entity's pending timers: one ordered set per kind, rebuilt by the ordinary
 * change-log fold on restore rather than held in a store of its own.
 *
 * <p>Pending timers are a <b>set</b>, so registering the same kind and timestamp
 * twice is idempotent. Not thread-safe: every mutation happens under the owning
 * {@code EntitySlot}'s state lock, the same exclusion {@code run} holds.
 */
public final class PendingTimers {

    private final NavigableSet<Long> eventTime = new TreeSet<>();
    private final NavigableSet<Long> processingTime = new TreeSet<>();

    public PendingTimers() {}

    /** Deep copy — a {@link TimerSession} works on one so a failed attempt leaves nothing behind. */
    @Nonnull
    public PendingTimers copy() {
        PendingTimers out = new PendingTimers();
        out.eventTime.addAll(eventTime);
        out.processingTime.addAll(processingTime);
        return out;
    }

    /** Replace this set's contents with {@code other}'s. */
    public void replaceWith(@Nonnull PendingTimers other) {
        eventTime.clear();
        eventTime.addAll(other.eventTime);
        processingTime.clear();
        processingTime.addAll(other.processingTime);
    }

    private NavigableSet<Long> setFor(TimerKind kind) {
        return kind == TimerKind.EVENT_TIME ? eventTime : processingTime;
    }

    /**
     * Fold one op. The single mutation seam: registration, cancellation, firing
     * and the restore replay all go through it, so the live set and a rebuilt
     * one cannot drift.
     *
     * @return whether the set actually changed
     */
    public boolean apply(@Nonnull TimerOp op) {
        return op.register()
                ? setFor(op.kind()).add(op.scheduledAt())
                : setFor(op.kind()).remove(op.scheduledAt());
    }

    public boolean contains(@Nonnull TimerKind kind, long scheduledAt) {
        return setFor(kind).contains(scheduledAt);
    }

    public int size() {
        return eventTime.size() + processingTime.size();
    }

    public boolean isEmpty() {
        return eventTime.isEmpty() && processingTime.isEmpty();
    }
}
