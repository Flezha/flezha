package com.flezha.sdk.runtime.entity;

import com.flezha.sdk.channel.OverflowPolicy;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nonnull;

/**
 * Per-Entity bounded mailbox + drain-inflight CAS gate. Push behaviour
 * on a full mailbox is governed by the channel's {@link OverflowPolicy}:
 *
 * <ul>
 *   <li>{@link OverflowPolicy#BLOCK} — producer parks until space is
 *       available (preserves ordering).</li>
 *   <li>{@link OverflowPolicy#DROP_NEWEST} — incoming item is discarded;
 *       {@link #dropsNewestTotal()} increments.</li>
 *   <li>{@link OverflowPolicy#DROP_OLDEST} — atomically pop the oldest
 *       item and accept the new one; {@link #dropsOldestTotal()}
 *       increments. Best-effort under contention.</li>
 * </ul>
 *
 * <p>Sticky per-Entity ordering invariant: at most one drainer per
 * slot is active at any time, so entity-state mutation in the consumer
 * serializes without further locking. {@link #push(Object)} returns
 * {@code true} only when the inflight CAS won — that's the caller's signal
 * to submit a drain bounded.
 */
public final class EntityMailbox<T> {

    private final BlockingQueue<T> queue;
    private final OverflowPolicy policy;
    private final AtomicBoolean inflight = new AtomicBoolean(false);
    private final AtomicLong dropsNewest = new AtomicLong(0);
    private final AtomicLong dropsOldest = new AtomicLong(0);

    public EntityMailbox(int capacity, @Nonnull OverflowPolicy policy) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("EntityMailbox capacity must be > 0, got " + capacity);
        }
        this.queue = new ArrayBlockingQueue<>(capacity);
        this.policy = policy;
    }

    /**
     * Push an item. Returns {@code true} if the caller won the inflight
     * CAS and SHOULD submit a drain bounded; {@code false} otherwise (either
     * an existing drainer will pick it up, or the item was dropped).
     *
     * <p>For {@link OverflowPolicy#BLOCK}, the producer thread parks until
     * the consumer drains space. If interrupted while parked the method
     * re-asserts the interrupt flag and returns {@code false}.
     */
    public boolean push(@Nonnull T item) {
        boolean accepted;
        switch (policy) {
            case BLOCK -> {
                try {
                    // Count only a real park: an offer that fits is not back-pressure.
                    if (queue.remainingCapacity() == 0) {
                        EntityCounters.noteBlockedPark();
                    }
                    queue.put(item);
                    accepted = true;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
            case DROP_NEWEST -> {
                accepted = queue.offer(item);
                if (!accepted) {
                    dropsNewest.incrementAndGet();
                    return false;
                }
            }
            case DROP_OLDEST -> {
                if (!queue.offer(item)) {
                    // Best-effort: pop one old item and retry. Under MPMC
                    // contention the second offer might still fail; we
                    // count the drop and give up rather than spin.
                    queue.poll();
                    accepted = queue.offer(item);
                    dropsOldest.incrementAndGet();
                    if (!accepted) {
                        return false;
                    }
                } else {
                    accepted = true;
                }
            }
            default -> throw new IllegalStateException("unknown OverflowPolicy " + policy);
        }
        return tryMarkInflight();
    }

    /** Pop one item, or {@code null} if the mailbox is empty. */
    public T pop() {
        return queue.poll();
    }

    /**
     * Discard every queued item, returning how many were dropped and freeing
     * any {@link OverflowPolicy#BLOCK} producer parked on space.
     *
     * <p>Used by the restore path, and only for a class whose inputs the
     * controller journals before forwarding — it re-forwards them, so the queue
     * is a subset of the durable log. See
     * {@link EntitySlot#discardQueuedInputOnRestore()}.
     */
    public int clear() {
        int discarded = 0;
        while (queue.poll() != null) {
            discarded++;
        }
        return discarded;
    }

    public boolean isEmpty() {
        return queue.isEmpty();
    }

    public int size() {
        return queue.size();
    }

    /** Bounded capacity declared at construction. */
    public int capacity() {
        return queue.remainingCapacity() + queue.size();
    }

    /** Per-channel backpressure policy applied when full. */
    @Nonnull
    public OverflowPolicy policy() {
        return policy;
    }

    /** Count of pushes rejected under {@link OverflowPolicy#DROP_NEWEST}. */
    public long dropsNewestTotal() {
        return dropsNewest.get();
    }

    /** Count of evictions under {@link OverflowPolicy#DROP_OLDEST}. */
    public long dropsOldestTotal() {
        return dropsOldest.get();
    }

    /**
     * Try to acquire the inflight gate. Returns {@code true} if the caller
     * won the CAS — the caller is now responsible for draining.
     */
    public boolean tryMarkInflight() {
        return inflight.compareAndSet(false, true);
    }

    /** Release the inflight gate. */
    public void releaseInflight() {
        inflight.set(false);
    }
}
