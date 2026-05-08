package com.flezha.sdk.runtime.partition;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nonnull;

/**
 * Per-Partition unbounded mailbox + drain-inflight CAS gate. A producer's
 * {@link #push(Object)} returns {@code true} when the caller should submit
 * a drain task (because the inflight CAS won), {@code false} when an
 * existing drainer will pick up the new item.
 *
 * <p>Sticky per-Partition ordering invariant: at most one drainer per slot
 * is active at any time, so {@link Object#wait()}-free serialization on the
 * partition state holds without further locking on the consumer side.
 */
public final class PartitionMailbox<T> {

    private final ConcurrentLinkedQueue<T> queue = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean inflight = new AtomicBoolean(false);

    /**
     * Push an item. Returns {@code true} if the caller won the inflight CAS
     * and SHOULD submit a drain task; {@code false} if an existing drainer
     * will pick up the new item from its drain loop.
     */
    public boolean push(@Nonnull T item) {
        queue.offer(item);
        return tryMarkInflight();
    }

    /** Pop one item, or {@code null} if the mailbox is empty. */
    public T pop() {
        return queue.poll();
    }

    public boolean isEmpty() {
        return queue.isEmpty();
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

    public int size() {
        return queue.size();
    }
}
