package com.flezha.sdk.runtime.operative;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nonnull;

/**
 * Per-{@code AddOperative} runtime marker for a bounded-kind operative, carrying
 * its live <strong>control surface</strong>: a cancellation token and an inbox.
 * Unlike {@link ContinuousOperativeInstance} it spawns no segment workers and
 * holds no continuous mailboxes — the bounded's sequential driver (see
 * {@code BoundedOperativeRunner}) runs on a dedicated thread and reaches this
 * instance for cancellation + post-start input.
 *
 * <p>The control surface is instance-scoped (not process-global): the
 * {@code RemOperative} hard stop ({@link #cancel}), the cooperative
 * {@code BoundedStop} handler ({@link #cancel}), the {@code maxDuration} watcher
 * ({@link #cancel}), and the primary-stream dispatch path ({@link #offerInbox})
 * all route through the live instance held in the operative instance registry,
 * which is itself cleared on reconnect.
 *
 * <p>Co-located with {@link ContinuousOperativeInstance} so the sealed
 * {@link Operative} seal holds; the runtime bounded plumbing otherwise lives in
 * {@code com.flezha.sdk.runtime.bounded}.
 */
public final class BoundedOperativeInstance implements Operative {

    /** Inbox capacity — post-start input frames beyond this are dropped (best-effort). */
    private static final int INBOX_CAPACITY = 1024;

    private final String instanceId;
    private final String factoryId;
    private final AtomicBoolean cancelled = new AtomicBoolean(false);
    private final BlockingQueue<byte[]> inbox = new LinkedBlockingQueue<>(INBOX_CAPACITY);

    public BoundedOperativeInstance(@Nonnull String instanceId, @Nonnull String factoryId) {
        this.instanceId = instanceId;
        this.factoryId = factoryId;
    }

    @Override
    @Nonnull
    public String instanceId() {
        return instanceId;
    }

    @Nonnull
    public String factoryId() {
        return factoryId;
    }

    /** Trip the cancellation token (cooperative {@code BoundedStop}, hard {@code RemOperative}, or duration elapse). */
    public void cancel() {
        cancelled.set(true);
    }

    /** True once the cancellation token has been tripped. */
    public boolean isCancelled() {
        return cancelled.get();
    }

    /** The inbox drained by {@code BoundedOperativeCtx.recv()}; fed by the dispatch path. */
    @Nonnull
    public BlockingQueue<byte[]> inbox() {
        return inbox;
    }

    /**
     * Route a post-start input frame to this instance's inbox. Returns
     * {@code true} if accepted, {@code false} if the inbox is full (the frame
     * is dropped — best-effort delivery).
     */
    public boolean offerInbox(@Nonnull byte[] payload) {
        return inbox.offer(payload);
    }
}
