package com.flezha.sdk.runtime.segment;

import com.flezha.sdk.channel.OverflowPolicy;
import com.flezha.sdk.runtime.worker.OperationDispatcher;
import com.flezha.sdk.state.Change;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nonnull;

/**
 * Per-(operation, segment) state + mailbox + dispatcher. The {@code state} field
 * is type-erased ({@code AtomicReference<Object>}) because the topology builder
 * wires Operations by reflective {@link Class}; the dispatcher knows the concrete
 * Operation / State / Input / Output types and casts inside the drain loop.
 *
 * <p>The change-buffer fields below back the per-Segment flusher's wire
 * frame construction. They are populated only when the Operation's {@code Change}
 * is non-{@code NoChange}; for {@code NoChange} Operations the buffer remains
 * empty and the flusher skips registration entirely.
 */
public final class SegmentSlot {

    private final ReentrantLock stateLock = new ReentrantLock();
    private final AtomicReference<Object> state = new AtomicReference<>();
    private final SegmentMailbox<Object> mailbox;
    private final OperationDispatcher dispatcher;
    private final int operationId;
    private final Optional<String> operationKey;
    private final boolean changeful;
    private final Object changeBufferLock = new Object();
    private final List<Change> changeBuffer = new ArrayList<>();
    private final AtomicLong changeSeq = new AtomicLong(0L);

    public SegmentSlot(@Nonnull OperationDispatcher dispatcher, int operationId, @Nonnull Optional<String> operationKey) {
        this(dispatcher, operationId, operationKey, false, 1024, OverflowPolicy.BLOCK);
    }

    public SegmentSlot(
            @Nonnull OperationDispatcher dispatcher, int operationId, @Nonnull Optional<String> operationKey, boolean changeful) {
        this(dispatcher, operationId, operationKey, changeful, 1024, OverflowPolicy.BLOCK);
    }

    public SegmentSlot(
            @Nonnull OperationDispatcher dispatcher,
            int operationId,
            @Nonnull Optional<String> operationKey,
            boolean changeful,
            int mailboxCapacity,
            @Nonnull OverflowPolicy overflowPolicy) {
        this.dispatcher = dispatcher;
        this.operationId = operationId;
        this.operationKey = operationKey;
        this.changeful = changeful;
        this.mailbox = new SegmentMailbox<>(mailboxCapacity, overflowPolicy);
    }

    @Nonnull
    public ReentrantLock stateLock() {
        return stateLock;
    }

    @Nonnull
    public AtomicReference<Object> state() {
        return state;
    }

    @Nonnull
    public SegmentMailbox<Object> mailbox() {
        return mailbox;
    }

    @Nonnull
    public OperationDispatcher dispatcher() {
        return dispatcher;
    }

    public int operationId() {
        return operationId;
    }

    @Nonnull
    public Optional<String> operationKey() {
        return operationKey;
    }

    /** Whether this Operation declares a non-{@code NoChange} Change type. */
    public boolean changeful() {
        return changeful;
    }

    /** Lock the flusher acquires before draining the buffer. */
    @Nonnull
    public Object changeBufferLock() {
        return changeBufferLock;
    }

    /** Mutable per-Segment Change buffer; the flusher drains and serializes. */
    @Nonnull
    public List<Change> changeBuffer() {
        return changeBuffer;
    }

    /** Monotonic per-Segment sequence number applied to flushed Change frames. */
    @Nonnull
    public AtomicLong changeSeq() {
        return changeSeq;
    }
}
