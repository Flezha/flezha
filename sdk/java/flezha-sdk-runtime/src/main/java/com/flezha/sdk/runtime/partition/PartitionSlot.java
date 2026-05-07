package com.flezha.sdk.runtime.partition;

import com.flezha.sdk.runtime.worker.ShardDispatcher;
import com.flezha.sdk.state.Change;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nonnull;

/**
 * Per-(shard, partition) state + mailbox + dispatcher. The {@code state} field
 * is type-erased ({@code AtomicReference<Object>}) because the topology builder
 * wires Shards by reflective {@link Class}; the dispatcher knows the concrete
 * Shard / State / Input / Output types and casts inside the drain loop.
 *
 * <p>The change-buffer fields below back the per-Partition flusher's wire
 * frame construction. They are populated only when the Shard's {@code Change}
 * is non-{@code NoChange}; for {@code NoChange} Shards the buffer remains
 * empty and the flusher skips registration entirely.
 */
public final class PartitionSlot {

    private final ReentrantLock stateLock = new ReentrantLock();
    private final AtomicReference<Object> state = new AtomicReference<>();
    private final PartitionMailbox<Object> mailbox = new PartitionMailbox<>();
    private final ShardDispatcher dispatcher;
    private final int shardId;
    private final Optional<String> shardKey;
    private final boolean changeful;
    private final Object changeBufferLock = new Object();
    private final List<Change> changeBuffer = new ArrayList<>();
    private final AtomicLong changeSeq = new AtomicLong(0L);

    public PartitionSlot(@Nonnull ShardDispatcher dispatcher, int shardId, @Nonnull Optional<String> shardKey) {
        this(dispatcher, shardId, shardKey, false);
    }

    public PartitionSlot(@Nonnull ShardDispatcher dispatcher, int shardId,
                         @Nonnull Optional<String> shardKey, boolean changeful) {
        this.dispatcher = dispatcher;
        this.shardId = shardId;
        this.shardKey = shardKey;
        this.changeful = changeful;
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
    public PartitionMailbox<Object> mailbox() {
        return mailbox;
    }

    @Nonnull
    public ShardDispatcher dispatcher() {
        return dispatcher;
    }

    public int shardId() {
        return shardId;
    }

    @Nonnull
    public Optional<String> shardKey() {
        return shardKey;
    }

    /** Whether this Shard declares a non-{@code NoChange} Change type. */
    public boolean changeful() {
        return changeful;
    }

    /** Lock the flusher acquires before draining the buffer. */
    @Nonnull
    public Object changeBufferLock() {
        return changeBufferLock;
    }

    /** Mutable per-Partition Change buffer; the flusher drains and serializes. */
    @Nonnull
    public List<Change> changeBuffer() {
        return changeBuffer;
    }

    /** Monotonic per-Partition sequence number applied to flushed Change frames. */
    @Nonnull
    public AtomicLong changeSeq() {
        return changeSeq;
    }
}
