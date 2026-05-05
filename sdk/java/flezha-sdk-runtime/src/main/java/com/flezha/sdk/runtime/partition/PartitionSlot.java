package com.flezha.sdk.runtime.partition;

import com.flezha.sdk.runtime.worker.ShardDispatcher;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nonnull;

/**
 * Per-(shard, partition) state + mailbox + dispatcher. Replaces the legacy
 * {@code ProcessorInstance}; carries forward the synchronized-per-instance
 * invariant via the inflight CAS pattern on {@link PartitionMailbox} (so a
 * single slot is drained by at most one thread at a time).
 *
 * <p>The {@code state} field is type-erased ({@code AtomicReference<Object>})
 * because the topology builder wires Shards by reflective {@link Class}; the
 * dispatcher knows the concrete Shard / State / Input / Output types and
 * casts inside the drain loop.
 */
public final class PartitionSlot {

    private final ReentrantLock stateLock = new ReentrantLock();
    private final AtomicReference<Object> state = new AtomicReference<>();
    private final PartitionMailbox<Object> mailbox = new PartitionMailbox<>();
    private final ShardDispatcher dispatcher;
    private final int shardId;
    private final Optional<String> shardKey;

    public PartitionSlot(@Nonnull ShardDispatcher dispatcher, int shardId, @Nonnull Optional<String> shardKey) {
        this.dispatcher = dispatcher;
        this.shardId = shardId;
        this.shardKey = shardKey;
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
}
