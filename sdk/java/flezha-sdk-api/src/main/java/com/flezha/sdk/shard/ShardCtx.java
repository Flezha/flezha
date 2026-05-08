package com.flezha.sdk.shard;

import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Per-invocation context for a {@link Shard#run} call. Borrows the runner's
 * per-Partition routing tables; not safe to retain past the call.
 *
 * <p>The dispatcher caches the producer-edge {@code outputChannelId} on the
 * {@link ShardCtx} instance at relay-build time so {@link #emit} pays no
 * channel-name lookup on the hot path.
 */
public interface ShardCtx {

    /**
     * Emit a typed value on this Shard's declared output channel. The channel
     * id was cached at {@code registerRelay} time from
     * {@code c.channel(T.class, "name").from(S.class)} wiring, so this call
     * performs no name lookups.
     *
     * <p>Inter-shard channels: the value is passed by reference to consumer
     * Shards; the user MUST treat it as immutable from the producer side
     * after this call returns (see {@code design.md} Decision 4).
     *
     * <p>Plugin-bound channels: the value is serialized synchronously via the
     * channel's serializer and queued for the wire with
     * {@code metadata = {"channel": "<name>"}}.
     */
    <C> void emit(@Nonnull C value) throws ShardException;

    /**
     * Same as {@link #emit(Object)} but pinned to a specific channel name —
     * for Shards that produce on multiple plugin-bound channels.
     */
    <C> void emit(@Nonnull String channel, @Nonnull C value) throws ShardException;

    /**
     * Emit with user-attached string-keyed metadata. Valid only on plugin-bound
     * channels; inter-shard emits throw
     * {@link ShardException.MetadataNotAllowedOnInterShardChannel} naming the
     * channel.
     *
     * <p>The reserved {@code channel} key is silently dropped from the user
     * map and overwritten with the channel's actual name during framing.
     */
    <C> void emitWithMeta(@Nonnull C value, @Nonnull Map<String, String> meta) throws ShardException;

    /** Same as {@link #emitWithMeta(Object, Map)} but pinned to a specific channel name. */
    <C> void emitWithMeta(@Nonnull String channel, @Nonnull C value, @Nonnull Map<String, String> meta)
            throws ShardException;
}
