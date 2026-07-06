package com.flezha.sdk.operation;

import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Per-invocation context for a {@link Operation#run} call. Borrows the runner's
 * per-Segment routing tables; not safe to retain past the call.
 *
 * <p>The dispatcher caches the producer-edge {@code outputChannelId} on the
 * {@link OperationCtx} instance at operative-build time so {@link #emit} pays no
 * channel-name lookup on the hot path.
 */
public interface OperationCtx {

    /**
     * Emit a typed value on this Operation's declared output channel. The channel
     * id was cached at {@code registerUnbounded} time from
     * {@code c.channel(T.class, "name").from(S.class)} wiring, so this call
     * performs no name lookups.
     *
     * <p>Inter-operation channels: the value is passed by reference to consumer
     * Operations; the user MUST treat it as immutable from the producer side
     * after this call returns (see {@code design.md} Decision 4).
     *
     * <p>Plugin-bound channels: the value is serialized synchronously via the
     * channel's serializer and queued for the wire with
     * {@code metadata = {"channel": "<name>"}}.
     */
    <C> void emit(@Nonnull C value) throws OperationException;

    /**
     * Same as {@link #emit(Object)} but pinned to a specific channel name —
     * for Operations that produce on multiple plugin-bound channels.
     */
    <C> void emit(@Nonnull String channel, @Nonnull C value) throws OperationException;

    /**
     * Emit with user-attached string-keyed metadata. Valid only on plugin-bound
     * channels; inter-operation emits throw
     * {@link OperationException.MetadataNotAllowedOnInterOperationChannel} naming the
     * channel.
     *
     * <p>The reserved {@code channel} key is silently dropped from the user
     * map and overwritten with the channel's actual name during framing.
     */
    <C> void emitWithMeta(@Nonnull C value, @Nonnull Map<String, String> meta) throws OperationException;

    /** Same as {@link #emitWithMeta(Object, Map)} but pinned to a specific channel name. */
    <C> void emitWithMeta(@Nonnull String channel, @Nonnull C value, @Nonnull Map<String, String> meta)
            throws OperationException;
}
