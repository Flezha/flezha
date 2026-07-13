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
     * Reserved metadata namespace: the runtime's identity stamps
     * ({@code flz.segment}, {@code flz.seq}, {@code flz.key}) live here.
     * User-supplied keys with this prefix are rejected at emit time so a stamp
     * can never be spoofed or clobbered.
     */
    String RESERVED_METADATA_PREFIX = "flz.";

    /**
     * The invocation's time value, computed <b>once</b> by the runtime from the
     * Operation's declared {@code Clock} before {@code run} executes: epoch
     * milliseconds for {@code WallClock}, the extracted input timestamp for
     * {@code EventTimeFrom}, the durable change seq for {@code LogicalClock}.
     * Every call within one invocation returns the same value.
     *
     * <p>This is the determinism-contract replacement for
     * {@code System.currentTimeMillis()}: a run that reads ambient time cannot
     * be replayed, because the replay would compute different outputs from the
     * same inputs.
     */
    long now();

    /**
     * A deterministic, replay-stable 128-bit identifier. Repeated calls within
     * one invocation differ; the n-th call of a replayed invocation equals the
     * n-th call of the original.
     *
     * <p>This is the determinism-contract replacement for
     * {@code UUID.randomUUID()}.
     */
    @Nonnull
    StableId stableId();

    /**
     * Close the current checkpoint interval: acknowledge every input consumed
     * since the last checkpoint, and make the state accumulated across them
     * durable — as one record.
     *
     * <p>Call it where the accumulated state is genuinely coherent (a closed
     * window, a completed batch). The interval you leave open is the work a
     * crash replays and the work the controller's input log retains.
     *
     * <p>A no-op unless the Operative declared {@code checkpointAck()}.
     */
    void checkpoint();

    /**
     * Emit on a channel declared {@code .keyed()}, supplying the output's
     * natural key. The runtime stamps {@code flz.key} (alongside
     * {@code flz.segment} and {@code flz.seq}) on the plugin frame, which is
     * what lets a keyed sink absorb a replayed emit as an idempotent upsert
     * rather than a duplicate row.
     *
     * <p>The key must itself be replay-stable by the determinism contract —
     * derived from the input, the state, or {@code ctx}, never from an ambient
     * source. Emitting a key on a channel not declared {@code .keyed()} throws
     * {@link OperationException.KeyNotAllowedOnNonKeyedChannel}.
     */
    <C> void emitKeyed(@Nonnull String key, @Nonnull C value) throws OperationException;

    /** {@link #emitKeyed} with user-attached metadata. */
    <C> void emitKeyedWithMeta(@Nonnull String key, @Nonnull C value, @Nonnull Map<String, String> meta)
            throws OperationException;

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
     * <p>The reserved {@code channel} key is dropped from the user map and
     * overwritten with the channel's actual name during framing. Keys in the
     * {@link #RESERVED_METADATA_PREFIX} namespace are rejected with
     * {@link OperationException.ReservedMetadataKey}.
     */
    <C> void emitWithMeta(@Nonnull C value, @Nonnull Map<String, String> meta) throws OperationException;

    /** Same as {@link #emitWithMeta(Object, Map)} but pinned to a specific channel name. */
    <C> void emitWithMeta(@Nonnull String channel, @Nonnull C value, @Nonnull Map<String, String> meta)
            throws OperationException;
}
