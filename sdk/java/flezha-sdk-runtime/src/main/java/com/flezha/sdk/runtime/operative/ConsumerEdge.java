package com.flezha.sdk.runtime.operative;

import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * One consumer subscription on a channel. A plain {@code .into(Proc.class)}
 * carries neither closure — the channel's payload IS the consumer's input, and
 * the consumer's Entity mode is its own; a chain of transforms, a
 * {@code .keyBy(f)}, or a {@code merge(I.class).from(stream, map)} subscription
 * stores them here, each composed into one closure.
 *
 * <p>The closures run on live delivery and again on WAL replay, so they MUST be
 * pure — the same contract entry parsers and output serializers carry.
 *
 * <p>Mirror of the Rust SDK's {@code channel::ConsumerEdge}.
 */
public record ConsumerEdge(
        int procedure,
        @Nonnull Optional<Function<Object, EdgeYield>> transform,
        @Nonnull Optional<Function<Object, String>> key) {

    /**
     * Apply this edge's transform, or {@code null} when the edge declares none
     * — a transform-free edge, which is every pre-existing topology, allocates
     * nothing on the routing path. A throwing closure surfaces as the caller's
     * park-ladder failure rather than killing the routing thread.
     */
    @Nullable
    public EdgeYield transformValue(@Nonnull Object value) {
        return transform.isPresent() ? transform.get().apply(value) : null;
    }

    /**
     * Evaluate this edge's key function on the post-transform value, or
     * {@code null} when the edge declares none.
     */
    @Nullable
    public String keyOf(@Nonnull Object value) {
        return key.isPresent() ? key.get().apply(value) : null;
    }
}
