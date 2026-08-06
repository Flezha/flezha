package com.flezha.sdk.runtime.operative;

import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nonnull;

/**
 * One consumer subscription on a channel. A plain {@code .into(Proc.class)}
 * carries no mapping — the channel's payload IS the consumer's input; a
 * {@code merge(I.class).from(stream, map)} subscription carries the mapping
 * that lifts this channel's payload into the merged input
 * ({@code multi-input-procedures}).
 *
 * <p>The mapping runs on live delivery and again on WAL replay, so it MUST be
 * pure — the same contract entry parsers and output serializers carry.
 *
 * <p>Mirror of the Rust SDK's {@code channel::ConsumerEdge}.
 */
public record ConsumerEdge(int procedure, @Nonnull Optional<Function<Object, Object>> map) {

    /** Apply this edge's mapping, or pass the value through unchanged. */
    @Nonnull
    public Object mapValue(@Nonnull Object value) {
        return map.isPresent() ? map.get().apply(value) : value;
    }
}
