package com.flezha.sdk.runtime.state;

import com.flezha.sdk.state.Change;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import javax.annotation.Nonnull;

/**
 * Per-Segment registration record carried by {@link SegmentFlusher}. The
 * flusher acquires {@code bufferLock}, drains {@code buffer}, applies coalescing,
 * builds frames via the {@code headerTemplate}, and (when
 * {@code persistenceEnabled}) sends them via {@code sendFrame}.
 *
 * <p>{@code persistenceEnabled} is the resolved toggle (executor override
 * &gt; ContinuousOperative default &gt; {@code true}; frozen at {@code ContinuousOperativeInstance}
 * construction). When {@code false}, the drain still runs (releasing the
 * buffer's memory) but no frame is built or sent.
 */
public record FlusherEntry(
        @Nonnull Object bufferLock,
        @Nonnull List<Change> buffer,
        boolean persistenceEnabled,
        @Nonnull AtomicLong changeSeq,
        @Nonnull Function<byte[], Boolean> sendFrame,
        @Nonnull HeaderTemplate headerTemplate,
        @Nonnull SegmentId segmentId,
        int schemaVersion,
        @Nonnull KryoCodec codec) {}
