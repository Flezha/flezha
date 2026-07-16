package com.flezha.sdk.runtime.state;

import com.flezha.sdk.state.Change;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import javax.annotation.Nonnull;

/**
 * Per-Segment registration record carried by {@link SegmentFlusher}. The
 * flusher acquires {@code bufferLock}, drains a buffer, builds frames via the
 * {@code headerTemplate}, and (when {@code persistenceEnabled}) sends them via
 * {@code sendFrame}.
 *
 * <p>A Segment drains ONE of two buffers, never both:
 *
 * <ul>
 *   <li>{@code buffer} — plain {@link Change} records, coalesced at drain and
 *       shipped as {@code segment_state_change}. The default.
 *   <li>{@code effects} — effectively-once {@link EffectRecord}s, each carrying
 *       one invocation's changes, outputs and consumed offset, shipped as
 *       {@code segment_effect_record}. <b>Never coalesced</b>: a coalesced-away
 *       effect record would take its outputs and its consumed offset with it.
 * </ul>
 *
 * <p>{@code sendFrame} takes the payload-id because those two paths ship under
 * different ones.
 *
 * <p>{@code persistenceEnabled} is the resolved toggle (runner override
 * &gt; UnboundedOperative default &gt; {@code true}; frozen at {@code UnboundedOperativeInstance}
 * construction). When {@code false}, the drain still runs (releasing the
 * buffer's memory) but no frame is built or sent.
 */
public record FlusherEntry(
        @Nonnull Object bufferLock,
        @Nonnull List<Change> buffer,
        @Nonnull List<EffectRecord> effects,
        boolean persistenceEnabled,
        @Nonnull AtomicLong changeSeq,
        @Nonnull BiFunction<String, byte[], Boolean> sendFrame,
        @Nonnull HeaderTemplate headerTemplate,
        @Nonnull SegmentId segmentId,
        int schemaVersion,
        @Nonnull KryoCodec codec) {}
