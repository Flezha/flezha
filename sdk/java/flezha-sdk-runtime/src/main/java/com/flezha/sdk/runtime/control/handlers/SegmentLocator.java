package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.runtime.RunnerContext;
import com.flezha.sdk.runtime.operative.UnboundedOperativeInstance;
import com.flezha.sdk.runtime.segment.SegmentSlot;
import com.flezha.sdk.runtime.state.SegmentId;
import java.util.Arrays;
import java.util.List;

/**
 * Locates a live unbounded Segment by its wire {@code segment_id}. Shared by the
 * control handlers that address one Segment ({@link RestoreInitStateHandler},
 * {@link CompactSegmentHandler}): the identity is built the same way the flusher
 * frames {@code segment_id}, so a byte-equal match is exact.
 */
final class SegmentLocator {

    private SegmentLocator() {}

    /** A located unbounded Segment: its owning instance, slot, and wire identity. */
    record Located(UnboundedOperativeInstance instance, SegmentSlot slot, SegmentId segmentId) {}

    /**
     * Scan every live unbounded operative's segments for the slot whose binary
     * identity equals {@code segmentId}; {@code null} if none matches.
     */
    static Located findUnbounded(RunnerContext ctx, byte[] segmentId) {
        for (UnboundedOperativeInstance instance :
                ctx.operativeInstanceRegistry().operativeSnapshot().values()) {
            List<List<SegmentSlot>> segments = instance.segments();
            for (List<SegmentSlot> operationSegments : segments) {
                for (SegmentSlot slot : operationSegments) {
                    String operationName = instance.topology()
                            .operations()
                            .get(slot.operationId())
                            .name();
                    SegmentId identity = SegmentId.of(instance.instanceId(), operationName, slot.operationKey());
                    if (Arrays.equals(identity.encodedBytes(), segmentId)) {
                        return new Located(instance, slot, identity);
                    }
                }
            }
        }
        return null;
    }
}
