package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.proto.CompactSegmentResponse;
import com.flezha.sdk.runtime.RunnerContext;
import com.flezha.sdk.runtime.control.CommandHandler;
import com.flezha.sdk.runtime.control.handlers.SegmentLocator.Located;
import com.flezha.sdk.runtime.segment.SegmentSlot;
import com.flezha.sdk.runtime.state.HeaderTemplate;
import com.flezha.sdk.runtime.state.KryoCodec;
import com.flezha.sdk.runtime.state.SegmentFlusher;
import com.flezha.sdk.runtime.state.SegmentId;
import com.flezha.sdk.runtime.state.SegmentStateSink;
import com.google.protobuf.ByteString;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * Handles the controller's {@code CompactSegment} command: produce a durable
 * snapshot so the controller can truncate the Segment's WAL behind it.
 *
 * <p>Locates the live {@link SegmentSlot} whose wire identity equals
 * {@code segment_id}, then under the slot's {@code stateLock} drains its pending
 * Changes (so the serialized state equals the WAL the controller holds),
 * serializes the state with <strong>Kryo</strong> — the codec
 * {@code SegmentStateRestore.rebuild} decodes, NOT the Jackson migration
 * serializer — stamps it with the Segment's schema header, and ships it as a
 * {@code segment_state_snapshot} data frame stamped with the highest seq the
 * snapshot already contains (the controller keeps only WAL records strictly
 * above it). It answers {@code CompactSegmentResponse{segment_id, snapshot_seq,
 * snapshot_bytes_size}}. A {@code segment_id} matching no live Segment is
 * reported as an error rather than acked as a completed compaction.
 */
public final class CompactSegmentHandler implements CommandHandler {

    @Override
    @Nonnull
    public byte[] handle(@Nonnull RunnerContext ctx, @Nonnull CommandRequest request, @Nonnull List<String> errors) {
        if (!request.hasCompactSegment()) {
            errors.add("CompactSegmentHandler: payload missing");
            return new byte[0];
        }
        byte[] segmentId = request.getCompactSegment().getSegmentId().toByteArray();

        long snapshotSeq = 0L;
        long snapshotBytesSize = 0L;

        Located located = SegmentLocator.findUnbounded(ctx, segmentId);
        if (located == null) {
            errors.add("compact_segment: no live segment matches the addressed segment_id");
        } else {
            long[] result = snapshot(located);
            snapshotSeq = result[0];
            snapshotBytesSize = result[1];
        }

        return CompactSegmentResponse.newBuilder()
                .setSegmentId(ByteString.copyFrom(segmentId))
                .setSnapshotSeq(snapshotSeq)
                .setSnapshotBytesSize(snapshotBytesSize)
                .build()
                .toByteArray();
    }

    /**
     * Capture the located slot's state as a snapshot under its {@code stateLock}
     * and ship it. Returns {@code [snapshotSeq, snapshotBytesSize]}.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static long[] snapshot(Located located) {
        SegmentSlot slot = located.slot();
        SegmentId segmentId = located.segmentId();
        // Same codec + header the restore path rebuilds with (see
        // RestoreInitStateHandler.restoreSlot / registerWithFlusher).
        KryoCodec codec = new KryoCodec(located.instance().kryoTypes());
        HeaderTemplate headerTemplate = new HeaderTemplate();
        int schemaVersion = slot.operationInstance().schemaVersion();

        slot.stateLock().lock();
        try {
            // Drain pending Changes FIRST so the state and its seq are captured
            // together: holding stateLock keeps any run from refilling the
            // buffer, so after this changeSeq is settled and the in-memory state
            // equals the shipped WAL. Stamp the snapshot with changeSeq - 1 (the
            // highest seq it folds), which the controller's truncate_up_to keeps
            // records strictly above.
            SegmentFlusher.getOrStart().flushSegment(segmentId);
            long changeSeq = slot.changeSeq().get();
            long snapshotSeq = Math.max(0L, changeSeq - 1L);

            byte[] body = codec.serialize(slot.state().get());
            byte[] header = headerTemplate.stamp(schemaVersion, body);
            byte[] frame = SegmentFlusher.buildSegmentStateChangeFrame(segmentId, snapshotSeq, header, body);
            // Ship the snapshot AFTER the drained change frames (both ride the
            // ordered reliable sink), so the controller sees every record the
            // snapshot folds before the snapshot that truncates them.
            SegmentStateSink.send(SegmentStateSink.SEGMENT_STATE_SNAPSHOT_PAYLOAD_ID, frame);
            return new long[] {snapshotSeq, body.length};
        } finally {
            slot.stateLock().unlock();
        }
    }
}
