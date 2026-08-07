package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.proto.CompactEntityResponse;
import com.flezha.sdk.runtime.RunnerContext;
import com.flezha.sdk.runtime.control.CommandHandler;
import com.flezha.sdk.runtime.control.handlers.EntityLocator.Located;
import com.flezha.sdk.runtime.entity.EntitySlot;
import com.flezha.sdk.runtime.state.EntityFlusher;
import com.flezha.sdk.runtime.state.EntityId;
import com.flezha.sdk.runtime.state.EntityStateSink;
import com.flezha.sdk.runtime.state.HeaderTemplate;
import com.flezha.sdk.runtime.state.KryoCodec;
import com.google.protobuf.ByteString;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * Handles the controller's {@code CompactEntity} command: produce a durable
 * snapshot so the controller can truncate the Entity's WAL behind it.
 *
 * <p>Locates the live {@link EntitySlot} whose wire identity equals
 * {@code entity_id}, then under the slot's {@code stateLock} drains its pending
 * Changes (so the serialized state equals the WAL the controller holds),
 * serializes the state with <strong>Kryo</strong> — the codec
 * {@code EntityStateRestore.rebuild} decodes, NOT the Jackson migration
 * serializer — stamps it with the Entity's schema header, and ships it as a
 * {@code entity_state_snapshot} data frame stamped with the highest seq the
 * snapshot already contains (the controller keeps only WAL records strictly
 * above it). It answers {@code CompactEntityResponse{entity_id, snapshot_seq,
 * snapshot_bytes_size}}. A {@code entity_id} matching no live Entity is
 * reported as an error rather than acked as a completed compaction.
 */
public final class CompactEntityHandler implements CommandHandler {

    @Override
    @Nonnull
    public byte[] handle(@Nonnull RunnerContext ctx, @Nonnull CommandRequest request, @Nonnull List<String> errors) {
        if (!request.hasCompactEntity()) {
            errors.add("CompactEntityHandler: payload missing");
            return new byte[0];
        }
        byte[] entityId = request.getCompactEntity().getEntityId().toByteArray();

        long snapshotSeq = 0L;
        long snapshotBytesSize = 0L;

        Located located = EntityLocator.findUnbounded(ctx, entityId);
        if (located == null) {
            errors.add("compact_entity: no live entity matches the addressed entity_id");
        } else {
            long[] result = snapshot(located);
            snapshotSeq = result[0];
            snapshotBytesSize = result[1];
        }

        return CompactEntityResponse.newBuilder()
                .setEntityId(ByteString.copyFrom(entityId))
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
        EntitySlot slot = located.slot();
        EntityId entityId = located.entityId();
        // Same codec + header the restore path rebuilds with (see
        // RestoreInitStateHandler.restoreSlot / registerWithFlusher).
        KryoCodec codec = new KryoCodec(located.instance().kryoTypes());
        HeaderTemplate headerTemplate = new HeaderTemplate();
        int schemaVersion = slot.procedureInstance().schemaVersion();

        slot.stateLock().lock();
        try {
            // Drain pending Changes FIRST so the state and its seq are captured
            // together: holding stateLock keeps any run from refilling the
            // buffer, so after this changeSeq is settled and the in-memory state
            // equals the shipped WAL. Stamp the snapshot with changeSeq - 1 (the
            // highest seq it folds), which the controller's truncate_up_to keeps
            // records strictly above.
            EntityFlusher.getOrStart().flushEntity(entityId);
            long changeSeq = slot.changeSeq().get();
            long snapshotSeq = Math.max(0L, changeSeq - 1L);

            // The snapshot body is the State alone, so the timer ops the coming
            // truncation would delete are re-stated as a full set AFTER the seq
            // is read — landing at snapshotSeq + 1, which truncate_up_to keeps.
            // A crash between the two leaves an untruncated log that still holds
            // the originals, so neither order can lose a timer.
            if (!slot.timers().isEmpty() && slot.flusherEntry() != null) {
                synchronized (slot.changeBufferLock()) {
                    slot.timerOps().addAll(slot.timers().asRegisterOps());
                }
                EntityFlusher.flushEntry(slot.flusherEntry());
            }

            byte[] body = codec.serialize(slot.state().get());
            byte[] header = headerTemplate.stamp(schemaVersion, body);
            byte[] frame = EntityFlusher.buildEntityStateChangeFrame(entityId, snapshotSeq, header, body);
            // Ship the snapshot AFTER the drained change frames (both ride the
            // ordered reliable sink), so the controller sees every record the
            // snapshot folds before the snapshot that truncates them.
            EntityStateSink.send(EntityStateSink.ENTITY_STATE_SNAPSHOT_PAYLOAD_ID, frame);
            return new long[] {snapshotSeq, body.length};
        } finally {
            slot.stateLock().unlock();
        }
    }
}
