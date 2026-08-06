package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.proto.RestoreInitState;
import com.flezha.sdk.proto.RestoreInitStateResponse;
import com.flezha.sdk.runtime.RunnerContext;
import com.flezha.sdk.runtime.bounded.BoundedOperativeRunner.BoundedEntity;
import com.flezha.sdk.runtime.control.CommandHandler;
import com.flezha.sdk.runtime.control.handlers.EntityLocator.Located;
import com.flezha.sdk.runtime.entity.EntitySlot;
import com.flezha.sdk.runtime.operative.BoundedOperativeInstance;
import com.flezha.sdk.runtime.operative.Operative;
import com.flezha.sdk.runtime.operative.UnboundedOperativeInstance;
import com.flezha.sdk.runtime.state.ChangeLogReader;
import com.flezha.sdk.runtime.state.EntityStateRestore;
import com.flezha.sdk.runtime.state.HeaderTemplate;
import com.flezha.sdk.runtime.state.KryoCodec;
import com.google.protobuf.ByteString;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the controller's {@code RestoreInitState} command: a warm restart of
 * an Entity after a runner restart / rebind.
 *
 * <p>The controller ships the persisted snapshot + WAL change-log for one
 * Entity. This handler locates the live {@link EntitySlot} whose wire
 * identity equals {@code entity_id}, rebuilds its state in place
 * (Kryo-decode the snapshot if present, then fold the change-log via
 * {@code Procedure.applyChange}), and re-arms the slot's {@code changeSeq} to
 * {@code terminal_seq + 1} so the next produced frame is contiguous with the
 * persisted history (the controller then skips a WAL realign). It replies with
 * {@code RestoreInitStateResponse{entity_id, terminal_seq}}.
 *
 * <p>When the unbounded scan misses, it scans live <strong>bounded</strong>
 * instances (whose Entities live on the {@link BoundedOperativeInstance}, not a
 * {@link UnboundedOperativeInstance}): a durable bounded operative that has been
 * re-added rebuilds its committed Entity state in place and releases one count
 * on the driver's crash-resume gate. Only when neither matches does it fall back
 * to validating the change-log and acking the terminal seq. There is no
 * SDK-side HotSwap guard: the controller never dispatches {@code RestoreInitState}
 * to a operative that arrived via HotSwap handover (its state was delivered in-band
 * by {@code MigrationDeserialization}), mirroring the Rust SDK.
 */
public final class RestoreInitStateHandler implements CommandHandler {

    private static final Logger LOG = LoggerFactory.getLogger(RestoreInitStateHandler.class);

    @Override
    @Nonnull
    public byte[] handle(@Nonnull RunnerContext ctx, @Nonnull CommandRequest request, @Nonnull List<String> errors) {
        if (!request.hasRestoreInitState()) {
            errors.add("RestoreInitStateHandler: payload missing");
            return new byte[0];
        }
        RestoreInitState req = request.getRestoreInitState();
        byte[] entityId = req.getEntityId().toByteArray();
        byte[] snapshot = req.getSnapshotBytes().toByteArray();
        byte[] changeLog = req.getChangeLogBytes().toByteArray();

        HeaderTemplate headerTemplate = new HeaderTemplate();
        long terminalSeq = 0L;

        Located located = EntityLocator.findUnbounded(ctx, entityId);
        if (located != null) {
            try {
                terminalSeq = restoreSlot(located, snapshot, changeLog, headerTemplate);
            } catch (RuntimeException e) {
                errors.add("restore_init_state: apply failed: " + e.getMessage());
            }
        } else {
            BoundedLocated bounded = findBoundedEntity(ctx, entityId);
            if (bounded != null) {
                // Durable bounded Entity: rebuild its committed state in place
                // and release one count on the driver's crash-resume gate. The
                // frame landed regardless of apply success, so noteRestored() is
                // unconditional (the gate never stalls on a corrupt record).
                try {
                    terminalSeq = bounded.entity().restore(snapshot, changeLog, headerTemplate);
                } catch (RuntimeException e) {
                    errors.add("restore_init_state: apply failed: " + e.getMessage());
                } finally {
                    // Saga crash-resume (§4.3): carry the per-Entity compensation
                    // markers so the driver finishes an in-flight undo (intent) and
                    // skips already-undone entities (done).
                    bounded.instance()
                            .noteCompensationMarkers(
                                    bounded.entity().identity().encoded(),
                                    req.getHasCompensationIntent(),
                                    req.getHasCompensationDone());
                    bounded.instance().noteRestored();
                }
            } else {
                // No live slot — validate the change-log and ack the terminal seq
                // (the controller still advances its handshake).
                try {
                    List<ChangeLogReader.Record> records = ChangeLogReader.parse(changeLog, headerTemplate);
                    terminalSeq = ChangeLogReader.terminalSeq(records);
                } catch (RuntimeException e) {
                    errors.add("change_log decode error: " + e.getMessage());
                    return new byte[0];
                }
            }
        }

        return RestoreInitStateResponse.newBuilder()
                .setEntityId(ByteString.copyFrom(entityId))
                .setTerminalSeq(terminalSeq)
                .build()
                .toByteArray();
    }

    /**
     * Rebuild a live unbounded slot's state from snapshot + change-log and
     * re-arm its change-seq. Returns the terminal (highest) WAL seq, 0 when the
     * change-log is empty.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static long restoreSlot(Located located, byte[] snapshot, byte[] changeLog, HeaderTemplate headerTemplate) {
        EntitySlot slot = located.slot();
        Procedure procedure = slot.procedureInstance();
        KryoCodec codec = new KryoCodec(located.instance().kryoTypes());

        slot.stateLock().lock();
        try {
            EntityStateRestore.Rebuilt rebuilt =
                    EntityStateRestore.rebuild(procedure, codec, slot.entityKey(), snapshot, changeLog, headerTemplate);
            slot.state().set(rebuilt.state());
            slot.changeSeq().set(rebuilt.terminalSeq() + 1);
            // A restore re-seats the Entity on a different past. An open
            // checkpoint interval's changes were never acknowledged, so the
            // restored state does not contain them and the re-forwarded inputs
            // produce them again; a half-filled correction slice belongs to a
            // slice that was abandoned. Both go, on every class.
            slot.discardStagedInterval();
            slot.discardReplaySlice();
            // The mailbox goes only where the controller re-forwards it; the
            // slot owns that rule.
            int discarded = slot.discardQueuedInputOnRestore();
            if (discarded > 0) {
                LOG.info(
                        "restore discarded {} queued input(s) for entity {} — the controller"
                                + " re-forwards them from the durable input log",
                        discarded,
                        located.entityId().encoded());
            }
            return rebuilt.terminalSeq();
        } finally {
            slot.stateLock().unlock();
        }
    }

    /**
     * Scan live bounded instances for the Entity whose wire identity equals
     * {@code entityId}. The unbounded {@link #findSlot} scan does not see
     * bounded Entities (they live on the {@link BoundedOperativeInstance}, in the
     * bounded package), so this closes the crash-resume reachability gap.
     */
    private static BoundedLocated findBoundedEntity(RunnerContext ctx, byte[] entityId) {
        for (Operative operative : ctx.operativeInstanceRegistry().snapshot().values()) {
            if (operative instanceof BoundedOperativeInstance bounded) {
                for (List<BoundedEntity> stage : bounded.stageEntities()) {
                    for (BoundedEntity seg : stage) {
                        if (Arrays.equals(seg.identity().encodedBytes(), entityId)) {
                            return new BoundedLocated(bounded, seg);
                        }
                    }
                }
            }
        }
        return null;
    }

    private record BoundedLocated(BoundedOperativeInstance instance, BoundedEntity entity) {}
}
