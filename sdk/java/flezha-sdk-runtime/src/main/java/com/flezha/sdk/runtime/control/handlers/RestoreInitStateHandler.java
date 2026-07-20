package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.operation.Operation;
import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.proto.RestoreInitState;
import com.flezha.sdk.proto.RestoreInitStateResponse;
import com.flezha.sdk.runtime.RunnerContext;
import com.flezha.sdk.runtime.bounded.BoundedOperativeRunner.BoundedSegment;
import com.flezha.sdk.runtime.control.CommandHandler;
import com.flezha.sdk.runtime.operative.BoundedOperativeInstance;
import com.flezha.sdk.runtime.operative.Operative;
import com.flezha.sdk.runtime.operative.UnboundedOperativeInstance;
import com.flezha.sdk.runtime.segment.SegmentSlot;
import com.flezha.sdk.runtime.state.ChangeLogReader;
import com.flezha.sdk.runtime.state.HeaderTemplate;
import com.flezha.sdk.runtime.state.KryoCodec;
import com.flezha.sdk.runtime.state.SegmentId;
import com.flezha.sdk.runtime.state.SegmentStateRestore;
import com.google.protobuf.ByteString;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the controller's {@code RestoreInitState} command: a warm restart of
 * a Segment after a runner restart / rebind.
 *
 * <p>The controller ships the persisted snapshot + WAL change-log for one
 * Segment. This handler locates the live {@link SegmentSlot} whose wire
 * identity equals {@code segment_id}, rebuilds its state in place
 * (Kryo-decode the snapshot if present, then fold the change-log via
 * {@code Operation.applyChange}), and re-arms the slot's {@code changeSeq} to
 * {@code terminal_seq + 1} so the next produced frame is contiguous with the
 * persisted history (the controller then skips a WAL realign). It replies with
 * {@code RestoreInitStateResponse{segment_id, terminal_seq}}.
 *
 * <p>When the unbounded scan misses, it scans live <strong>bounded</strong>
 * instances (whose Segments live on the {@link BoundedOperativeInstance}, not a
 * {@link UnboundedOperativeInstance}): a durable bounded operative that has been
 * re-added rebuilds its committed Segment state in place and releases one count
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
        // The op-journal rides the restore for a durable structured bounded
        // class. Stash it on the instance BEFORE releasing the resume gate: the
        // driver reads it to skip already-committed operations, and a driver that
        // started without it would re-run the chain from the top.
        if (req.hasOpJournal()) {
            BoundedLocated owner = findBoundedSegment(ctx, req.getSegmentId().toByteArray());
            if (owner != null) {
                owner.instance().setOpJournal(req.getOpJournal());
            }
        }
        byte[] segmentId = req.getSegmentId().toByteArray();
        byte[] snapshot = req.getSnapshotBytes().toByteArray();
        byte[] changeLog = req.getChangeLogBytes().toByteArray();

        HeaderTemplate headerTemplate = new HeaderTemplate();
        long terminalSeq = 0L;

        Located located = findSlot(ctx, segmentId);
        if (located != null) {
            try {
                terminalSeq = restoreSlot(located, snapshot, changeLog, headerTemplate);
            } catch (RuntimeException e) {
                errors.add("restore_init_state: apply failed: " + e.getMessage());
            }
        } else {
            BoundedLocated bounded = findBoundedSegment(ctx, segmentId);
            if (bounded != null) {
                // Durable bounded Segment: rebuild its committed state in place
                // and release one count on the driver's crash-resume gate. The
                // frame landed regardless of apply success, so noteRestored() is
                // unconditional (the gate never stalls on a corrupt record).
                try {
                    terminalSeq = bounded.segment().restore(snapshot, changeLog, headerTemplate);
                } catch (RuntimeException e) {
                    errors.add("restore_init_state: apply failed: " + e.getMessage());
                } finally {
                    // Saga crash-resume (§4.3): carry the per-Segment compensation
                    // markers so the driver finishes an in-flight undo (intent) and
                    // skips already-undone segments (done).
                    bounded.instance()
                            .noteCompensationMarkers(
                                    bounded.segment().identity().encoded(),
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
                .setSegmentId(ByteString.copyFrom(segmentId))
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
        SegmentSlot slot = located.slot();
        Operation operation = slot.operationInstance();
        KryoCodec codec = new KryoCodec(located.instance().kryoTypes());

        slot.stateLock().lock();
        try {
            SegmentStateRestore.Rebuilt rebuilt = SegmentStateRestore.rebuild(
                    operation, codec, slot.operationKey(), snapshot, changeLog, headerTemplate);
            slot.state().set(rebuilt.state());
            slot.changeSeq().set(rebuilt.terminalSeq() + 1);
            // A restore re-seats the Segment on a different past. An open
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
                        "restore discarded {} queued input(s) for segment {} — the controller"
                                + " re-forwards them from the durable input log",
                        discarded,
                        located.segmentId().encoded());
            }
            return rebuilt.terminalSeq();
        } finally {
            slot.stateLock().unlock();
        }
    }

    /**
     * Scan every live unbounded operative's segments for the slot whose binary
     * identity equals {@code segmentId}. The identity is built the same way the
     * flusher frames {@code segment_id}, so a byte-equal match is exact.
     */
    private static Located findSlot(RunnerContext ctx, byte[] segmentId) {
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

    /**
     * Scan live bounded instances for the Segment whose wire identity equals
     * {@code segmentId}. The unbounded {@link #findSlot} scan does not see
     * bounded Segments (they live on the {@link BoundedOperativeInstance}, in the
     * bounded package), so this closes the crash-resume reachability gap.
     */
    private static BoundedLocated findBoundedSegment(RunnerContext ctx, byte[] segmentId) {
        for (Operative operative : ctx.operativeInstanceRegistry().snapshot().values()) {
            if (operative instanceof BoundedOperativeInstance bounded) {
                for (List<BoundedSegment> stage : bounded.stageSegments()) {
                    for (BoundedSegment seg : stage) {
                        if (Arrays.equals(seg.identity().encodedBytes(), segmentId)) {
                            return new BoundedLocated(bounded, seg);
                        }
                    }
                }
            }
        }
        return null;
    }

    private record Located(UnboundedOperativeInstance instance, SegmentSlot slot, SegmentId segmentId) {}

    private record BoundedLocated(BoundedOperativeInstance instance, BoundedSegment segment) {}
}
