package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.proto.RestoreInitState;
import com.flezha.sdk.proto.RestoreInitStateResponse;
import com.flezha.sdk.runtime.ExecutorContext;
import com.flezha.sdk.runtime.control.CommandHandler;
import com.flezha.sdk.runtime.partition.PartitionSlot;
import com.flezha.sdk.runtime.relay.RelayInstance;
import com.flezha.sdk.runtime.state.ChangeLogReader;
import com.flezha.sdk.runtime.state.HeaderTemplate;
import com.flezha.sdk.runtime.state.KryoCodec;
import com.flezha.sdk.runtime.state.PartitionId;
import com.flezha.sdk.shard.Shard;
import com.flezha.sdk.state.Change;
import com.flezha.sdk.state.RestoreInitPayload;
import com.google.protobuf.ByteString;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;

/**
 * Handles the controller's {@code RestoreInitState} command: a warm restart of
 * a Partition after an executor restart / rebind.
 *
 * <p>The controller ships the persisted snapshot + WAL change-log for one
 * Partition. This handler locates the live {@link PartitionSlot} whose wire
 * identity equals {@code partition_id}, rebuilds its state in place
 * (Kryo-decode the snapshot if present, then fold the change-log via
 * {@code Shard.applyChange}), and re-arms the slot's {@code changeSeq} to
 * {@code terminal_seq + 1} so the next produced frame is contiguous with the
 * persisted history (the controller then skips a WAL realign). It replies with
 * {@code RestoreInitStateResponse{partition_id, terminal_seq}}.
 *
 * <p>When no live slot matches (the relay has not been re-added), it falls back
 * to validating the change-log and acking the terminal seq. There is no
 * SDK-side HotSwap guard: the controller never dispatches {@code RestoreInitState}
 * to a relay that arrived via HotSwap handover (its state was delivered in-band
 * by {@code MigrationDeserialization}), mirroring the Rust SDK.
 */
public final class RestoreInitStateHandler implements CommandHandler {

    @Override
    @Nonnull
    public byte[] handle(@Nonnull ExecutorContext ctx, @Nonnull CommandRequest request, @Nonnull List<String> errors) {
        if (!request.hasRestoreInitState()) {
            errors.add("RestoreInitStateHandler: payload missing");
            return new byte[0];
        }
        RestoreInitState req = request.getRestoreInitState();
        byte[] partitionId = req.getPartitionId().toByteArray();
        byte[] snapshot = req.getSnapshotBytes().toByteArray();
        byte[] changeLog = req.getChangeLogBytes().toByteArray();

        HeaderTemplate headerTemplate = new HeaderTemplate();
        long terminalSeq = 0L;

        Located located = findSlot(ctx, partitionId);
        if (located != null) {
            try {
                terminalSeq = restoreSlot(located, snapshot, changeLog, headerTemplate);
            } catch (RuntimeException e) {
                errors.add("restore_init_state: apply failed: " + e.getMessage());
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

        return RestoreInitStateResponse.newBuilder()
                .setPartitionId(ByteString.copyFrom(partitionId))
                .setTerminalSeq(terminalSeq)
                .build()
                .toByteArray();
    }

    /**
     * Rebuild the slot's state from snapshot + change-log and re-arm its
     * change-seq. Returns the terminal (highest) WAL seq, 0 when the change-log
     * is empty.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static long restoreSlot(Located located, byte[] snapshot, byte[] changeLog, HeaderTemplate headerTemplate) {
        PartitionSlot slot = located.slot();
        Shard shard = slot.dispatcher().shardInstance();
        KryoCodec codec = new KryoCodec(located.instance().kryoTypes());

        slot.stateLock().lock();
        try {
            // User init first; then the snapshot REPLACES it (the controller has
            // already stripped the snapshot's 16-byte header), and finally the
            // change-log folds on top — so snapshot-covered changes are never
            // re-applied.
            Object state = shard.initState(slot.shardKey(), Optional.of(new RestoreInitPayload(snapshot, changeLog)));
            if (snapshot.length > 0) {
                state = codec.deserialize(snapshot, Object.class);
            }
            List<ChangeLogReader.Record> records = ChangeLogReader.parse(changeLog, headerTemplate);
            for (ChangeLogReader.Record record : records) {
                Change change = codec.deserialize(record.changeBytes(), Change.class);
                shard.applyChange(state, change);
            }
            slot.state().set(state);
            long terminal = ChangeLogReader.terminalSeq(records);
            slot.changeSeq().set(terminal + 1);
            return terminal;
        } finally {
            slot.stateLock().unlock();
        }
    }

    /**
     * Scan every live relay's partitions for the slot whose binary identity
     * equals {@code partitionId}. The identity is built the same way the flusher
     * frames {@code partition_id}, so a byte-equal match is exact.
     */
    private static Located findSlot(ExecutorContext ctx, byte[] partitionId) {
        for (RelayInstance instance :
                ctx.relayInstanceRegistry().relaySnapshot().values()) {
            List<List<PartitionSlot>> partitions = instance.partitions();
            for (List<PartitionSlot> shardPartitions : partitions) {
                for (PartitionSlot slot : shardPartitions) {
                    String shardName =
                            instance.topology().shards().get(slot.shardId()).name();
                    byte[] identity = PartitionId.of(instance.instanceId(), shardName, slot.shardKey())
                            .encodedBytes();
                    if (Arrays.equals(identity, partitionId)) {
                        return new Located(instance, slot);
                    }
                }
            }
        }
        return null;
    }

    private record Located(RelayInstance instance, PartitionSlot slot) {}
}
