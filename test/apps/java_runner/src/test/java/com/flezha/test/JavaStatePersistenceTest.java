package com.flezha.test;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.proto.AddOperative;
import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.proto.CompactEntity;
import com.flezha.sdk.proto.CompactEntityResponse;
import com.flezha.sdk.proto.RestoreInitState;
import com.flezha.sdk.proto.RestoreInitStateResponse;
import com.flezha.sdk.runtime.operative.UnboundedOperativeClassRegistry;
import com.flezha.sdk.runtime.RunnerContext;
import com.flezha.sdk.runtime.config.FlezhaEnv;
import com.flezha.sdk.runtime.control.handlers.AddOperativeHandler;
import com.flezha.sdk.runtime.control.handlers.CompactEntityHandler;
import com.flezha.sdk.runtime.control.handlers.RestoreInitStateHandler;
import com.flezha.sdk.runtime.entity.EntitySlot;
import com.flezha.sdk.runtime.state.HeaderTemplate;
import com.flezha.sdk.runtime.state.KryoCodec;
import com.flezha.sdk.runtime.state.EntityFlusher;
import com.flezha.sdk.runtime.state.EntityId;
import com.flezha.sdk.runtime.state.EntityStateRestore;
import com.flezha.sdk.runtime.state.EntityStateSink;
import com.flezha.sdk.state.CoalesceKey;
import com.flezha.sdk.testing.ProcedureTestHarness;
import com.google.protobuf.ByteString;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.zip.CRC32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JavaStatePersistenceTest {

    @BeforeEach
    void resetFlusher() {
        EntityFlusher.resetForTesting();
        EntityStateSink.resetForTesting();
        UnboundedOperativeClassRegistry.reset();
    }

    @Test
    void counterProcedureRecordsChangesAndReplaysDeterministically() {
        ProcedureTestHarness<Long, CounterProcedure.State, Void, CounterChange> h = ProcedureTestHarness
                .of(new CounterProcedure())
                .applyInputs(List.of(1L, 2L, 3L));

        assertThat(h.recordedChanges()).containsExactly(
                new CounterChange(1L), new CounterChange(2L), new CounterChange(3L));
        assertThat(h.state().value).isEqualTo(6L);
        assertThat(h.replayEquals()).isEmpty();
    }

    @Test
    void coalesceCollapsesKeyedChangesToLatest() {
        // Five Changes with two distinct (interval, ts) keys should collapse
        // to two: the latest (interval=1, ts=10) and the latest
        // (interval=2, ts=20).
        TickChange a1 = new TickChange(1, 10, 1);
        TickChange a2 = new TickChange(1, 10, 2);  // collapses with a3
        TickChange a3 = new TickChange(1, 10, 5);  // latest of (1, 10)
        TickChange b1 = new TickChange(2, 20, 7);
        TickChange b2 = new TickChange(2, 20, 9);  // latest of (2, 20)

        var coalesced = EntityFlusher.coalesce(List.of(a1, a2, a3, b1, b2));
        assertThat(coalesced).containsExactly(a3, b2);
    }

    @Test
    void coalesceKeyHashEquivalence() {
        CoalesceKey k1 = CoalesceKey.fromHash(1, 10L);
        CoalesceKey k2 = CoalesceKey.fromHash(1, 10L);
        assertThat(k1).isEqualTo(k2);
    }

    @Test
    void persistenceOffOperativeDoesNotRegisterFlusherForPersistenceCheck() {
        // Indirect check: the toggle is read from UnboundedOperative.persistenceEnabled().
        assertThat(new PersistenceOffUnboundedOperative().persistenceEnabled()).isFalse();
        assertThat(new StatefulCounterUnboundedOperative().persistenceEnabled()).isTrue();
    }

    @Test
    void statefulCounterOperativeResumesFromWalReplayAfterRestart() throws Exception {
        // Mirror of the Rust SDK resume_probe proof against the REAL operative used
        // by the integration harness: a fresh StatefulCounterUnboundedOperative
        // instance is handed the controller's persisted change-log and must resume
        // at N (not 0, the pre-fix bug; not 2N, a double-apply).
        UnboundedOperativeClassRegistry.registerClass(StatefulCounterUnboundedOperative.class);
        FlezhaEnv.ResolvedConfig cfg = FlezhaEnv.resolve(Map.of("FLEZHA_WORKER_POOL_SIZE", "1"));
        RunnerContext ctx = new RunnerContext("ns", cfg, 0L);
        ctx.setWorkerPool(Executors.newFixedThreadPool(1));
        String instanceId = "stateful-1";
        new AddOperativeHandler()
                .handle(
                        ctx,
                        CommandRequest.newBuilder()
                                .setReferenceId("r")
                                .setAddOperative(AddOperative.newBuilder()
                                        .setOperativeId(instanceId)
                                        .setFactoryId("stateful_counter")
                                        .build())
                                .build(),
                        new ArrayList<>());

        // StatefulCounterUnboundedOperative wires IntInputAdapterProcedure (name
        // "int_input_adapter"), whose State/Change are CounterProcedure.State / CounterChange.
        KryoCodec codec = new KryoCodec(List.of(CounterProcedure.State.class, CounterChange.class));
        HeaderTemplate template = new HeaderTemplate();
        int n = 7;
        ByteArrayOutputStream log = new ByteArrayOutputStream();
        for (long seq = 0; seq < n; seq++) {
            byte[] body = codec.serialize(new CounterChange(1L));
            log.writeBytes(walRecord(seq, template.stamp(1, body), body));
        }

        byte[] entityId =
                EntityId.of(instanceId, "int_input_adapter", Optional.empty()).encodedBytes();
        byte[] resp = new RestoreInitStateHandler()
                .handle(
                        ctx,
                        CommandRequest.newBuilder()
                                .setReferenceId("r")
                                .setRestoreInitState(RestoreInitState.newBuilder()
                                        .setEntityId(ByteString.copyFrom(entityId))
                                        .setChangeLogBytes(ByteString.copyFrom(log.toByteArray()))
                                        .build())
                                .build(),
                        new ArrayList<>());

        EntitySlot slot =
                ctx.operativeInstanceRegistry().getUnboundedOperative(instanceId).entities().get(0).get(0);
        assertThat(((CounterProcedure.State) slot.state().get()).value).isEqualTo((long) n);
        assertThat(RestoreInitStateResponse.parseFrom(resp).getTerminalSeq()).isEqualTo((long) (n - 1));
        assertThat(slot.changeSeq().get()).isEqualTo((long) n);
    }

    @Test
    @SuppressWarnings("rawtypes")
    void compactEntityShipsASnapshotThatRoundTripsThroughRestore() throws Exception {
        // The producing half of compaction: a live Entity seeded to N must ship
        // an `entity_state_snapshot` whose Kryo body rebuilds to N through the
        // exact restore path, stamped with the highest seq it folds (N-1).
        UnboundedOperativeClassRegistry.registerClass(StatefulCounterUnboundedOperative.class);
        FlezhaEnv.ResolvedConfig cfg = FlezhaEnv.resolve(Map.of("FLEZHA_WORKER_POOL_SIZE", "1"));
        RunnerContext ctx = new RunnerContext("ns", cfg, 0L);
        ctx.setWorkerPool(Executors.newFixedThreadPool(1));
        String instanceId = "compact-1";
        new AddOperativeHandler()
                .handle(
                        ctx,
                        CommandRequest.newBuilder()
                                .setReferenceId("r")
                                .setAddOperative(AddOperative.newBuilder()
                                        .setOperativeId(instanceId)
                                        .setFactoryId("stateful_counter")
                                        .build())
                                .build(),
                        new ArrayList<>());

        // Seed the Entity's state to N via the restore path (re-arms changeSeq to N).
        KryoCodec codec = new KryoCodec(List.of(CounterProcedure.State.class, CounterChange.class));
        HeaderTemplate template = new HeaderTemplate();
        int n = 5;
        ByteArrayOutputStream log = new ByteArrayOutputStream();
        for (long seq = 0; seq < n; seq++) {
            byte[] body = codec.serialize(new CounterChange(1L));
            log.writeBytes(walRecord(seq, template.stamp(1, body), body));
        }
        byte[] entityId =
                EntityId.of(instanceId, "int_input_adapter", Optional.empty()).encodedBytes();
        new RestoreInitStateHandler()
                .handle(
                        ctx,
                        CommandRequest.newBuilder()
                                .setReferenceId("r")
                                .setRestoreInitState(RestoreInitState.newBuilder()
                                        .setEntityId(ByteString.copyFrom(entityId))
                                        .setChangeLogBytes(ByteString.copyFrom(log.toByteArray()))
                                        .build())
                                .build(),
                        new ArrayList<>());

        // Capture the snapshot frame the compaction ships through the reliable sink.
        List<byte[]> snapshots = new ArrayList<>();
        EntityStateSink.installGlobalSink((payloadId, frame) -> {
            if (payloadId.equals(EntityStateSink.ENTITY_STATE_SNAPSHOT_PAYLOAD_ID)) {
                snapshots.add(frame);
            }
            return true;
        });

        byte[] resp = new CompactEntityHandler()
                .handle(
                        ctx,
                        CommandRequest.newBuilder()
                                .setReferenceId("r")
                                .setCompactEntity(CompactEntity.newBuilder()
                                        .setEntityId(ByteString.copyFrom(entityId))
                                        .build())
                                .build(),
                        new ArrayList<>());

        CompactEntityResponse response = CompactEntityResponse.parseFrom(resp);
        assertThat(response.getSnapshotSeq()).isEqualTo((long) (n - 1));
        assertThat(response.getSnapshotBytesSize()).isGreaterThan(0L);
        assertThat(snapshots).hasSize(1);

        byte[] body = snapshotBody(snapshots.get(0));
        assertThat(response.getSnapshotBytesSize()).isEqualTo((long) body.length);

        EntitySlot slot = ctx.operativeInstanceRegistry()
                .getUnboundedOperative(instanceId)
                .entities()
                .get(0)
                .get(0);
        Procedure procedure = slot.procedureInstance();
        EntityStateRestore.Rebuilt rebuilt =
                EntityStateRestore.rebuild(procedure, codec, Optional.empty(), body, new byte[0], template);
        assertThat(((CounterProcedure.State) rebuilt.state()).value).isEqualTo((long) n);
    }

    /** Extract the header-less body from an entity_state_change/snapshot wire frame. */
    private static byte[] snapshotBody(byte[] frame) {
        int pidLen = ((frame[0] & 0xFF) << 8) | (frame[1] & 0xFF);
        int bodyLenOff = 2 + pidLen + 8 + 16;
        int bodyLen = ((frame[bodyLenOff] & 0xFF) << 24)
                | ((frame[bodyLenOff + 1] & 0xFF) << 16)
                | ((frame[bodyLenOff + 2] & 0xFF) << 8)
                | (frame[bodyLenOff + 3] & 0xFF);
        byte[] body = new byte[bodyLen];
        System.arraycopy(frame, bodyLenOff + 4, body, 0, bodyLen);
        return body;
    }

    /** Synthesize one controller-format WAL record (matches WalWriter.encode_record). */
    private static byte[] walRecord(long seq, byte[] header, byte[] changeBytes) throws Exception {
        int len = 16 + changeBytes.length;
        CRC32 crc = new CRC32();
        crc.update(header);
        crc.update(changeBytes);
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(bos)) {
            out.writeLong(seq);
            out.writeInt(len);
            out.write(header);
            out.write(changeBytes);
            out.writeInt((int) crc.getValue());
            return bos.toByteArray();
        }
    }
}
