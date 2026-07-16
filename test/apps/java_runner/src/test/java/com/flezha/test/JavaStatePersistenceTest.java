package com.flezha.test;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.sdk.proto.AddOperative;
import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.proto.RestoreInitState;
import com.flezha.sdk.proto.RestoreInitStateResponse;
import com.flezha.sdk.operative.UnboundedOperativeRegistry;
import com.flezha.sdk.runtime.RunnerContext;
import com.flezha.sdk.runtime.config.FlezhaEnv;
import com.flezha.sdk.runtime.control.handlers.AddOperativeHandler;
import com.flezha.sdk.runtime.control.handlers.RestoreInitStateHandler;
import com.flezha.sdk.runtime.segment.SegmentSlot;
import com.flezha.sdk.runtime.state.HeaderTemplate;
import com.flezha.sdk.runtime.state.KryoCodec;
import com.flezha.sdk.runtime.state.SegmentFlusher;
import com.flezha.sdk.runtime.state.SegmentId;
import com.flezha.sdk.runtime.state.SegmentStateSink;
import com.flezha.sdk.state.CoalesceKey;
import com.flezha.sdk.testing.OperationTestHarness;
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
        SegmentFlusher.resetForTesting();
        SegmentStateSink.resetForTesting();
        UnboundedOperativeRegistry.reset();
    }

    @Test
    void counterOperationRecordsChangesAndReplaysDeterministically() {
        OperationTestHarness<Long, CounterOperation.State, Void, CounterChange> h = OperationTestHarness
                .of(new CounterOperation())
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

        var coalesced = SegmentFlusher.coalesce(List.of(a1, a2, a3, b1, b2));
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
        UnboundedOperativeRegistry.registerUnbounded(StatefulCounterUnboundedOperative.class);
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

        // StatefulCounterUnboundedOperative wires IntInputAdapterOperation (name
        // "int_input_adapter"), whose State/Change are CounterOperation.State / CounterChange.
        KryoCodec codec = new KryoCodec(List.of(CounterOperation.State.class, CounterChange.class));
        HeaderTemplate template = new HeaderTemplate();
        int n = 7;
        ByteArrayOutputStream log = new ByteArrayOutputStream();
        for (long seq = 0; seq < n; seq++) {
            byte[] body = codec.serialize(new CounterChange(1L));
            log.writeBytes(walRecord(seq, template.stamp(1, body), body));
        }

        byte[] segmentId =
                SegmentId.of(instanceId, "int_input_adapter", Optional.empty()).encodedBytes();
        byte[] resp = new RestoreInitStateHandler()
                .handle(
                        ctx,
                        CommandRequest.newBuilder()
                                .setReferenceId("r")
                                .setRestoreInitState(RestoreInitState.newBuilder()
                                        .setSegmentId(ByteString.copyFrom(segmentId))
                                        .setChangeLogBytes(ByteString.copyFrom(log.toByteArray()))
                                        .build())
                                .build(),
                        new ArrayList<>());

        SegmentSlot slot =
                ctx.operativeInstanceRegistry().getUnboundedOperative(instanceId).segments().get(0).get(0);
        assertThat(((CounterOperation.State) slot.state().get()).value).isEqualTo((long) n);
        assertThat(RestoreInitStateResponse.parseFrom(resp).getTerminalSeq()).isEqualTo((long) (n - 1));
        assertThat(slot.changeSeq().get()).isEqualTo((long) n);
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
