package com.flezha.test;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.sdk.proto.AddRelay;
import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.proto.RestoreInitState;
import com.flezha.sdk.proto.RestoreInitStateResponse;
import com.flezha.sdk.relay.RelayRegistry;
import com.flezha.sdk.runtime.ExecutorContext;
import com.flezha.sdk.runtime.config.FlezhaEnv;
import com.flezha.sdk.runtime.control.handlers.AddRelayHandler;
import com.flezha.sdk.runtime.control.handlers.RestoreInitStateHandler;
import com.flezha.sdk.runtime.partition.PartitionSlot;
import com.flezha.sdk.runtime.state.HeaderTemplate;
import com.flezha.sdk.runtime.state.KryoCodec;
import com.flezha.sdk.runtime.state.PartitionFlusher;
import com.flezha.sdk.runtime.state.PartitionId;
import com.flezha.sdk.runtime.state.PartitionStateSink;
import com.flezha.sdk.state.CoalesceKey;
import com.flezha.sdk.testing.ShardTestHarness;
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
        PartitionFlusher.resetForTesting();
        PartitionStateSink.resetForTesting();
        RelayRegistry.reset();
    }

    @Test
    void counterShardRecordsChangesAndReplaysDeterministically() {
        ShardTestHarness<Long, CounterShard.State, Void, CounterChange> h = ShardTestHarness
                .of(new CounterShard())
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

        var coalesced = PartitionFlusher.coalesce(List.of(a1, a2, a3, b1, b2));
        assertThat(coalesced).containsExactly(a3, b2);
    }

    @Test
    void coalesceKeyHashEquivalence() {
        CoalesceKey k1 = CoalesceKey.fromHash(1, 10L);
        CoalesceKey k2 = CoalesceKey.fromHash(1, 10L);
        assertThat(k1).isEqualTo(k2);
    }

    @Test
    void persistenceOffRelayDoesNotRegisterFlusherForPersistenceCheck() {
        // Indirect check: the toggle is read from Relay.persistenceEnabled().
        assertThat(new PersistenceOffRelay().persistenceEnabled()).isFalse();
        assertThat(new StatefulCounterRelay().persistenceEnabled()).isTrue();
    }

    @Test
    void statefulCounterRelayResumesFromWalReplayAfterRestart() throws Exception {
        // Mirror of the Rust SDK resume_probe proof against the REAL relay used
        // by the integration harness: a fresh StatefulCounterRelay instance is
        // handed the controller's persisted change-log and must resume at N
        // (not 0, the pre-fix bug; not 2N, a double-apply).
        RelayRegistry.registerRelay(StatefulCounterRelay.class);
        FlezhaEnv.ResolvedConfig cfg = FlezhaEnv.resolve(Map.of("FLEZHA_WORKER_POOL_SIZE", "1"));
        ExecutorContext ctx = new ExecutorContext("ns", cfg, 0L);
        ctx.setWorkerPool(Executors.newFixedThreadPool(1));
        String instanceId = "stateful-1";
        new AddRelayHandler()
                .handle(
                        ctx,
                        CommandRequest.newBuilder()
                                .setReferenceId("r")
                                .setAddRelay(AddRelay.newBuilder()
                                        .setRelayId(instanceId)
                                        .setFactoryId("stateful_counter")
                                        .build())
                                .build(),
                        new ArrayList<>());

        // StatefulCounterRelay wires IntInputAdapterShard (name "int_input_adapter"),
        // whose State/Change are CounterShard.State / CounterChange.
        KryoCodec codec = new KryoCodec(List.of(CounterShard.State.class, CounterChange.class));
        HeaderTemplate template = new HeaderTemplate();
        int n = 7;
        ByteArrayOutputStream log = new ByteArrayOutputStream();
        for (long seq = 0; seq < n; seq++) {
            byte[] body = codec.serialize(new CounterChange(1L));
            log.writeBytes(walRecord(seq, template.stamp(1, body), body));
        }

        byte[] partitionId =
                PartitionId.of(instanceId, "int_input_adapter", Optional.empty()).encodedBytes();
        byte[] resp = new RestoreInitStateHandler()
                .handle(
                        ctx,
                        CommandRequest.newBuilder()
                                .setReferenceId("r")
                                .setRestoreInitState(RestoreInitState.newBuilder()
                                        .setPartitionId(ByteString.copyFrom(partitionId))
                                        .setChangeLogBytes(ByteString.copyFrom(log.toByteArray()))
                                        .build())
                                .build(),
                        new ArrayList<>());

        PartitionSlot slot =
                ctx.relayInstanceRegistry().get(instanceId).partitions().get(0).get(0);
        assertThat(((CounterShard.State) slot.state().get()).value).isEqualTo((long) n);
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
