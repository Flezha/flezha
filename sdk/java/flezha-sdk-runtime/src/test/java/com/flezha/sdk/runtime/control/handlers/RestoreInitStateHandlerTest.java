package com.flezha.sdk.runtime.control.handlers;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.proto.AddRelay;
import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.proto.RestoreInitState;
import com.flezha.sdk.proto.RestoreInitStateResponse;
import com.flezha.sdk.relay.Relay;
import com.flezha.sdk.relay.RelayRegistry;
import com.flezha.sdk.runtime.ExecutorContext;
import com.flezha.sdk.runtime.config.FlezhaEnv;
import com.flezha.sdk.runtime.partition.PartitionSlot;
import com.flezha.sdk.runtime.state.HeaderTemplate;
import com.flezha.sdk.runtime.state.KryoCodec;
import com.flezha.sdk.runtime.state.PartitionFlusher;
import com.flezha.sdk.runtime.state.PartitionId;
import com.flezha.sdk.runtime.state.PartitionStateSink;
import com.flezha.sdk.shard.Shard;
import com.flezha.sdk.shard.ShardCtx;
import com.flezha.sdk.shard.ShardException;
import com.flezha.sdk.state.Change;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import com.google.protobuf.ByteString;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.zip.CRC32;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Proves a stateful Java relay resumes its accumulated state from the
 * controller's WAL replay after an executor restart / rebind — the Java mirror
 * of the Rust SDK's {@code resume_probe} proof. Resume must land at N (no loss),
 * never 0 (the pre-fix bug) and never 2N (double-apply).
 */
class RestoreInitStateHandlerTest {

    private static final String FACTORY = "restore_counter";
    private static final String SHARD = "restore_counter_shard";
    private static final String INSTANCE = "inst-A";

    @BeforeEach
    @AfterEach
    void reset() {
        RelayRegistry.reset();
        PartitionFlusher.resetForTesting();
        PartitionStateSink.resetForTesting();
    }

    @Test
    void pureReplayResumesAtN() throws Exception {
        ExecutorContext ctx = newContextWithRelay();
        KryoCodec codec = new KryoCodec(new CounterShard().kryoTypes());
        HeaderTemplate template = new HeaderTemplate();

        int n = 5;
        ByteArrayOutputStream log = new ByteArrayOutputStream();
        for (long seq = 0; seq < n; seq++) {
            byte[] body = codec.serialize(new CounterChange(1L));
            byte[] header = template.stamp(new CounterShard().schemaVersion(), body);
            log.writeBytes(walRecord(seq, header, body));
        }

        byte[] partitionId = PartitionId.of(INSTANCE, SHARD, Optional.empty()).encodedBytes();
        CommandRequest req = CommandRequest.newBuilder()
                .setReferenceId("r")
                .setRestoreInitState(RestoreInitState.newBuilder()
                        .setPartitionId(ByteString.copyFrom(partitionId))
                        .setChangeLogBytes(ByteString.copyFrom(log.toByteArray()))
                        .build())
                .build();

        List<String> errors = new ArrayList<>();
        byte[] resp = new RestoreInitStateHandler().handle(ctx, req, errors);

        assertThat(errors).isEmpty();
        PartitionSlot slot = slot(ctx);
        assertThat(((CounterShard.State) slot.state().get()).value)
                .as("resume must land at N, not 0")
                .isEqualTo((long) n);
        assertThat(RestoreInitStateResponse.parseFrom(resp).getTerminalSeq()).isEqualTo((long) (n - 1));
        assertThat(slot.changeSeq().get())
                .as("change-seq re-armed to terminal+1")
                .isEqualTo((long) n);
    }

    @Test
    void snapshotPlusChangeLogDoesNotDoubleApply() throws Exception {
        ExecutorContext ctx = newContextWithRelay();
        KryoCodec codec = new KryoCodec(new CounterShard().kryoTypes());
        HeaderTemplate template = new HeaderTemplate();

        long k = 3L; // snapshot already accounts for changes 0..k
        CounterShard.State snapState = new CounterShard.State();
        snapState.value = k;
        byte[] snapshot = codec.serialize(snapState);

        int extra = 4; // change-log holds only seq k+1..k+extra
        ByteArrayOutputStream log = new ByteArrayOutputStream();
        for (int i = 0; i < extra; i++) {
            long seq = k + 1 + i;
            byte[] body = codec.serialize(new CounterChange(1L));
            byte[] header = template.stamp(1, body);
            log.writeBytes(walRecord(seq, header, body));
        }

        byte[] partitionId = PartitionId.of(INSTANCE, SHARD, Optional.empty()).encodedBytes();
        CommandRequest req = CommandRequest.newBuilder()
                .setReferenceId("r")
                .setRestoreInitState(RestoreInitState.newBuilder()
                        .setPartitionId(ByteString.copyFrom(partitionId))
                        .setSnapshotBytes(ByteString.copyFrom(snapshot))
                        .setChangeLogBytes(ByteString.copyFrom(log.toByteArray()))
                        .build())
                .build();

        List<String> errors = new ArrayList<>();
        byte[] resp = new RestoreInitStateHandler().handle(ctx, req, errors);

        assertThat(errors).isEmpty();
        // Snapshot REPLACES (value=k); change-log adds `extra`. No re-fold of 0..k.
        assertThat(((CounterShard.State) slot(ctx).state().get()).value).isEqualTo(k + extra);
        assertThat(RestoreInitStateResponse.parseFrom(resp).getTerminalSeq()).isEqualTo(k + extra);
    }

    @Test
    void noLiveSlotValidatesAndAcksWithoutMutating() throws Exception {
        ExecutorContext ctx = newContextWithRelay();
        KryoCodec codec = new KryoCodec(new CounterShard().kryoTypes());
        HeaderTemplate template = new HeaderTemplate();

        ByteArrayOutputStream log = new ByteArrayOutputStream();
        for (long seq = 0; seq < 2; seq++) {
            byte[] body = codec.serialize(new CounterChange(1L));
            log.writeBytes(walRecord(seq, template.stamp(1, body), body));
        }

        // Identity for an instance that was never added — no live slot matches.
        byte[] partitionId =
                PartitionId.of("unknown-inst", SHARD, Optional.empty()).encodedBytes();
        CommandRequest req = CommandRequest.newBuilder()
                .setReferenceId("r")
                .setRestoreInitState(RestoreInitState.newBuilder()
                        .setPartitionId(ByteString.copyFrom(partitionId))
                        .setChangeLogBytes(ByteString.copyFrom(log.toByteArray()))
                        .build())
                .build();

        List<String> errors = new ArrayList<>();
        byte[] resp = new RestoreInitStateHandler().handle(ctx, req, errors);

        assertThat(errors).isEmpty();
        assertThat(RestoreInitStateResponse.parseFrom(resp).getTerminalSeq()).isEqualTo(1L);
        // The real (unmatched) slot stays at its cold-start value.
        assertThat(((CounterShard.State) slot(ctx).state().get()).value).isZero();
    }

    private static ExecutorContext newContextWithRelay() {
        RelayRegistry.registerRelay(CounterRelay.class);
        FlezhaEnv.ResolvedConfig cfg = FlezhaEnv.resolve(Map.of("FLEZHA_WORKER_POOL_SIZE", "1"));
        ExecutorContext ctx = new ExecutorContext("ns", cfg, 0L);
        ctx.setWorkerPool(Executors.newFixedThreadPool(1));
        CommandRequest add = CommandRequest.newBuilder()
                .setReferenceId("r")
                .setAddRelay(AddRelay.newBuilder()
                        .setRelayId(INSTANCE)
                        .setFactoryId(FACTORY)
                        .build())
                .build();
        new AddRelayHandler().handle(ctx, add, new ArrayList<>());
        return ctx;
    }

    private static PartitionSlot slot(ExecutorContext ctx) {
        return ctx.relayInstanceRegistry().get(INSTANCE).partitions().get(0).get(0);
    }

    /** Synthesize one controller-format WAL record. */
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

    // --- Test relay: single changeful Shard whose State holds a running total. ---

    public static final class CounterRelay implements Relay {
        @Override
        public String factoryId() {
            return FACTORY;
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "input").into(CounterShard.class);
        }
    }

    public record CounterChange(long delta) implements Change {}

    public static final class CounterShard
            implements Shard<String, CounterShard.State, Void, CounterChange, WallClock, Void> {

        public static final class State {
            public long value;
        }

        @Override
        public String name() {
            return SHARD;
        }

        @Override
        public Class<String> inputType() {
            return String.class;
        }

        @Override
        public Class<Void> outputType() {
            return Void.class;
        }

        @Override
        public Class<CounterChange> changeType() {
            return CounterChange.class;
        }

        @Override
        public Class<WallClock> clockType() {
            return WallClock.class;
        }

        @Override
        public Class<Void> configType() {
            return Void.class;
        }

        @Override
        public State initState(Optional<String> shardKey, Optional<RestoreInitPayload> initPayload) {
            return new State();
        }

        @Override
        public void applyChange(State state, CounterChange change) {
            state.value += change.delta();
        }

        @Override
        public List<Class<?>> kryoTypes() {
            return List.of(State.class, CounterChange.class);
        }

        @Override
        public void run(String input, StateHandle<State, CounterChange> state, ShardCtx ctx) throws ShardException {
            try {
                state.apply(new CounterChange(Long.parseLong(input)));
            } catch (NumberFormatException ignored) {
                // non-numeric input is dropped
            }
        }
    }
}
