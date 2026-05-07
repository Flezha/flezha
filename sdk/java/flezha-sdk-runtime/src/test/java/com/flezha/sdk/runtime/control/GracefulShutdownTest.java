package com.flezha.sdk.runtime.control;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.proto.AddProcessor;
import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.proto.OperatorShutdownNotification;
import com.flezha.sdk.relay.Relay;
import com.flezha.sdk.relay.RelayRegistry;
import com.flezha.sdk.runtime.ComputeEngineContext;
import com.flezha.sdk.runtime.config.FlezhaEnv;
import com.flezha.sdk.runtime.control.handlers.AddProcessorHandler;
import com.flezha.sdk.runtime.framing.Frame;
import com.flezha.sdk.runtime.framing.FrameParser;
import com.flezha.sdk.shard.Shard;
import com.flezha.sdk.shard.ShardCtx;
import com.flezha.sdk.shard.ShardException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Bridge mechanics for {@link GracefulShutdown}. Drives {@code run()}
 * against a capturing {@link GracefulShutdown.FrameWriter} that stands
 * in for the agent's control channel, then decodes the captured frame
 * and asserts protocol shape.
 */
class GracefulShutdownTest {

    private static final String FACTORY_ID = "shutdown_test_factory";
    private static final long CONTROL_STREAM_ID = 7L;

    @BeforeEach
    void reset() {
        RelayRegistry.reset();
        GracefulShutdown.resetForTests();
    }

    @Test
    @DisplayName("run() emits a framed OperatorShutdownNotification per processor")
    void emitsFramedOperatorShutdownNotification() throws Exception {
        ComputeEngineContext ctx = newCtxWithProcessor("p1");

        List<byte[]> captured = new ArrayList<>();
        AtomicReference<Long> capturedStreamId = new AtomicReference<>();
        GracefulShutdown.FrameWriter capturingWriter = (streamId, frame) -> {
            capturedStreamId.set(streamId);
            captured.add(frame);
            return 0;
        };

        GracefulShutdown subject = new GracefulShutdown(ctx, CONTROL_STREAM_ID, capturingWriter);

        Thread runThread = new Thread(subject::run, "shutdown-run");
        runThread.start();

        long deadline = System.currentTimeMillis() + 1_000;
        while (captured.isEmpty() && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }

        assertThat(captured)
                .as("graceful shutdown emits exactly one frame for one processor")
                .hasSize(1);

        byte[] frame = captured.get(0);
        Frame parsed = parseFirstFrame(frame);
        assertThat(parsed.payloadId()).isEqualTo("control_command");

        CommandRequest request = CommandRequest.parseFrom(parsed.message());
        assertThat(request.getCommandCase()).isEqualTo(CommandRequest.CommandCase.OPERATOR_SHUTDOWN_NOTIFICATION);
        OperatorShutdownNotification notif = request.getOperatorShutdownNotification();
        assertThat(notif.getProcessorId()).isEqualTo("p1");
        assertThat(notif.getSerializedState().toByteArray())
                .as("notification carries a non-empty serialized state binary")
                .isNotEmpty();
        assertThat(request.getReferenceId()).isNotEmpty();
        assertThat(capturedStreamId.get()).isEqualTo(CONTROL_STREAM_ID);

        GracefulShutdown.signalAck(request.getReferenceId());
        runThread.join(Duration.ofSeconds(3).toMillis());
        assertThat(runThread.isAlive()).isFalse();
    }

    @Test
    @DisplayName("run() returns within ~2.5s when no ack arrives")
    void returnsWithin2sWhenAckNeverArrives() {
        ComputeEngineContext ctx = newCtxWithProcessor("p_timeout");

        GracefulShutdown.FrameWriter neverAcking = (streamId, frame) -> 0;
        GracefulShutdown subject = new GracefulShutdown(ctx, CONTROL_STREAM_ID, neverAcking);

        long started = System.currentTimeMillis();
        subject.run();
        long elapsed = System.currentTimeMillis() - started;

        assertThat(elapsed)
                .as("graceful shutdown should bound wait at 2s + small slack")
                .isLessThan(3_000L);
        assertThat(elapsed)
                .as("graceful shutdown should wait the full 2s before timing out")
                .isGreaterThanOrEqualTo(1_900L);
    }

    @Test
    @DisplayName("signalAck() for an unknown reference does not throw")
    void signalAckForUnknownReferenceIsBenign() {
        GracefulShutdown.signalAck("never-registered-ref");
    }

    @Test
    @DisplayName("run() is idempotent across SIGTERM + close()")
    void runIsIdempotent() throws InterruptedException {
        ComputeEngineContext ctx = newCtxWithProcessor("p_idem");
        CountDownLatch firstWriteSeen = new CountDownLatch(1);
        AtomicReference<String> capturedRef = new AtomicReference<>();

        GracefulShutdown.FrameWriter writer = (streamId, frame) -> {
            try {
                Frame parsed = parseFirstFrame(frame);
                CommandRequest req = CommandRequest.parseFrom(parsed.message());
                capturedRef.set(req.getReferenceId());
            } catch (Exception ignored) {
                // ignore
            }
            firstWriteSeen.countDown();
            return 0;
        };

        GracefulShutdown subject = new GracefulShutdown(ctx, CONTROL_STREAM_ID, writer);
        Thread firstRun = new Thread(subject::run, "first-run");
        firstRun.start();

        assertThat(firstWriteSeen.await(2, TimeUnit.SECONDS)).isTrue();
        GracefulShutdown.signalAck(capturedRef.get());
        firstRun.join(Duration.ofSeconds(3).toMillis());

        long started = System.currentTimeMillis();
        subject.run();
        long elapsed = System.currentTimeMillis() - started;
        assertThat(elapsed).isLessThan(50L);
    }

    private static ComputeEngineContext newCtxWithProcessor(String processorId) {
        if (RelayRegistry.factoryIds().isEmpty()) {
            RelayRegistry.registerRelay(ShutdownTestRelay.class);
        }
        FlezhaEnv.ResolvedConfig cfg = FlezhaEnv.resolve(Map.of("FLEZHA_WORKER_POOL_SIZE", "1"));
        ComputeEngineContext ctx = new ComputeEngineContext("ns", cfg, 0L);
        ctx.setWorkerPool(Executors.newFixedThreadPool(1));

        CommandRequest add = CommandRequest.newBuilder()
                .setReferenceId("ref")
                .setAddProcessor(AddProcessor.newBuilder()
                        .setProcessorId(processorId)
                        .setFactoryId(FACTORY_ID)
                        .build())
                .build();
        new AddProcessorHandler().handle(ctx, add, new ArrayList<>());
        return ctx;
    }

    private static Frame parseFirstFrame(byte[] frameBytes) {
        FrameParser parser = new FrameParser();
        Iterable<Frame> frames = parser.feed(frameBytes);
        for (Frame frame : frames) {
            return frame;
        }
        throw new IllegalStateException("expected at least one frame in captured bytes");
    }

    public static final class ShutdownTestRelay implements Relay {
        @Override public String factoryId() { return FACTORY_ID; }

        @Override public void channels(ChannelBuilder c) {
            c.input(String.class, "input").into(ShutdownTestShard.class);
        }
    }

    public static final class ShutdownTestShard
            implements Shard<String, String, String,
                    com.flezha.sdk.state.NoChange,
                    com.flezha.sdk.state.WallClock, Void> {
        @Override public String name() { return "shutdown_test_shard"; }
        @Override public Class<String> inputType() { return String.class; }
        @Override public Class<String> outputType() { return String.class; }
        @Override public Class<com.flezha.sdk.state.NoChange> changeType() { return com.flezha.sdk.state.NoChange.class; }
        @Override public Class<com.flezha.sdk.state.WallClock> clockType() { return com.flezha.sdk.state.WallClock.class; }
        @Override public Class<Void> configType() { return Void.class; }
        @Override public String initState(Optional<String> shardKey,
                Optional<com.flezha.sdk.state.RestoreInitPayload> initPayload) { return "initial-state"; }
        @Override public void run(String input,
                com.flezha.sdk.state.StateHandle<String, com.flezha.sdk.state.NoChange> state,
                ShardCtx ctx) throws ShardException { }
    }
}
