package com.flezha.sdk.runtime.control;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.sdk.FactoryRegistry;
import com.flezha.sdk.InputTransformers;
import com.flezha.sdk.OutputTransformers;
import com.flezha.sdk.ProcessorHandler;
import com.flezha.sdk.ProcessorRunner;
import com.flezha.sdk.ProcessorSettings;
import com.flezha.sdk.proto.AddProcessor;
import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.proto.OperatorShutdownNotification;
import com.flezha.sdk.runtime.ComputeEngineContext;
import com.flezha.sdk.runtime.config.FlezhaEnv;
import com.flezha.sdk.runtime.control.handlers.AddProcessorHandler;
import com.flezha.sdk.runtime.framing.Frame;
import com.flezha.sdk.runtime.framing.FrameParser;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
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
 *
 * <p>The full SIGTERM-from-Kubernetes path is exercised in the
 * dev-compose state-freshness scenario (§6 of
 * ship-fresh-state-on-sdk-shutdown); here we cover the unit contract:
 * notification correctly framed + bounded ack wait.
 */
class GracefulShutdownTest {

    private static final String FACTORY_ID = "shutdown_test_factory";
    private static final long CONTROL_STREAM_ID = 7L;

    @BeforeEach
    void reset() {
        FactoryRegistry.reset();
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

        // run() blocks waiting for an ack we never send; off-thread it so
        // we can drive the ack within the timeout.
        Thread runThread = new Thread(subject::run, "shutdown-run");
        runThread.start();

        // Poll briefly until the writer captures the frame, then drain
        // the reference_id and signal the ack.
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
        // Empty initial state for the stub processor still serializes via Jackson
        // (e.g. "{}"); just assert it was actually populated, not the exact bytes.
        assertThat(notif.getSerializedState().toByteArray())
                .as("notification carries a non-empty serialized state binary")
                .isNotEmpty();
        assertThat(request.getReferenceId()).isNotEmpty();
        assertThat(capturedStreamId.get()).isEqualTo(CONTROL_STREAM_ID);

        // Releasing the ack lets run() complete before the 2 s timeout.
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

        // 2 s per-future timeout + 500 ms slack inside run(). A larger
        // bound here would mask a regression that lets the SDK block
        // its container's shutdown grace period.
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
        // Matches the post-timeout cleanup race: the wait already removed
        // its registration; a late ack arrives and finds nothing. Must
        // not panic.
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

        // Wait for the first run to emit, then ack it.
        assertThat(firstWriteSeen.await(2, TimeUnit.SECONDS)).isTrue();
        GracefulShutdown.signalAck(capturedRef.get());
        firstRun.join(Duration.ofSeconds(3).toMillis());

        // Second invocation must short-circuit: no additional frames, no
        // additional waits.
        long started = System.currentTimeMillis();
        subject.run();
        long elapsed = System.currentTimeMillis() - started;
        assertThat(elapsed).isLessThan(50L);
    }

    private static ComputeEngineContext newCtxWithProcessor(String processorId) {
        // String state — `new Object()` would serialize to a Jackson
        // FAIL_ON_EMPTY_BEANS error and silently skip the notification,
        // making the bridge mechanics impossible to assert on.
        ProcessorHandler<String, String, String> handler = (input, state) -> input;

        ProcessorSettings<String, String, String> settings = ProcessorSettings
                .<String, String, String>builder()
                .inputTransformer(InputTransformers.text())
                .outputTransformer(OutputTransformers.returnText())
                .runner(ProcessorRunner.THREAD_SPAWN)
                .build();

        FactoryRegistry.registerFactory(FACTORY_ID, settings, "initial-state", () -> handler);

        FlezhaEnv.ResolvedConfig cfg = FlezhaEnv.resolve(Map.of("FLEZHA_WORKER_POOL_SIZE", "1"));
        ComputeEngineContext ctx = new ComputeEngineContext("ns", cfg, 0L);

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
}
