package com.flezha.sdk.runtime.control;

import com.flezha.sdk.native_.NativeTransport;
import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.proto.RunnerShutdownNotification;
import com.flezha.sdk.runtime.RunnerContext;
import com.flezha.sdk.runtime.framing.FrameEncoder;
import com.flezha.sdk.runtime.operative.UnboundedOperativeInstance;
import com.flezha.sdk.runtime.operative.UnboundedOperativeStateSerializer;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Java SDK counterpart to the Rust SDK's {@code shutdown.rs} bridge.
 *
 * <p>On graceful shutdown — SIGTERM (Kubernetes pre-stop), explicit
 * {@code Flezha.close()}, or JVM exit via the registered shutdown hook —
 * the SDK serializes each currently-running operative's state via the
 * same per-driver path used for {@code MigrationSerialization} and
 * emits one {@link RunnerShutdownNotification} per operative on the
 * control channel. The controller overwrites its
 * {@code Overwatch.state.snapshot} with the supplied binary so the next
 * reconnecting runner picks up the runner's true final state — see
 * ship-fresh-state-on-sdk-shutdown.
 *
 * <h2>Timing contract</h2>
 *
 * <ul>
 *   <li>Per-operative ack wait is bounded at <strong>2 seconds</strong>,
 *       parallelised across operatives via {@link CompletableFuture#allOf}
 *       so total wait is bounded by 2 s (not 2N s).</li>
 *   <li>The serializer step is in-process (Jackson) and effectively
 *       constant-time; no separate cap is enforced. UnboundedOperatives with
 *       multi-second serializers should refactor.</li>
 *   <li>On timeout the SDK proceeds to exit unconditionally and SHALL
 *       NOT block its container's shutdown grace period.</li>
 * </ul>
 *
 * <h2>SIGKILL is out of scope</h2>
 *
 * {@code kill -9} bypasses the JVM shutdown hook by definition. The
 * controller's {@code Overwatch.state.snapshot} falls back to the most recent
 * periodic snapshot, exactly as today.
 */
public final class GracefulShutdown {

    private static final Logger LOG = LoggerFactory.getLogger(GracefulShutdown.class);

    /** Per-operative ack wait. Total wait is bounded at this value via parallelism. */
    private static final long ACK_TIMEOUT_MS = 2_000L;

    /** Wire-level payload_id reused from the existing controller -> SDK CommandRequest flow. */
    private static final String SHUTDOWN_PAYLOAD_ID = "control_command";

    /** Pluggable wire writer so tests can capture frames without a real native context. */
    @FunctionalInterface
    interface FrameWriter {
        /** Returns the same status code semantics as {@link NativeTransport#streamWrite}. */
        int write(long streamId, byte[] frame);
    }

    private final RunnerContext ctx;
    private final long controlStreamId;
    private final FrameWriter writer;
    private final AtomicBoolean shutdownStarted = new AtomicBoolean(false);

    public GracefulShutdown(@Nonnull RunnerContext ctx, long controlStreamId) {
        this(
                ctx,
                controlStreamId,
                (streamId, frame) -> NativeTransport.streamWrite(ctx.nativeHandle(), streamId, frame));
    }

    /**
     * Test-only constructor — package-private so the outer test in this
     * module can inject a capturing writer. Production callers go through
     * the 2-argument form, which forwards to {@link NativeTransport#streamWrite}.
     */
    GracefulShutdown(@Nonnull RunnerContext ctx, long controlStreamId, @Nonnull FrameWriter writer) {
        this.ctx = ctx;
        this.controlStreamId = controlStreamId;
        this.writer = writer;
    }

    /**
     * Run the graceful-shutdown sequence. Idempotent: subsequent calls
     * (e.g. JVM shutdown hook firing after an explicit {@code close()})
     * observe {@code shutdownStarted == true} and return immediately.
     */
    public void run() {
        if (!shutdownStarted.compareAndSet(false, true)) {
            return;
        }

        Map<String, UnboundedOperativeInstance> operatives =
                ctx.operativeInstanceRegistry().operativeSnapshot();
        if (operatives.isEmpty()) {
            return;
        }

        List<CompletableFuture<String>> waits = new ArrayList<>(operatives.size());

        for (Map.Entry<String, UnboundedOperativeInstance> entry : operatives.entrySet()) {
            String operativeId = entry.getKey();
            UnboundedOperativeInstance instance = entry.getValue();

            byte[] serializedState;
            try {
                serializedState = UnboundedOperativeStateSerializer.serialize(instance);
            } catch (Exception e) {
                LOG.warn(
                        "Skipping shutdown notification for {} — serializeState failed: {}", operativeId, e.toString());
                continue;
            }

            String referenceId = "shutdown-" + operativeId + "-" + System.nanoTime();

            CommandRequest request = CommandRequest.newBuilder()
                    .setReferenceId(referenceId)
                    .setRunnerShutdownNotification(RunnerShutdownNotification.newBuilder()
                            .setOperativeId(operativeId)
                            .setSerializedState(ByteString.copyFrom(serializedState))
                            .build())
                    .build();

            byte[] frameBytes = FrameEncoder.build(SHUTDOWN_PAYLOAD_ID, request.toByteArray(), Map.of());

            CompletableFuture<String> ackFuture = AckRegistry.register(referenceId);

            int writeStatus = writer.write(controlStreamId, frameBytes);
            if (writeStatus != 0) {
                LOG.warn("Failed to write RunnerShutdownNotification for {} (status={})", operativeId, writeStatus);
                AckRegistry.forget(referenceId);
                continue;
            }

            String pidForLog = operativeId;
            CompletableFuture<String> bounded = ackFuture
                    .orTimeout(ACK_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                    .exceptionally(throwable -> {
                        if (throwable instanceof TimeoutException || throwable.getCause() instanceof TimeoutException) {
                            LOG.warn("RunnerShutdownAck timeout for {}", pidForLog);
                        } else {
                            LOG.warn("RunnerShutdownAck wait failed for {}: {}", pidForLog, throwable.toString());
                        }
                        AckRegistry.forget(referenceId);
                        return null;
                    });

            waits.add(bounded);
        }

        if (waits.isEmpty()) {
            return;
        }

        try {
            // The per-future timeouts already bound this; the outer get()
            // is just here to wait for the parallel completions. Add a small
            // slack so a slow thread-spawn doesn't blow past 2 s.
            CompletableFuture.allOf(waits.toArray(new CompletableFuture<?>[0]))
                    .get(ACK_TIMEOUT_MS + 500L, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            // Either a timeout (ack never arrived) or an exception inside
            // a future. Both end with the same outcome — exit anyway, the
            // controller has the snapshot it could reach in time.
            LOG.debug("Graceful shutdown allOf finished with: {}", e.toString());
        }
    }

    /** Test-only: drains the pending-acks map so independent tests do not interfere. */
    static void resetForTests() {
        AckRegistry.resetForTests();
    }
}
