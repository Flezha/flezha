package com.flezha.sdk.runtime.control;

import com.flezha.sdk.native_.NativeTransport;
import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.proto.OperatorShutdownNotification;
import com.flezha.sdk.runtime.ComputeEngineContext;
import com.flezha.sdk.runtime.framing.FrameEncoder;
import com.flezha.sdk.runtime.processor.ProcessorInstance;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
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
 * the SDK serializes each currently-running processor's state via the
 * same per-driver path used for {@code MigrationSerialization} and
 * emits one {@link OperatorShutdownNotification} per processor on the
 * control channel. The agent overwrites its
 * {@code Overwatch.state.snapshot} with the supplied binary so the next
 * reconnecting operator picks up the operator's true final state — see
 * ship-fresh-state-on-sdk-shutdown.
 *
 * <h2>Timing contract</h2>
 *
 * <ul>
 *   <li>Per-processor ack wait is bounded at <strong>2 seconds</strong>,
 *       parallelised across processors via {@link CompletableFuture#allOf}
 *       so total wait is bounded by 2 s (not 2N s).</li>
 *   <li>The serializer step is in-process (Jackson) and effectively
 *       constant-time; no separate cap is enforced. Processors with
 *       multi-second serializers should refactor.</li>
 *   <li>On timeout the SDK proceeds to exit unconditionally and SHALL
 *       NOT block its container's shutdown grace period.</li>
 * </ul>
 *
 * <h2>SIGKILL is out of scope</h2>
 *
 * {@code kill -9} bypasses the JVM shutdown hook by definition. The
 * agent's {@code Overwatch.state.snapshot} falls back to the most recent
 * periodic snapshot, exactly as today.
 */
public final class GracefulShutdown {

    private static final Logger LOG = LoggerFactory.getLogger(GracefulShutdown.class);

    /** Per-processor ack wait. Total wait is bounded at this value via parallelism. */
    private static final long ACK_TIMEOUT_MS = 2_000L;

    /** Wire-level payload_id reused from the existing agent -> SDK CommandRequest flow. */
    private static final String SHUTDOWN_PAYLOAD_ID = "control_command";

    /** Pending acks keyed by reference_id. The router pushes completion via {@link #signalAck}. */
    private static final Map<String, CompletableFuture<Void>> PENDING_ACKS = new ConcurrentHashMap<>();

    /** Pluggable wire writer so tests can capture frames without a real native context. */
    @FunctionalInterface
    interface FrameWriter {
        /** Returns the same status code semantics as {@link NativeTransport#streamWrite}. */
        int write(long streamId, byte[] frame);
    }

    private final ComputeEngineContext ctx;
    private final long controlStreamId;
    private final FrameWriter writer;
    private final AtomicBoolean shutdownStarted = new AtomicBoolean(false);

    public GracefulShutdown(@Nonnull ComputeEngineContext ctx, long controlStreamId) {
        this(ctx, controlStreamId,
                (streamId, frame) -> NativeTransport.streamWrite(ctx.nativeHandle(), streamId, frame));
    }

    /**
     * Test-only constructor — package-private so the outer test in this
     * module can inject a capturing writer. Production callers go through
     * the 2-argument form, which forwards to {@link NativeTransport#streamWrite}.
     */
    GracefulShutdown(@Nonnull ComputeEngineContext ctx, long controlStreamId, @Nonnull FrameWriter writer) {
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

        Map<String, ProcessorInstance<?, ?, ?>> processors = ctx.processorRegistry().snapshot();
        if (processors.isEmpty()) {
            return;
        }

        List<CompletableFuture<Void>> waits = new ArrayList<>(processors.size());

        for (Map.Entry<String, ProcessorInstance<?, ?, ?>> entry : processors.entrySet()) {
            String processorId = entry.getKey();
            ProcessorInstance<?, ?, ?> instance = entry.getValue();

            byte[] serializedState;
            try {
                serializedState = instance.serializeState(ctx.objectMapper());
            } catch (Exception e) {
                LOG.warn(
                        "Skipping shutdown notification for {} — serializeState failed: {}",
                        processorId,
                        e.toString());
                continue;
            }

            String referenceId = "shutdown-" + processorId + "-" + System.nanoTime();

            CommandRequest request = CommandRequest.newBuilder()
                    .setReferenceId(referenceId)
                    .setOperatorShutdownNotification(OperatorShutdownNotification.newBuilder()
                            .setProcessorId(processorId)
                            .setSerializedState(ByteString.copyFrom(serializedState))
                            .build())
                    .build();

            byte[] frameBytes = FrameEncoder.build(
                    SHUTDOWN_PAYLOAD_ID,
                    request.toByteArray(),
                    Map.of());

            CompletableFuture<Void> ackFuture = registerPendingAck(referenceId);

            int writeStatus = writer.write(controlStreamId, frameBytes);
            if (writeStatus != 0) {
                LOG.warn(
                        "Failed to write OperatorShutdownNotification for {} (status={})",
                        processorId,
                        writeStatus);
                PENDING_ACKS.remove(referenceId);
                continue;
            }

            String pidForLog = processorId;
            CompletableFuture<Void> bounded = ackFuture
                    .orTimeout(ACK_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                    .exceptionally(throwable -> {
                        if (throwable instanceof TimeoutException
                                || throwable.getCause() instanceof TimeoutException) {
                            LOG.warn("OperatorShutdownAck timeout for {}", pidForLog);
                        } else {
                            LOG.warn(
                                    "OperatorShutdownAck wait failed for {}: {}",
                                    pidForLog,
                                    throwable.toString());
                        }
                        PENDING_ACKS.remove(referenceId);
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
            CompletableFuture
                    .allOf(waits.toArray(new CompletableFuture[0]))
                    .get(ACK_TIMEOUT_MS + 500L, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            // Either a timeout (ack never arrived) or an exception inside
            // a future. Both end with the same outcome — exit anyway, the
            // agent has the snapshot it could reach in time.
            LOG.debug("Graceful shutdown allOf finished with: {}", e.toString());
        }
    }

    /**
     * Wakes the per-processor wait registered by {@link #run}. Called
     * from {@code CommandRouter} when an
     * {@link com.flezha.sdk.proto.OperatorShutdownAck} arrives. Missing
     * entries — i.e. the wait already timed out — are logged and
     * dropped without panic.
     */
    public static void signalAck(@Nonnull String referenceId) {
        CompletableFuture<Void> future = PENDING_ACKS.remove(referenceId);
        if (future == null) {
            LOG.debug(
                    "OperatorShutdownAck for unknown reference_id {} — either timed out or never registered",
                    referenceId);
            return;
        }
        future.complete(null);
    }

    /** Test-only: drains the pending-acks map so independent tests do not interfere. */
    static void resetForTests() {
        PENDING_ACKS.clear();
    }

    private static CompletableFuture<Void> registerPendingAck(String referenceId) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        PENDING_ACKS.put(referenceId, future);
        return future;
    }
}
