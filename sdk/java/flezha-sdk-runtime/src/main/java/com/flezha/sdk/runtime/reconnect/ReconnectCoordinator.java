package com.flezha.sdk.runtime.reconnect;

import com.flezha.sdk.runtime.ComputeEngineContext;
import com.flezha.sdk.runtime.ComputeEngineStatus;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reconnect state machine. Launched on a dedicated virtual thread; wakes up
 * on every {@code onConnectionLost} dispatch from the JNI layer.
 *
 * <p>On unexpected disconnect (status != {@link ComputeEngineStatus#STOPPED}):
 * <ol>
 *   <li>Clear {@code io_streams} registry.</li>
 *   <li>Clear the live processor registry.</li>
 *   <li>Drain worker queues (via {@link com.flezha.sdk.runtime.worker.ProcessorDispatcher#shutdown()}).</li>
 *   <li>Reset {@code compute_engine_id} to {@code "undefined"}.</li>
 *   <li>Loop connect attempts with exponential backoff capped at 30 s until
 *       the handshake succeeds again.</li>
 * </ol>
 *
 * <p>On intentional disconnect (status == STOPPED): do nothing.
 */
public final class ReconnectCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(ReconnectCoordinator.class);

    /** Backoff delays in ms: 200, 400, 800, 1600, 3200, 6400, 12800, 25600, capped at 30_000. */
    public static final long[] BACKOFF_MS = backoffTable();

    private final ComputeEngineContext ctx;
    private final Supplier<Boolean> reconnectAttempt;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread thread;

    public ReconnectCoordinator(
            @Nonnull ComputeEngineContext ctx,
            @Nonnull Supplier<Boolean> reconnectAttempt) {
        this.ctx = ctx;
        this.reconnectAttempt = reconnectAttempt;
    }

    /** Starts the coordinator. Safe to call multiple times; only the first starts a thread. */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        this.thread = Thread.ofVirtual().name("flz-reconnect").start(this::loop);
    }

    /** Handle a connection-lost event from the JNI layer. */
    public void onConnectionLost() {
        if (ctx.status() == ComputeEngineStatus.STOPPED) {
            return;
        }
        resetState();
        if (thread != null) {
            thread.interrupt();
        }
    }

    private void loop() {
        while (running.get()) {
            try {
                Thread.sleep(Duration.ofMillis(100));
            } catch (InterruptedException e) {
                // A connection-lost event woke us up; begin reconnect attempts.
                if (!running.get()) {
                    return;
                }
                attemptReconnectLoop();
            }
        }
    }

    private void attemptReconnectLoop() {
        int attempt = 0;
        while (running.get() && ctx.status() != ComputeEngineStatus.STOPPED) {
            long backoff = BACKOFF_MS[Math.min(attempt, BACKOFF_MS.length - 1)];
            LOG.warn("Reconnecting (attempt {}), sleeping {} ms", attempt + 1, backoff);
            try {
                Thread.sleep(backoff);
            } catch (InterruptedException e) {
                return;
            }
            try {
                if (Boolean.TRUE.equals(reconnectAttempt.get())) {
                    LOG.info("Reconnect succeeded after {} attempts", attempt + 1);
                    return;
                }
            } catch (Throwable t) {
                LOG.warn("Reconnect attempt {} failed: {}", attempt + 1, t.toString());
            }
            attempt++;
        }
    }

    private void resetState() {
        ctx.streamRegistry().clear();
        ctx.processorRegistry().clear();
        if (ctx.processorDispatcher() != null) {
            ctx.processorDispatcher().shutdown();
        }
        ctx.setComputeEngineId("undefined");
    }

    public void shutdown() {
        running.set(false);
        if (thread != null) {
            thread.interrupt();
        }
    }

    private static long[] backoffTable() {
        long[] table = new long[9];
        long value = 200L;
        for (int i = 0; i < table.length; i++) {
            table[i] = Math.min(value, 30_000L);
            value = Math.min(value * 2, 30_000L);
        }
        return table;
    }
}
