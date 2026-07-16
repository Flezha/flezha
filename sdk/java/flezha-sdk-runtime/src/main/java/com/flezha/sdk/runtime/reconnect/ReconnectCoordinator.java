package com.flezha.sdk.runtime.reconnect;

import com.flezha.sdk.runtime.RunnerContext;
import com.flezha.sdk.runtime.RunnerStatus;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reconnect state machine. Launched on a dedicated virtual thread; wakes up
 * on every {@code onConnectionLost} dispatch from the JNI layer.
 *
 * <p>On unexpected disconnect (status != {@link RunnerStatus#STOPPED}):
 * <ol>
 *   <li>Clear {@code io_streams} registry.</li>
 *   <li>Clear the live operative registry.</li>
 *   <li>Drain worker queues (via {@link com.flezha.sdk.runtime.worker.OperationDispatcher#shutdown()}).</li>
 *   <li>Reset {@code runner_id} to {@code "undefined"}.</li>
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

    private final RunnerContext ctx;
    private final Supplier<Boolean> reconnectAttempt;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread thread;

    public ReconnectCoordinator(@Nonnull RunnerContext ctx, @Nonnull Supplier<Boolean> reconnectAttempt) {
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
        if (ctx.status() == RunnerStatus.STOPPED) {
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
        while (running.get() && ctx.status() != RunnerStatus.STOPPED) {
            // Full jitter on the schedule so a controller restart that drops the
            // whole fleet doesn't produce a synchronized reconnect storm (the
            // Rust SDK jitters the same schedule via flz_transport::liveness).
            long backoff = jitter(BACKOFF_MS[Math.min(attempt, BACKOFF_MS.length - 1)]);
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
        ctx.operativeInstanceRegistry().clear();
        ctx.setRunnerId("undefined");
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

    /** Full jitter into {@code [base/2, base]}. Decorrelates a reconnecting fleet. */
    public static long jitter(long baseMs) {
        if (baseMs < 2) {
            return baseMs;
        }
        long half = baseMs / 2;
        return half + ThreadLocalRandom.current().nextLong(half + 1);
    }
}
