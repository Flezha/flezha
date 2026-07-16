package com.flezha.sdk.runtime.control;

import com.flezha.sdk.native_.NativeTransport;
import com.flezha.sdk.proto.Ping;
import com.flezha.sdk.runtime.RunnerContext;
import com.flezha.sdk.runtime.config.FlezhaEnv;
import com.flezha.sdk.runtime.framing.FrameEncoder;
import com.flezha.sdk.runtime.reconnect.ReconnectCoordinator;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Steady-state control-channel keepalive. Periodically sends a bare {@link Ping}
 * on the dedicated {@code control_ping} payload; the controller echoes it, which
 * re-arms the SDK's flz_jni read-level idle watchdog. A wedged controller stops
 * echoing and the watchdog drives reconnect.
 *
 * <p>Shared design with the Rust SDK ({@code flz_transport::liveness}); both read
 * the same {@code FLEZHA_CONTROL_*} env and jitter the same way. The send goes
 * through {@link NativeTransport#streamWrite} under the JNI per-stream write
 * lock, so it never interleaves with command-response writes.
 */
public final class KeepaliveSender {

    /** Dedicated keepalive payload — never enters the command path on either side. */
    public static final String CONTROL_PING_PAYLOAD_ID = "control_ping";

    private static final Logger LOG = LoggerFactory.getLogger(KeepaliveSender.class);

    private final RunnerContext ctx;
    private final long intervalMs;
    private final ScheduledExecutorService scheduler;

    public KeepaliveSender(@Nonnull RunnerContext ctx) {
        this(ctx, FlezhaEnv.controlKeepaliveMs());
    }

    KeepaliveSender(@Nonnull RunnerContext ctx, long intervalMs) {
        this.ctx = ctx;
        this.intervalMs = intervalMs;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "flz-keepalive");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        scheduleNext();
    }

    public void stop() {
        scheduler.shutdownNow();
    }

    private void scheduleNext() {
        try {
            scheduler.schedule(this::tick, ReconnectCoordinator.jitter(intervalMs), TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException ignored) {
            // Scheduler shut down (close()) — stop quietly.
        }
    }

    private void tick() {
        long sid = ctx.streamRegistry().controlStreamId();
        if (sid > 0) {
            try {
                byte[] frame = FrameEncoder.build(
                        CONTROL_PING_PAYLOAD_ID,
                        Ping.newBuilder()
                                .setNonce("ka-" + System.nanoTime())
                                .build()
                                .toByteArray(),
                        Map.of());
                // Best-effort: a failed write means the control stream died; the
                // flz_jni reader detects the same and drives reconnect.
                NativeTransport.streamWrite(ctx.nativeHandle(), sid, frame);
            } catch (Throwable t) {
                LOG.debug("keepalive ping send failed: {}", t.toString());
            }
        }
        scheduleNext();
    }
}
