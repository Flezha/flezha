package com.flezha.sdk.runtime.control;

import com.flezha.sdk.native_.NativeTransport;
import com.flezha.sdk.runtime.RunnerContext;
import com.flezha.sdk.runtime.config.FlezhaEnv;
import com.flezha.sdk.runtime.entity.EntityCounters;
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
 * Steady-state data-channel heartbeat. Periodically writes a bare frame on the
 * {@code data_heartbeat} payload to every secondary stream, so an
 * idle-but-healthy operative keeps the controller's per-channel liveness clock
 * fresh without producing real output. A silent data-plane wedge stops the
 * heartbeats and the controller flags {@code output_stall}.
 *
 * <p>Parity with the Rust SDK ({@code flz_transport::liveness} +
 * {@code spawn_data_heartbeat}): same {@code FLEZHA_DATA_HEARTBEAT_MS} knob and
 * jitter. The send goes through {@link NativeTransport#streamWrite} under the
 * JNI per-stream write lock, so it never interleaves with operative-output writes.
 */
public final class DataHeartbeatSender {

    /** Dedicated heartbeat payload — recognized by the controller's StreamChannel. */
    public static final String DATA_HEARTBEAT_PAYLOAD_ID = "data_heartbeat";

    private static final Logger LOG = LoggerFactory.getLogger(DataHeartbeatSender.class);

    private final RunnerContext ctx;
    private final long intervalMs;
    private final ScheduledExecutorService scheduler;

    public DataHeartbeatSender(@Nonnull RunnerContext ctx) {
        this(ctx, FlezhaEnv.dataHeartbeatMs());
    }

    DataHeartbeatSender(@Nonnull RunnerContext ctx, long intervalMs) {
        this.ctx = ctx;
        this.intervalMs = intervalMs;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "flz-data-heartbeat");
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
        // Dispatch-progress self-report: this scheduler thread stays alive even
        // when the dispatch pool is wedged, so the heartbeat alone proves only
        // that the process exists. These counters are what let the controller
        // tell "alive but frozen" apart from "healthy and idle".
        byte[] frame = FrameEncoder.build(
                DATA_HEARTBEAT_PAYLOAD_ID,
                new byte[0],
                Map.of(
                        "dispatches",
                        Long.toString(EntityCounters.dispatchesCompleted()),
                        "blocked_parks",
                        Long.toString(EntityCounters.blockedParkEpisodes()),
                        "credit_parked",
                        Long.toString(com.flezha.sdk.runtime.state.CreditWindow.creditParked()),
                        "restore_discarded_inputs",
                        Long.toString(EntityCounters.restoreDiscardedInputs()),
                        // Wheel occupancy: a runaway registration shows here long
                        // before it is fatal, and a `timers_fired` that stops
                        // moving while `timers_armed` climbs is the wheel wedged
                        // rather than the Operative idle.
                        "timers_armed",
                        Long.toString(com.flezha.sdk.runtime.entity.TimerCounters.armed()),
                        "timers_fired",
                        Long.toString(com.flezha.sdk.runtime.entity.TimerCounters.firedTotal())));
        for (long sid : ctx.streamRegistry().secondaries()) {
            if (sid > 0) {
                try {
                    // Best-effort: a failed write means that secondary died; the
                    // next ConnectSocket repopulates the registry.
                    NativeTransport.streamWrite(ctx.nativeHandle(), sid, frame);
                } catch (Throwable t) {
                    LOG.debug("data heartbeat send failed on stream {}: {}", sid, t.toString());
                }
            }
        }
        scheduleNext();
    }
}
