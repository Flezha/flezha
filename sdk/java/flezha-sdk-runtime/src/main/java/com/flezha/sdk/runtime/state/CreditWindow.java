package com.flezha.sdk.runtime.state;

import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sender-side credit window for the runner → controller state stream.
 * Mirrors {@code sdk/rust/src/segment/credit.rs} with identical semantics.
 *
 * <p>The controller offers a byte window via the {@code CreditWindowInit}
 * control command; {@link #arm} adopts it and the ack rides back as the
 * command's {@code CommandResponse}. Every {@code segment_state_change}
 * frame {@link #acquire}s its wire size before the sink write, and
 * {@code CreditGrant} frames {@link #grant} bytes back as the controller
 * fsyncs. An exhausted window parks the sending thread — park, never drop.
 *
 * <p>Guardrails: {@link #arm}/{@link #grant} run on the control router and
 * never block, so heartbeats and grants keep flowing while senders are
 * parked; the park is a bounded wait loop (50 ms recheck) so a lost grant
 * degrades to a retry loop with warnings; {@link #creditParked()} advances
 * on every wait iteration so a credit-parked runner classifies as
 * fed-but-parked in the liveness self-report. A frame larger than the whole
 * window is sent once the window is fully replenished (single-frame
 * overshoot), mirroring the controller's accept-on-empty-buffer rule.
 */
public final class CreditWindow {

    private static final Logger LOG = LoggerFactory.getLogger(CreditWindow.class);

    private static final long PARK_TIMEOUT_MS = 50;
    private static final long WARN_AFTER_NANOS = 5_000_000_000L;

    private static final Object LOCK = new Object();
    private static final AtomicLong CREDIT_PARKED = new AtomicLong();

    @Nullable
    private static String session;

    private static long windowBytes;
    private static long available;

    private CreditWindow() {}

    /** Adopt a controller-offered window, replacing any previous session. */
    public static void arm(@Nonnull String newSession, long newWindowBytes) {
        synchronized (LOCK) {
            LOG.info("credit window armed: session={} window_bytes={}", newSession, newWindowBytes);
            session = newSession;
            windowBytes = newWindowBytes;
            available = newWindowBytes;
            LOCK.notifyAll();
        }
    }

    /**
     * Drop the current credit session (reconnect / new {@code ConnectSocket}).
     * Parked senders wake and proceed un-metered until the next {@link #arm}.
     */
    public static void disarm() {
        synchronized (LOCK) {
            if (session != null) {
                LOG.info("credit window disarmed (reconnect)");
            }
            session = null;
            windowBytes = 0;
            available = 0;
            LOCK.notifyAll();
        }
    }

    /**
     * Apply a {@code CreditGrant}. A grant whose session does not match the
     * current one is stale (minted for a pre-reconnect session): discarded.
     */
    public static void grant(@Nonnull String grantSession, long bytes) {
        synchronized (LOCK) {
            if (grantSession.equals(session)) {
                available = Math.min(available + bytes, windowBytes);
                LOCK.notifyAll();
            } else {
                LOG.debug("discarding stale credit grant for session {}", grantSession);
            }
        }
    }

    /**
     * Debit {@code frameLen} bytes from the window, parking the calling
     * thread until enough credit is available. No-op for a legacy session.
     * Must never be called from the control router thread.
     */
    public static void acquire(long frameLen) {
        synchronized (LOCK) {
            long parkedSince = 0;
            boolean warned = false;
            while (true) {
                if (session == null) {
                    return;
                }
                if (available >= frameLen || available >= windowBytes) {
                    available -= frameLen;
                    return;
                }

                CREDIT_PARKED.incrementAndGet();
                if (parkedSince == 0) {
                    parkedSince = System.nanoTime();
                }
                if (!warned && System.nanoTime() - parkedSince >= WARN_AFTER_NANOS) {
                    warned = true;
                    LOG.warn(
                            "sender parked on credit for {}ms (need {} bytes, {} available)"
                                    + " — controller fsync stalled or grant lost",
                            (System.nanoTime() - parkedSince) / 1_000_000,
                            frameLen,
                            available);
                }
                try {
                    LOCK.wait(PARK_TIMEOUT_MS);
                } catch (InterruptedException e) {
                    // Shutdown path: re-assert and let the frame go un-metered
                    // rather than wedging teardown on a dead controller.
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    /** Process-global count of credit-park wait iterations. */
    public static long creditParked() {
        return CREDIT_PARKED.get();
    }

    /** Current credit-session id, if armed. Test/diagnostic surface. */
    @Nullable
    public static String currentSession() {
        synchronized (LOCK) {
            return session;
        }
    }

    /** Currently available bytes. Test/diagnostic surface. */
    public static long availableBytes() {
        synchronized (LOCK) {
            return available;
        }
    }
}
