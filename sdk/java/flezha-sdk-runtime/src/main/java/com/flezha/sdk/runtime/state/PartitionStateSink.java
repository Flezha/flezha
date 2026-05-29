package com.flezha.sdk.runtime.state;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Process-global send path for the per-Partition flusher, mirroring the Rust
 * SDK's {@code partition_state_sink}.
 *
 * <p>At {@code ConnectSocket} (initial boot and again after every reconnect)
 * the streams-connect path calls {@link #installGlobalSink} with a writer
 * wrapping a secondary stream. The flusher ships every wire frame through
 * {@link #send}, which resolves the live sink each call so a reconnect's fresh
 * stream is picked up without re-registering partitions.
 *
 * <p>Two reliability behaviours (Rust Phase D parity):
 * <ul>
 *   <li><b>Reconnect ring</b> — while no sink is installed (reconnect window)
 *       or a write fails, frames are held in a bounded ring (default 1024,
 *       drop-oldest) and replayed through the next installed sink, so state
 *       changes produced mid-reconnect are not silently lost.</li>
 *   <li><b>Broken-pipe-flood detection</b> — a sustained run of write failures
 *       (default &ge; 10 within 5 s) means the data stream is dead while the
 *       control channel still looks alive; the installed reconnect-requester is
 *       invoked to drive a full control reconnect.</li>
 * </ul>
 */
public final class PartitionStateSink {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionStateSink.class);

    /** Data-stream payload id carrying a per-Partition change record. */
    public static final String PAYLOAD_ID_CHANGE = "partition_state_change";

    /** Low-level writer wrapping the current secondary stream; returns {@code true} on a successful write. */
    @FunctionalInterface
    public interface FrameWriter {
        boolean write(@Nonnull byte[] frame);
    }

    private static final Object LOCK = new Object();
    private static FrameWriter sink;
    private static final Deque<byte[]> ring = new ArrayDeque<>();
    private static final Deque<Long> failureAtMs = new ArrayDeque<>();
    private static boolean reconnectRequested;
    private static Runnable reconnectRequester;

    private PartitionStateSink() {}

    /**
     * Install the hook used to drive a control reconnect when a broken-pipe
     * flood is detected. Wired once from the boot path.
     */
    public static void installReconnectRequester(@Nonnull Runnable requester) {
        synchronized (LOCK) {
            reconnectRequester = requester;
        }
    }

    /**
     * Install (or replace) the live sink and replay any frames buffered while
     * it was absent. Called at every {@code ConnectSocket}.
     */
    public static void installGlobalSink(@Nonnull FrameWriter writer) {
        List<byte[]> replay;
        synchronized (LOCK) {
            sink = writer;
            reconnectRequested = false;
            failureAtMs.clear();
            replay = new ArrayList<>(ring);
            ring.clear();
        }
        for (byte[] frame : replay) {
            send(frame);
        }
    }

    /** Drop the current sink (connection lost). Buffered frames are retained for replay. */
    public static void clearSink() {
        synchronized (LOCK) {
            sink = null;
        }
    }

    /**
     * Ship one {@code partition_state_change} wire frame. Resolves the live
     * sink each call; on a write failure (or while no sink is installed) the
     * frame is buffered in the reconnect ring. Never throws.
     */
    public static boolean send(@Nonnull byte[] frame) {
        FrameWriter writer;
        synchronized (LOCK) {
            writer = sink;
        }
        if (writer == null) {
            bufferFrame(frame, false);
            return false;
        }
        boolean ok;
        try {
            ok = writer.write(frame);
        } catch (RuntimeException e) {
            ok = false;
        }
        if (!ok) {
            bufferFrame(frame, true);
            return false;
        }
        return true;
    }

    private static void bufferFrame(byte[] frame, boolean countFailure) {
        Runnable requester = null;
        synchronized (LOCK) {
            if (ring.size() >= ringCapacity()) {
                ring.pollFirst();
            }
            ring.addLast(frame);
            if (countFailure && noteBrokenPipe()) {
                requester = reconnectRequester;
            }
        }
        if (requester != null) {
            LOG.warn("partition state sink: broken-pipe flood detected; requesting control reconnect");
            try {
                requester.run();
            } catch (RuntimeException e) {
                LOG.warn("reconnect requester failed: {}", e.toString());
            }
        }
    }

    /** Caller holds {@link #LOCK}. Returns {@code true} when a reconnect should be requested. */
    private static boolean noteBrokenPipe() {
        long now = System.nanoTime() / 1_000_000L;
        long window = floodWindowMs();
        while (!failureAtMs.isEmpty() && now - failureAtMs.peekFirst() > window) {
            failureAtMs.pollFirst();
        }
        failureAtMs.addLast(now);
        if (!reconnectRequested && failureAtMs.size() >= floodThreshold()) {
            reconnectRequested = true;
            failureAtMs.clear();
            return true;
        }
        return false;
    }

    private static int ringCapacity() {
        return envInt("FLEZHA_RECONNECT_RING_CAPACITY", 1024);
    }

    private static int floodThreshold() {
        return envInt("FLEZHA_BROKEN_PIPE_RECONNECT_THRESHOLD", 10);
    }

    private static long floodWindowMs() {
        return envInt("FLEZHA_BROKEN_PIPE_WINDOW_MS", 5000);
    }

    private static int envInt(String key, int fallback) {
        String raw = System.getenv(key);
        if (raw != null) {
            try {
                int v = Integer.parseInt(raw.trim());
                if (v > 0) {
                    return v;
                }
            } catch (NumberFormatException ignored) {
                // fall through to default
            }
        }
        return fallback;
    }

    /** Test-only: reset all global state. */
    public static void resetForTesting() {
        synchronized (LOCK) {
            sink = null;
            ring.clear();
            failureAtMs.clear();
            reconnectRequested = false;
            reconnectRequester = null;
        }
    }
}
