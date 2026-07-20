package com.flezha.sdk.runtime.state;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Process-global send path for the per-Segment flusher, mirroring the Rust
 * SDK's {@code segment_state_sink}.
 *
 * <p>At {@code ConnectSocket} (initial boot and again after every reconnect)
 * the streams-connect path calls {@link #installGlobalSink} with a writer
 * wrapping a secondary stream. The flusher ships every wire frame through
 * {@link #send}, which resolves the live sink each call so a reconnect's fresh
 * stream is picked up without re-registering segments.
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
public final class SegmentStateSink {

    private static final Logger LOG = LoggerFactory.getLogger(SegmentStateSink.class);

    /** Data-stream payload id carrying a per-Segment change record. */
    public static final String PAYLOAD_ID_CHANGE = "segment_state_change";

    /**
     * Data-stream payload id carrying an effectively-once effect record: one
     * invocation's changes, outputs, consumed offset and (on a park) its DLQ
     * entry, which the controller persists as ONE WAL record under ONE fsync.
     */
    public static final String PAYLOAD_ID_EFFECT_RECORD = "segment_effect_record";

    /**
     * Destructive eviction of an idle dynamic Segment: the controller unlinks its
     * WAL and snapshots. A recreated Segment cold-starts fresh — this is not a
     * cache flush, the history is gone.
     */
    public static final String PAYLOAD_ID_DELETE_SEGMENT_STATE = "delete_segment_state";

    /**
     * Data-stream payload-ids reserved for the bounded-operative saga compensation
     * change-log (SDK -&gt; controller), same frame layout as {@code segment_state_change}.
     * {@code compensation_intent} records an operation's completion;
     * {@code compensation_done} records a compensation hook's completion.
     */
    public static final String PAYLOAD_ID_COMPENSATION_INTENT = "compensation_intent";

    public static final String PAYLOAD_ID_COMPENSATION_DONE = "compensation_done";

    /**
     * Data-stream payload-id for a per-Segment state snapshot (compaction, SDK
     * -&gt; controller): the Kryo-encoded {@code Self.State} captured at a
     * {@code changeSeq} the Segment has reached, same frame layout as
     * {@code segment_state_change}. The controller writes the snapshot file,
     * truncates the WAL up to that seq, and clears the in-flight compaction.
     * Rides the same reliable sink so a snapshot produced mid-reconnect is not
     * lost. Matches the Rust SDK's {@code SEGMENT_STATE_SNAPSHOT_PAYLOAD_ID}.
     */
    public static final String SEGMENT_STATE_SNAPSHOT_PAYLOAD_ID = "segment_state_snapshot";

    /**
     * Low-level writer wrapping the current secondary stream. The installed
     * writer stamps {@code payloadId} on the payloader envelope, so the same
     * reliable sink carries {@code segment_state_change} and the saga
     * {@code compensation_*} records. Returns {@code true} on a successful write.
     */
    @FunctionalInterface
    public interface FrameWriter {
        boolean write(@Nonnull String payloadId, @Nonnull byte[] frame);
    }

    /**
     * One buffered frame plus its data-stream payload-id, so a frame replayed
     * after a reconnect is re-wrapped under its own id.
     */
    private record RingFrame(String payloadId, byte[] frame) {}

    private static final Object LOCK = new Object();
    private static FrameWriter sink;
    private static boolean everInstalled;
    private static final Deque<RingFrame> ring = new ArrayDeque<>();
    private static final Deque<Long> failureAtMs = new ArrayDeque<>();
    private static boolean reconnectRequested;
    private static Runnable reconnectRequester;

    private SegmentStateSink() {}

    /**
     * Whether a global sink has ever been installed. {@code false} only for
     * transport-less runs (unit tests, standalone); a reconnect window
     * ({@link #clearSink()}) still reports {@code true} — frames buffer in
     * the ring and replay on the next install.
     */
    public static boolean everInstalled() {
        synchronized (LOCK) {
            return everInstalled;
        }
    }

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
        // Credit never carries across sessions: drop the old window (waking
        // any parked sender into the legacy path) until the controller
        // re-offers CreditWindowInit for the fresh attach.
        CreditWindow.disarm();
        List<RingFrame> replay;
        synchronized (LOCK) {
            sink = writer;
            everInstalled = true;
            reconnectRequested = false;
            failureAtMs.clear();
            replay = new ArrayList<>(ring);
            ring.clear();
        }
        for (RingFrame rf : replay) {
            send(rf.payloadId(), rf.frame());
        }
    }

    /** Drop the current sink (connection lost). Buffered frames are retained for replay. */
    public static void clearSink() {
        synchronized (LOCK) {
            sink = null;
        }
    }

    /**
     * Ship one {@code segment_state_change} wire frame. Resolves the live
     * sink each call; on a write failure (or while no sink is installed) the
     * frame is buffered in the reconnect ring. Never throws.
     */
    public static boolean send(@Nonnull String payloadId, @Nonnull byte[] frame) {
        FrameWriter writer;
        synchronized (LOCK) {
            writer = sink;
        }
        if (writer == null) {
            bufferFrame(payloadId, frame, false);
            return false;
        }
        boolean ok;
        try {
            ok = writer.write(payloadId, frame);
        } catch (RuntimeException e) {
            ok = false;
        }
        if (!ok) {
            bufferFrame(payloadId, frame, true);
            return false;
        }
        return true;
    }

    /**
     * Ship one bounded-operative saga compensation marker
     * ({@code compensation_intent} / {@code compensation_done}) for a Segment
     * through the reliable sink. The body is empty — the record's information is
     * the Segment identity plus the payload-id — matching the controller's
     * {@code Flezha.State.Wire.decode_frame/1} (crc32 over an empty body is 0).
     */
    public static void sendCompensationRecord(@Nonnull String payloadId, @Nonnull SegmentId segmentId) {
        byte[] frame = SegmentFlusher.buildSegmentStateChangeFrame(segmentId, 0L, new byte[16], new byte[0]);
        send(payloadId, frame);
    }

    private static void bufferFrame(String payloadId, byte[] frame, boolean countFailure) {
        Runnable requester = null;
        synchronized (LOCK) {
            if (ring.size() >= ringCapacity()) {
                ring.pollFirst();
            }
            ring.addLast(new RingFrame(payloadId, frame));
            if (countFailure && noteBrokenPipe()) {
                requester = reconnectRequester;
            }
        }
        if (requester != null) {
            LOG.warn("segment state sink: broken-pipe flood detected; requesting control reconnect");
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
