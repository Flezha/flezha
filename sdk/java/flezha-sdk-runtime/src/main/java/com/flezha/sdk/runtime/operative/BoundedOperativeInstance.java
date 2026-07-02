package com.flezha.sdk.runtime.operative;

import com.flezha.sdk.runtime.bounded.BoundedOperativeRunner.BoundedSegment;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-{@code AddOperative} runtime marker for a bounded-kind operative, carrying
 * its live <strong>control surface</strong>: a cancellation token and an inbox.
 * Unlike {@link ContinuousOperativeInstance} it spawns no segment workers and
 * holds no continuous mailboxes — the bounded's sequential driver (see
 * {@code BoundedOperativeRunner}) runs on a dedicated thread and reaches this
 * instance for cancellation + post-start input.
 *
 * <p>The control surface is instance-scoped (not process-global): the
 * {@code RemOperative} hard stop ({@link #cancel}), the cooperative
 * {@code BoundedStop} handler ({@link #cancel}), the {@code maxDuration} watcher
 * ({@link #cancel}), and the primary-stream dispatch path ({@link #offerInbox})
 * all route through the live instance held in the operative instance registry,
 * which is itself cleared on reconnect.
 *
 * <p>Co-located with {@link ContinuousOperativeInstance} so the sealed
 * {@link Operative} seal holds; the runtime bounded plumbing otherwise lives in
 * {@code com.flezha.sdk.runtime.bounded}.
 */
public final class BoundedOperativeInstance implements Operative {

    private static final Logger LOG = LoggerFactory.getLogger(BoundedOperativeInstance.class);

    /** Inbox capacity — post-start input frames beyond this are dropped (best-effort). */
    private static final int INBOX_CAPACITY = 1024;

    /**
     * Safety-net cap on the crash-resume wait. The expected count is the
     * authoritative release signal; this only prevents a permanent hold if a
     * restore never lands (e.g. a controller/SDK persisted-Segment mismatch),
     * degrading to a best-effort re-run rather than a hung operative.
     */
    private static final long RESUME_WAIT_TIMEOUT_MS = 30_000L;

    /** Re-check cadence so a cooperative cancel is observed within ~200 ms. */
    private static final long RESUME_WAIT_CHUNK_MS = 200L;

    private final String instanceId;
    private final String factoryId;
    private final AtomicBoolean cancelled = new AtomicBoolean(false);
    private final BlockingQueue<byte[]> inbox = new LinkedBlockingQueue<>(INBOX_CAPACITY);

    // Crash-resume gate + pre-built Segment table (structured bounded only). The
    // Segments are wired before the driver spawns so the inbound RestoreInitState
    // handler reaches them by wire identity and the driver holds its first
    // Operation on awaitRestore() until every expected restore has landed — one
    // countdown per landed Segment restore. A fresh / run-sugar operative gets a
    // 0-count latch (no wait).
    private volatile List<List<BoundedSegment>> stageSegments = List.of();
    private volatile CountDownLatch restoreGate = new CountDownLatch(0);

    // Saga crash-resume markers (§4.3), keyed by SegmentId.encoded(), shipped
    // back on RestoreInitState: `intents` ⇒ the Segment's operation completed
    // forward and may need undo; `done` ⇒ its compensation already ran (skip).
    // Non-empty `intents` ⇒ the driver finishes the compensation instead of
    // re-running the forward chain.
    private final Set<String> compensationIntents = ConcurrentHashMap.newKeySet();
    private final Set<String> compensationDone = ConcurrentHashMap.newKeySet();

    public BoundedOperativeInstance(@Nonnull String instanceId, @Nonnull String factoryId) {
        this.instanceId = instanceId;
        this.factoryId = factoryId;
    }

    @Override
    @Nonnull
    public String instanceId() {
        return instanceId;
    }

    @Nonnull
    public String factoryId() {
        return factoryId;
    }

    /** Trip the cancellation token (cooperative {@code BoundedStop}, hard {@code RemOperative}, or duration elapse). */
    public void cancel() {
        cancelled.set(true);
    }

    /** True once the cancellation token has been tripped. */
    public boolean isCancelled() {
        return cancelled.get();
    }

    /** The inbox drained by {@code BoundedOperativeCtx.recv()}; fed by the dispatch path. */
    @Nonnull
    public BlockingQueue<byte[]> inbox() {
        return inbox;
    }

    /**
     * Route a post-start input frame to this instance's inbox. Returns
     * {@code true} if accepted, {@code false} if the inbox is full (the frame
     * is dropped — best-effort delivery).
     */
    public boolean offerInbox(@Nonnull byte[] payload) {
        return inbox.offer(payload);
    }

    // ---- crash-resume gate (structured bounded) -----------------------------

    /**
     * Install the pre-built per-stage Segments and the number of Controller
     * Segment restores to wait for before the first Operation runs. Called by
     * {@code AddOperativeHandler} before the instance is registered and the
     * driver spawns, so the restore frames that follow the ACK reach live
     * Segments and the driver holds on {@link #awaitRestore}. Run-sugar wires an
     * empty list and {@code 0}.
     */
    public void wireStageSegments(@Nonnull List<List<BoundedSegment>> segments, int expected) {
        this.stageSegments = segments;
        this.restoreGate = new CountDownLatch(Math.max(0, expected));
    }

    /** The pre-built per-stage Segments (empty for run-sugar / before wiring). */
    @Nonnull
    public List<List<BoundedSegment>> stageSegments() {
        return stageSegments;
    }

    /**
     * Record one landed Segment restore and wake a driver parked on the resume
     * gate. Called unconditionally by the {@code RestoreInitState} handler for
     * every restore frame that matches one of this instance's Segments — so the
     * gate advances even when an individual apply fails (the frame landed
     * regardless), and never stalls on a corrupt record.
     */
    public void noteRestored() {
        restoreGate.countDown();
    }

    /**
     * Record the shipped-back saga compensation markers for a Segment (crash-
     * resume): {@code intent} ⇒ its operation completed forward and may need undo;
     * {@code done} ⇒ its compensation already ran (skip it on resume).
     */
    public void noteCompensationMarkers(@Nonnull String segmentKey, boolean intent, boolean done) {
        if (intent) {
            compensationIntents.add(segmentKey);
        }
        if (done) {
            compensationDone.add(segmentKey);
        }
    }

    /** Whether this Segment completed forward and needs undo (drives which hooks a resume runs). */
    public boolean hasCompensationIntent(@Nonnull String segmentKey) {
        return compensationIntents.contains(segmentKey);
    }

    /** Whether this Segment's compensation already ran (skip on resume). */
    public boolean hasCompensationDone(@Nonnull String segmentKey) {
        return compensationDone.contains(segmentKey);
    }

    /**
     * Hold the calling driver until every expected Segment restore has landed, a
     * cooperative cancel (or thread interrupt) trips, or the safety-net timeout
     * elapses. A no-op when no restores are expected (fresh spawn / opted-out /
     * run-sugar), so it adds nothing to the non-resume path.
     */
    public void awaitRestore() {
        CountDownLatch gate = restoreGate;
        long deadlineNanos = System.nanoTime() + RESUME_WAIT_TIMEOUT_MS * 1_000_000L;
        try {
            while (gate.getCount() > 0) {
                if (isCancelled()) {
                    return;
                }
                long remainingMs = (deadlineNanos - System.nanoTime()) / 1_000_000L;
                if (remainingMs <= 0) {
                    LOG.warn(
                            "bounded {} crash-resume timed out waiting for {} Segment restore(s); "
                                    + "proceeding with partially-restored state",
                            instanceId,
                            gate.getCount());
                    return;
                }
                if (gate.await(Math.min(remainingMs, RESUME_WAIT_CHUNK_MS), TimeUnit.MILLISECONDS)) {
                    return;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
