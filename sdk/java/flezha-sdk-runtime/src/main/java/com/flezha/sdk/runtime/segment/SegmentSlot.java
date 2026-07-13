package com.flezha.sdk.runtime.segment;

import com.flezha.sdk.channel.OverflowPolicy;
import com.flezha.sdk.runtime.state.EffectRecord;
import com.flezha.sdk.runtime.worker.OperationDispatcher;
import com.flezha.sdk.state.Change;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nonnull;

/**
 * Per-(operation, segment) state + mailbox + dispatcher. The {@code state} field
 * is type-erased ({@code AtomicReference<Object>}) because the topology builder
 * wires Operations by reflective {@link Class}; the dispatcher knows the concrete
 * Operation / State / Input / Output types and casts inside the drain loop.
 *
 * <p>The change-buffer fields below back the per-Segment flusher's wire
 * frame construction. They are populated only when the Operation's {@code Change}
 * is non-{@code NoChange}; for {@code NoChange} Operations the buffer remains
 * empty and the flusher skips registration entirely.
 */
public final class SegmentSlot {

    private final ReentrantLock stateLock = new ReentrantLock();
    private final AtomicReference<Object> state = new AtomicReference<>();
    private final SegmentMailbox<Object> mailbox;
    private final OperationDispatcher dispatcher;
    private final int operationId;
    private final Optional<String> operationKey;
    private final boolean changeful;
    private final Object changeBufferLock = new Object();
    private final List<Change> changeBuffer = new ArrayList<>();
    private final AtomicLong changeSeq = new AtomicLong(0L);
    private final List<EffectRecord> effectBuffer = new ArrayList<>();
    private final List<byte[]> stagedChanges = new ArrayList<>();

    /** Max event time this Segment has ever observed. NEVER lowered — see {@link #observeEventTime}. */
    private final AtomicLong eventTimeHighWater = new AtomicLong(Long.MIN_VALUE);

    private final List<PendingInput> replayBuffer = new ArrayList<>();
    private boolean replaySliceOpen;

    /** Wall-clock ms of the last dispatch, for the dynamic-segment idle reaper. */
    private final AtomicLong lastTouchedMs = new AtomicLong(System.currentTimeMillis());

    public SegmentSlot(
            @Nonnull OperationDispatcher dispatcher, int operationId, @Nonnull Optional<String> operationKey) {
        this(dispatcher, operationId, operationKey, false, 1024, OverflowPolicy.BLOCK);
    }

    public SegmentSlot(
            @Nonnull OperationDispatcher dispatcher,
            int operationId,
            @Nonnull Optional<String> operationKey,
            boolean changeful) {
        this(dispatcher, operationId, operationKey, changeful, 1024, OverflowPolicy.BLOCK);
    }

    public SegmentSlot(
            @Nonnull OperationDispatcher dispatcher,
            int operationId,
            @Nonnull Optional<String> operationKey,
            boolean changeful,
            int mailboxCapacity,
            @Nonnull OverflowPolicy overflowPolicy) {
        this.dispatcher = dispatcher;
        this.operationId = operationId;
        this.operationKey = operationKey;
        this.changeful = changeful;
        this.mailbox = new SegmentMailbox<>(mailboxCapacity, overflowPolicy);
    }

    @Nonnull
    public ReentrantLock stateLock() {
        return stateLock;
    }

    @Nonnull
    public AtomicReference<Object> state() {
        return state;
    }

    @Nonnull
    public SegmentMailbox<Object> mailbox() {
        return mailbox;
    }

    @Nonnull
    public OperationDispatcher dispatcher() {
        return dispatcher;
    }

    public int operationId() {
        return operationId;
    }

    @Nonnull
    public Optional<String> operationKey() {
        return operationKey;
    }

    /** Whether this Operation declares a non-{@code NoChange} Change type. */
    public boolean changeful() {
        return changeful;
    }

    /** Lock the flusher acquires before draining the buffer. */
    @Nonnull
    public Object changeBufferLock() {
        return changeBufferLock;
    }

    /** Mutable per-Segment Change buffer; the flusher drains and serializes. */
    @Nonnull
    public List<Change> changeBuffer() {
        return changeBuffer;
    }

    /**
     * Mutable per-Segment effect buffer (effectively-once tier): one
     * {@link EffectRecord} per invocation, drained uncoalesced under the same
     * {@link #changeBufferLock()}. Empty on a non-tier Segment, which drains
     * {@link #changeBuffer()} instead.
     */
    @Nonnull
    public List<EffectRecord> effectBuffer() {
        return effectBuffer;
    }

    /**
     * The OPEN checkpoint interval's changes (checkpoint ack-granularity only):
     * serialized changes accumulated since the last {@code ctx.checkpoint()}.
     *
     * <p>Deliberately NOT drained by the flusher. An open interval is not
     * flushable — shipping it alone would put the state ahead of the offset that
     * has not yet been acknowledged, which is the exact tear this mode exists to
     * prevent. It ships only when an invocation closes the interval, and a
     * restore discards it (it belongs to a past that no longer exists).
     */
    @Nonnull
    public List<byte[]> stagedChanges() {
        return stagedChanges;
    }

    /** Monotonic per-Segment sequence number applied to flushed Change frames. */
    @Nonnull
    public AtomicLong changeSeq() {
        return changeSeq;
    }

    /**
     * One input held while a correction replay slice is open: either a member of
     * the slice (re-ordered by event time) or a live input that arrived behind it
     * (kept in arrival order, dispatched after the slice).
     */
    public record PendingInput(
            Object input, long primaryStreamId, OptionalLong offset, long eventTime, boolean replay) {}

    /**
     * Fold {@code eventTime} into the high-water mark and return the mark.
     *
     * <p>Monotonic by construction: a replay's inputs can carry event times lower
     * than the recovered state's, and lowering the mark would make a genuinely
     * late input read as on-time — silently skipping the correction it needed.
     */
    public long observeEventTime(long eventTime) {
        return eventTimeHighWater.accumulateAndGet(eventTime, Math::max);
    }

    /** The current event-time high-water, or {@link Long#MIN_VALUE} if nothing was seen. */
    public long eventTimeHighWater() {
        return eventTimeHighWater.get();
    }

    /**
     * Hold an input while a correction replay slice is open, opening the slice if
     * {@code p} is its first member.
     *
     * <p>From the first slice member until {@code flz.replay_end}, EVERY input is
     * held — including live ones, which must not jump ahead of the very inputs
     * being re-ordered.
     *
     * @return {@code true} if the input was buffered (the caller must not dispatch it)
     */
    public boolean bufferIfSliceOpen(@Nonnull PendingInput p) {
        synchronized (replayBuffer) {
            if (p.replay()) {
                replaySliceOpen = true;
            }
            if (!replaySliceOpen) {
                return false;
            }
            replayBuffer.add(p);
            return true;
        }
    }

    /**
     * Close the slice and return its inputs in dispatch order: slice members in
     * <b>event-time order</b> — the whole point of a correction — then the live
     * inputs that arrived behind them, in arrival order.
     *
     * <p>That ordering is load-bearing: a live input released ahead of the slice
     * would jump in front of the inputs being re-ordered, and the corrected state
     * would be wrong in a way nothing detects.
     *
     * @return the ordered inputs, or empty if no slice was open
     */
    @Nonnull
    public List<PendingInput> closeReplaySlice() {
        List<PendingInput> pending;
        synchronized (replayBuffer) {
            if (!replaySliceOpen) {
                return List.of();
            }
            pending = new ArrayList<>(replayBuffer);
            replayBuffer.clear();
            replaySliceOpen = false;
        }
        List<PendingInput> ordered = new ArrayList<>(pending.size());
        pending.stream()
                .filter(PendingInput::replay)
                .sorted(java.util.Comparator.comparingLong(PendingInput::eventTime))
                .forEach(ordered::add);
        pending.stream().filter(p -> !p.replay()).forEach(ordered::add);
        return ordered;
    }

    /** Mark this Segment as active; resets its idle clock. */
    public void touch() {
        lastTouchedMs.set(System.currentTimeMillis());
    }

    public long idleMs() {
        return System.currentTimeMillis() - lastTouchedMs.get();
    }
}
