package com.flezha.sdk.runtime.entity;

import com.flezha.sdk.channel.OverflowPolicy;
import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.runtime.state.EffectRecord;
import com.flezha.sdk.runtime.worker.ProcedureDispatcher;
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
 * Per-(procedure, entity) state + mailbox + dispatcher. The {@code state} field
 * is type-erased ({@code AtomicReference<Object>}) because the topology builder
 * wires Procedures by reflective {@link Class}; the dispatcher knows the concrete
 * Procedure / State / Input / Output types and casts inside the drain loop.
 *
 * <p>The change-buffer fields below back the per-Entity flusher's wire
 * frame construction. They are populated only when the Procedure's {@code Change}
 * is non-{@code NoChange}; for {@code NoChange} Procedures the buffer remains
 * empty and the flusher skips registration entirely.
 */
public final class EntitySlot {

    private final ReentrantLock stateLock = new ReentrantLock();
    private final AtomicReference<Object> state = new AtomicReference<>();
    private final EntityMailbox<Object> mailbox;
    private final ProcedureDispatcher dispatcher;

    /** This Entity's own Procedure instance — see {@link #procedureInstance()}. */
    @SuppressWarnings("rawtypes")
    private final Procedure procedureInstance;

    private final int procedureId;
    private final Optional<String> entityKey;
    private final boolean changeful;
    private final Object changeBufferLock = new Object();
    private final List<Change> changeBuffer = new ArrayList<>();
    private final AtomicLong changeSeq = new AtomicLong(0L);
    private final List<EffectRecord> effectBuffer = new ArrayList<>();
    private final List<byte[]> stagedChanges = new ArrayList<>();

    /** Max event time this Entity has ever observed. NEVER lowered — see {@link #observeEventTime}. */
    private final AtomicLong eventTimeHighWater = new AtomicLong(Long.MIN_VALUE);

    private final List<PendingInput> replayBuffer = new ArrayList<>();
    private boolean replaySliceOpen;

    /** Wall-clock ms of the last dispatch, for the dynamic-entity idle reaper. */
    private final AtomicLong lastTouchedMs = new AtomicLong(System.currentTimeMillis());

    public EntitySlot(
            @Nonnull ProcedureDispatcher dispatcher,
            @Nonnull Procedure<?, ?, ?, ?, ?, ?> procedureInstance,
            int procedureId,
            @Nonnull Optional<String> entityKey,
            boolean changeful,
            int mailboxCapacity,
            @Nonnull OverflowPolicy overflowPolicy) {
        this.dispatcher = dispatcher;
        this.procedureInstance = procedureInstance;
        this.procedureId = procedureId;
        this.entityKey = entityKey;
        this.changeful = changeful;
        this.mailbox = new EntityMailbox<>(mailboxCapacity, overflowPolicy);
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
    public EntityMailbox<Object> mailbox() {
        return mailbox;
    }

    @Nonnull
    public ProcedureDispatcher dispatcher() {
        return dispatcher;
    }

    /**
     * This Entity's own {@link Procedure} instance. {@code run} and
     * {@code applyChange} are invoked on it, so a mutable field on a user's
     * Procedure is per-Entity scratch — never a shared, unsynchronised channel
     * across Entity keys. Mirrors the Rust SDK, which holds one Procedure per
     * Entity alongside that Entity's State.
     */
    @Nonnull
    @SuppressWarnings("rawtypes")
    public Procedure procedureInstance() {
        return procedureInstance;
    }

    public int procedureId() {
        return procedureId;
    }

    @Nonnull
    public Optional<String> entityKey() {
        return entityKey;
    }

    /** Whether this Procedure declares a non-{@code NoChange} Change type. */
    public boolean changeful() {
        return changeful;
    }

    /** Lock the flusher acquires before draining the buffer. */
    @Nonnull
    public Object changeBufferLock() {
        return changeBufferLock;
    }

    /** Mutable per-Entity Change buffer; the flusher drains and serializes. */
    @Nonnull
    public List<Change> changeBuffer() {
        return changeBuffer;
    }

    /**
     * Mutable per-Entity effect buffer (effectively-once tier): one
     * {@link EffectRecord} per invocation, drained uncoalesced under the same
     * {@link #changeBufferLock()}. Empty on a non-tier Entity, which drains
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

    /** Monotonic per-Entity sequence number applied to flushed Change frames. */
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
        List<PendingInput> pending = takeSlice();
        List<PendingInput> ordered = new ArrayList<>(pending.size());
        pending.stream()
                .filter(PendingInput::replay)
                .sorted(java.util.Comparator.comparingLong(PendingInput::eventTime))
                .forEach(ordered::add);
        pending.stream().filter(p -> !p.replay()).forEach(ordered::add);
        return ordered;
    }

    /**
     * Restore-time disposition of queued input: drop it only where the
     * controller will hand it back. It journals inputs before forwarding for
     * the effectively-once tier (input log) and for durable bounded instances
     * (proc inbox), and for nothing else — so a plain stateful Operative's
     * mailbox is the only copy its input has, and clearing it is data loss.
     * Where the log does exist, keeping the queue would double-apply it and,
     * during a correction, re-order it behind the replayed slice.
     *
     * @return the number of items discarded, counted into
     *     {@link EntityCounters#noteRestoreDiscarded(int)}
     */
    public int discardQueuedInputOnRestore() {
        if (!dispatcher.tier()) {
            return 0;
        }
        int discarded = mailbox.clear();
        EntityCounters.noteRestoreDiscarded(discarded);
        return discarded;
    }

    /**
     * Drop a half-filled correction slice: it belongs to a past the restore
     * replaced, and releasing it later would order its inputs behind the very
     * replay meant to re-order them. Unlike the mailbox this applies to every
     * class — a slice only ever exists for one the controller re-forwards.
     */
    public void discardReplaySlice() {
        takeSlice();
    }

    /**
     * Drop an open checkpoint interval's staged changes. A restore re-seats the
     * Entity on a past in which they were never applied, and the controller
     * re-forwards the inputs that produced them.
     */
    public void discardStagedInterval() {
        synchronized (changeBufferLock) {
            stagedChanges.clear();
        }
    }

    /** Close the slice and hand back its members, or empty if none was open. */
    private List<PendingInput> takeSlice() {
        synchronized (replayBuffer) {
            if (!replaySliceOpen) {
                return List.of();
            }
            List<PendingInput> pending = new ArrayList<>(replayBuffer);
            replayBuffer.clear();
            replaySliceOpen = false;
            return pending;
        }
    }

    /** Mark this Entity as active; resets its idle clock. */
    public void touch() {
        lastTouchedMs.set(System.currentTimeMillis());
    }

    public long idleMs() {
        return System.currentTimeMillis() - lastTouchedMs.get();
    }
}
