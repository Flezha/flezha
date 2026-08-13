package com.flezha.sdk.procedure;

import java.time.Duration;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Per-invocation context for a {@link Procedure#run} call. Borrows the runner's
 * per-Entity routing tables; not safe to retain past the call.
 *
 * <p>The dispatcher caches the producer-edge {@code outputChannelId} on the
 * {@link ProcedureCtx} instance at operative-build time so {@link #emit} pays no
 * channel-name lookup on the hot path.
 */
public interface ProcedureCtx {

    /**
     * Reserved metadata namespace: the runtime's identity stamps
     * ({@code flz.entity}, {@code flz.seq}, {@code flz.key}) live here.
     * User-supplied keys with this prefix are rejected at emit time so a stamp
     * can never be spoofed or clobbered.
     */
    String RESERVED_METADATA_PREFIX = "flz.";

    /**
     * The invocation's time value, computed <b>once</b> by the runtime from the
     * Procedure's declared {@code Clock} before {@code run} executes: epoch
     * milliseconds for {@code WallClock}, the extracted input timestamp for
     * {@code EventTimeFrom}, the durable change seq for {@code LogicalClock}.
     * Every call within one invocation returns the same value.
     *
     * <p>This is the determinism-contract replacement for
     * {@code System.currentTimeMillis()}: a run that reads ambient time cannot
     * be replayed, because the replay would compute different outputs from the
     * same inputs.
     */
    long now();

    /**
     * A deterministic, replay-stable 128-bit identifier. Repeated calls within
     * one invocation differ; the n-th call of a replayed invocation equals the
     * n-th call of the original.
     *
     * <p>This is the determinism-contract replacement for
     * {@code UUID.randomUUID()}.
     */
    @Nonnull
    StableId stableId();

    /**
     * Close the current checkpoint interval: acknowledge every input consumed
     * since the last checkpoint, and make the state accumulated across them
     * durable — as one record.
     *
     * <p>Call it where the accumulated state is genuinely coherent (a closed
     * window, a completed batch). The interval you leave open is the work a
     * crash replays and the work the controller's input log retains.
     *
     * <p>A no-op unless the Operative declared {@code checkpointAck()}.
     */
    void checkpoint();

    /**
     * Ask the controller for a workflow instance: start {@code factoryId} under
     * {@code instanceId} if that id is free, otherwise apply {@code ifRunning}
     * ({@link IfRunning#DELIVER} feeds the running instance,
     * {@link IfRunning#ERROR} reports {@link SpawnException.AlreadyRunning}).
     * {@code payload} is the instance's first input on a spawn, one more input
     * on a delivery.
     *
     * <p>Returns immediately. The request cannot fault this procedure: a
     * refusal, an unreachable controller, or an unknown factory surfaces only
     * through the returned {@link SpawnHandle} — or, if the handle is never
     * inspected, as one warning line.
     *
     * <p>Concurrent requests for one {@code instanceId} race-resolve
     * controller-side: exactly one spawns and every other payload is delivered
     * to it.
     *
     * <p>Spawning is a side effect of <b>live</b> processing. Replayed input — a
     * crash-resume or a late-input correction re-running this invocation —
     * issues no request and answers {@link SpawnOutcome#SKIPPED_ON_REPLAY}, so a
     * replay never doubles the workers a run created.
     *
     * <p>The timer calls on this same context behave the OPPOSITE way and
     * deliberately so: a timer is internal state, so a replay registers it,
     * where skipping would rebuild a pending set the live run never had.
     */
    @Nonnull
    default SpawnHandle spawnWorkflow(
            @Nonnull String instanceId,
            @Nonnull String factoryId,
            @Nonnull byte[] payload,
            @Nonnull IfRunning ifRunning) {
        return SpawnRequests.request(instanceId, factoryId, payload, ifRunning, replaying());
    }

    /**
     * Whether this invocation is re-running already-processed input. Runtime
     * detail: the dispatcher overrides it, and it is what makes
     * {@link #spawnWorkflow} a live-processing-only side effect.
     */
    default boolean replaying() {
        return false;
    }

    /**
     * Register an <b>event-time</b> timer at an absolute timestamp, returning
     * that timestamp. It fires when this Entity's event-time high-water reaches
     * it — so an Entity that stops receiving input never fires one, however much
     * wall-clock time passes. That is inherent to event time;
     * {@link #timerAfter} is the answer to "nothing has arrived".
     *
     * <p>Refused with
     * {@link ProcedureException.EventTimeTimerWithoutEventTimeClock} unless the
     * Procedure declares {@code EventTimeFrom} as its Clock.
     *
     * <p>Registering the same timestamp twice is idempotent — pending timers are
     * a set. Handle the firing in {@link Procedure#onTimer}.
     */
    default long timerAtEventTime(long scheduledAt) throws ProcedureException {
        throw new ProcedureException.TimersUnavailable();
    }

    /**
     * Register a <b>processing-time</b> timer {@code delay} after {@link #now},
     * returning the absolute timestamp it is scheduled at.
     *
     * <p>The anchor is {@code ctx.now()} — the declared Clock — not an ambient
     * read, which is what makes a replayed invocation register the same absolute
     * timestamp the live one did. Under an {@code EventTimeFrom} clock the
     * anchor is therefore the input's event time: a stream that is caught up
     * schedules the same instant a wall clock would, and one replaying history
     * schedules a past instant, which fires at once.
     */
    default long timerAfter(@Nonnull Duration delay) throws ProcedureException {
        throw new ProcedureException.TimersUnavailable();
    }

    /**
     * Cancel a pending event-time timer by the timestamp
     * {@link #timerAtEventTime} returned. {@code false} if none was pending.
     */
    default boolean cancelEventTimeTimer(long scheduledAt) throws ProcedureException {
        throw new ProcedureException.TimersUnavailable();
    }

    /**
     * Cancel a pending processing-time timer by the timestamp
     * {@link #timerAfter} returned. {@code false} if none was pending.
     */
    default boolean cancelProcessingTimeTimer(long scheduledAt) throws ProcedureException {
        throw new ProcedureException.TimersUnavailable();
    }

    /**
     * Emit on a channel declared {@code .keyed()}, supplying the output's
     * natural key. The runtime stamps {@code flz.key} (alongside
     * {@code flz.entity} and {@code flz.seq}) on the plugin frame, which is
     * what lets a keyed sink absorb a replayed emit as an idempotent upsert
     * rather than a duplicate row.
     *
     * <p>The key must itself be replay-stable by the determinism contract —
     * derived from the input, the state, or {@code ctx}, never from an ambient
     * source. Emitting a key on a channel not declared {@code .keyed()} throws
     * {@link ProcedureException.KeyNotAllowedOnNonKeyedChannel}.
     */
    <C> void emitKeyed(@Nonnull String key, @Nonnull C value) throws ProcedureException;

    /** {@link #emitKeyed} with user-attached metadata. */
    <C> void emitKeyedWithMeta(@Nonnull String key, @Nonnull C value, @Nonnull Map<String, String> meta)
            throws ProcedureException;

    /**
     * Emit a typed value on this Procedure's declared output channel. The channel
     * id was cached at {@code registerStream} time from
     * {@code c.channel(T.class, "name").from(S.class)} wiring, so this call
     * performs no name lookups.
     *
     * <p>Inter-procedure channels: the value is passed by reference to consumer
     * Procedures; the user MUST treat it as immutable from the producer side
     * after this call returns (see {@code design.md} Decision 4).
     *
     * <p>Plugin-bound channels: the value is serialized synchronously via the
     * channel's serializer and queued for the wire with
     * {@code metadata = {"channel": "<name>"}}.
     */
    <C> void emit(@Nonnull C value) throws ProcedureException;

    /**
     * Same as {@link #emit(Object)} but pinned to a specific channel name —
     * for Procedures that produce on multiple plugin-bound channels.
     */
    <C> void emit(@Nonnull String channel, @Nonnull C value) throws ProcedureException;

    /**
     * Emit with user-attached string-keyed metadata. Valid only on plugin-bound
     * channels; inter-procedure emits throw
     * {@link ProcedureException.MetadataNotAllowedOnInterProcedureChannel} naming the
     * channel.
     *
     * <p>The reserved {@code channel} key is dropped from the user map and
     * overwritten with the channel's actual name during framing. Keys in the
     * {@link #RESERVED_METADATA_PREFIX} namespace are rejected with
     * {@link ProcedureException.ReservedMetadataKey}.
     */
    <C> void emitWithMeta(@Nonnull C value, @Nonnull Map<String, String> meta) throws ProcedureException;

    /** Same as {@link #emitWithMeta(Object, Map)} but pinned to a specific channel name. */
    <C> void emitWithMeta(@Nonnull String channel, @Nonnull C value, @Nonnull Map<String, String> meta)
            throws ProcedureException;
}
