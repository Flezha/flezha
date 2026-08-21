package com.flezha.sdk.channel;

import com.flezha.sdk.procedure.Procedure;
import java.util.function.Function;
import java.util.function.Predicate;
import javax.annotation.Nonnull;

/**
 * The surface every stream handle carries, declared once so the entry-side
 * ({@link EntryChannel}) and producer-side ({@link Stream}) handles cannot
 * drift: wiring a consumer, reshaping and keying the values on the way, and the
 * consumer-side channel configuration.
 *
 * <p><b>Transforms are edge closures, not Procedures.</b> {@link #map},
 * {@link #filter} and {@link #flatMap} add no node, no Entity, no mailbox and
 * no dispatch hop — every transform declared before the closing {@code into}
 * is composed into ONE closure stored on that consumer edge and evaluated at
 * the routing seam. Fusion is therefore structural: there is no optimizer to
 * second-guess and no directive to disable chaining, because {@code into} IS
 * the boundary.
 *
 * <p>Because they run at the routing seam, edge closures MUST be:
 *
 * <ul>
 *   <li><b>pure</b> — they re-run on WAL replay, and a closure that read
 *       ambient state would rebuild a state the live run never had;
 *   <li><b>cheap</b> — they run on the routing thread, so real work belongs in
 *       a Procedure behind {@link #keyBy}, which buys per-Entity parallelism.
 * </ul>
 *
 * <p>A closure that throws does not take the routing thread down: the value
 * climbs the channel's retry→DLQ ladder, the same one a failing entry parse
 * rides.
 *
 * <p>{@link #map} and {@link #flatMap} take the post-transform type as an
 * explicit {@code Class}, matching the {@code input(Class, String)}
 * convention, because erasure cannot recover it from a lambda and the runtime
 * compares declared types when wiring a consumer. {@link #filter} and
 * {@link #keyBy} need no token — the payload type is unchanged and a key is a
 * {@code String}.
 */
public interface EdgeStream<T> {

    /**
     * Wire a consumer procedure onto this stream's channel and return
     * <b>the consumer's</b> output stream handle, so a linear pipeline is one
     * chained expression reading in flow order. The runtime verifies at
     * registration that the procedure's {@code inputType()} equals this
     * stream's payload type — as reshaped by any transforms declared above —
     * and fails registration naming both sides on a mismatch.
     *
     * <p>Calling {@code .into} on the same held handle repeatedly declares
     * fan-out; wiring the same unnamed class from two streams declares fan-in.
     */
    @Nonnull
    <O> Stream<O> into(@Nonnull Class<? extends Procedure<T, ?, O, ?, ?, ?>> procedure);

    /**
     * Like {@link #into(Class)}, but the consumer is a distinct <b>named
     * node</b>: its identity string is {@code nodeName} instead of the
     * procedure's {@code name()}, for every purpose identity serves — per-entity
     * state directories, journal entries, the derived channel name,
     * diagnostics. This lets one Procedure class appear at several graph
     * positions with independent state. Node identity strings are unique per
     * operative; a collision fails registration.
     */
    @Nonnull
    <O> Stream<O> into(@Nonnull Class<? extends Procedure<T, ?, O, ?, ?, ?>> procedure, @Nonnull String nodeName);

    /** Reshape every value on the way to the next consumer. */
    @Nonnull
    <U> EdgeStream<U> map(@Nonnull Class<U> type, @Nonnull Function<T, U> mapper);

    /**
     * Expand one value into zero or more. Each yielded value is keyed and
     * delivered independently, so {@code flatMap} plus {@link #keyBy} is how
     * one input fans out across several Entities.
     *
     * <p>The yielded values are pushed one at a time under the channel's
     * {@link #onFull} policy, so a drop policy may accept some of a fan-out
     * and drop the rest — the same shape multi-consumer fan-out already has.
     */
    @Nonnull
    <U> EdgeStream<U> flatMap(@Nonnull Class<U> type, @Nonnull Function<T, ? extends Iterable<U>> mapper);

    /**
     * Drop the values the predicate rejects. A surviving value is forwarded by
     * reference — a filtered stream costs one predicate call per input and
     * nothing else.
     */
    @Nonnull
    EdgeStream<T> filter(@Nonnull Predicate<T> predicate);

    /**
     * Name the Entity each value is routed to, making the consumer a
     * dynamic-mode Procedure whose Entities are created on first sight of a
     * key. This is the only way to declare dynamic keying — the consumer
     * Procedure carries no keying member at all, so a reader of
     * {@code channels()} can see where work fans out.
     *
     * <p>Total by signature: it names an Entity, it never declines. Discard
     * inputs with {@link #filter} before it. It is evaluated on the
     * <b>post-transform</b> value, so it must be the last link before
     * {@code into}. Wiring it onto a Procedure declaring {@code entityKeys()}
     * (static mode) fails registration.
     */
    @Nonnull
    EdgeStream<T> keyBy(@Nonnull Function<T, String> key);

    /**
     * Override the default {@link OverflowPolicy}, which is
     * {@link OverflowPolicy#BLOCK} for every channel kind. On an entry channel
     * {@code BLOCK} parks the shared wire reader on this operative's
     * saturation; declare a drop policy when shedding under saturation is
     * acceptable and ordering or completeness can be sacrificed for liveness.
     */
    @Nonnull
    EdgeStream<T> onFull(@Nonnull OverflowPolicy policy);

    /**
     * Override the consumer-side retry budget (total attempts, default 3)
     * applied before a failing payload is parked to the dead-letter queue.
     */
    @Nonnull
    EdgeStream<T> retryMax(int maxAttempts);

    /**
     * Override the exponential-backoff base (milliseconds, default 50)
     * slept between failed attempts.
     */
    @Nonnull
    EdgeStream<T> retryBackoffMs(long baseMs);
}
