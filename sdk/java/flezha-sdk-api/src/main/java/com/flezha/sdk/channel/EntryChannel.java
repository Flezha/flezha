package com.flezha.sdk.channel;

import com.flezha.sdk.procedure.Procedure;
import javax.annotation.Nonnull;

/**
 * The entry channel's handle: wires the graph's root consumer via
 * {@link #into(Class)} and carries the entry-side channel configuration.
 * Entry channels are fed by the controller, so there is no producer side.
 *
 * <p>Like {@link Stream}, this handle is only usable inside
 * {@code channels()} / {@code procedures()}: using a retained handle after
 * the topology froze at registration throws {@link IllegalStateException}
 * naming the operative.
 */
public interface EntryChannel<T> {

    /**
     * Wire the entry consumer and return <b>its</b> output {@link Stream}
     * handle, so a linear pipeline is one chained expression. The runtime
     * verifies at registration that the procedure's {@code inputType()}
     * equals {@code T}; a mismatch fails registration naming both the
     * channel and both types.
     */
    @Nonnull
    <O> Stream<O> into(@Nonnull Class<? extends Procedure<T, ?, O, ?, ?, ?>> procedure);

    /**
     * Like {@link #into(Class)}, but the consumer is a distinct named node
     * whose identity string is {@code nodeName} instead of the procedure's
     * {@code name()} — see {@link Stream#into(Class, String)}.
     */
    @Nonnull
    <O> Stream<O> into(@Nonnull Class<? extends Procedure<T, ?, O, ?, ?, ?>> procedure, @Nonnull String nodeName);

    /**
     * Override the per-kind default {@link OverflowPolicy}. Entry channels
     * default to {@link OverflowPolicy#DROP_NEWEST}; declare
     * {@link OverflowPolicy#BLOCK} here only if you accept stalling the
     * shared wire reader on this UnboundedOperative's saturation.
     */
    @Nonnull
    EntryChannel<T> onFull(@Nonnull OverflowPolicy policy);

    /**
     * Override the consumer-side retry budget (total attempts, default 3)
     * applied before a failing payload is parked to the dead-letter queue.
     */
    @Nonnull
    EntryChannel<T> retryMax(int maxAttempts);

    /**
     * Override the exponential-backoff base (milliseconds, default 50)
     * slept between failed attempts.
     */
    @Nonnull
    EntryChannel<T> retryBackoffMs(long baseMs);
}
