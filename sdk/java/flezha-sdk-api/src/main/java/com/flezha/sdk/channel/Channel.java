package com.flezha.sdk.channel;

/**
 * The finalized channel handle returned by {@link Stream#sealed()}: a
 * phantom-typed marker exposing no builder methods, so a consumer or a
 * transform declared after it does not type-check.
 *
 * <p>It carries no state, which is what makes "sealing changes no topology"
 * true by construction rather than by convention — the handle cannot reach the
 * builder to record anything, not even the output channel a leaf node
 * deliberately does not have.
 */
public final class Channel<T> {}
