package com.flezha.sdk.state;

/**
 * {@link Clock} serving the Segment's durable change seq, re-armed to
 * {@code terminal + 1} on restore — so a replayed invocation observes the same
 * value it did originally.
 *
 * <p>Requires a non-{@code NoChange} Change: a stateless Operation has no
 * durable seq to serve, and declaring this clock without one fails Operative
 * registration with {@code UnservableClock}.
 */
public final class LogicalClock implements Clock {}
