/**
 * Repairing an already-published result when an input arrives after its interval closed.
 *
 * <p>Trades carry their own timestamps, and one occasionally arrives minutes after the candle it
 * belongs to was computed and emitted. The alternatives are to discard it, which loses data, or to
 * treat it as current, which corrupts the interval it lands in. Neither is satisfactory, so the
 * runtime instead replays the affected entity with the late message in its correct position and
 * re-emits the corrected result.
 *
 * <p>Two declarations make this possible. The clock is
 * {@link com.flezha.sdk.state.EventTimeFrom}, so {@code ctx.now()} returns the message's own
 * timestamp rather than processing time, which is what gives "late" a meaning and what places a
 * replayed message in the interval it always belonged to. Java's erasure cannot bind the extractor
 * from the type argument, so a procedure declaring this clock must also override {@code eventTime};
 * failing to do so is refused at registration rather than at run time.
 *
 * <p>The emit is keyed by the interval bucket, so the corrected candle overwrites the stale record
 * rather than appending a second one. This is the same keyed-emit mechanism the
 * {@code effectivelyonce} example relies on, applied to a different problem.
 *
 * <p>{@code correctionWindow} bounds how far back the runtime will replay, and it is not free: the
 * window sizes the controller's input-log retention, so it should reflect the lateness actually
 * expected. {@link com.flezha.sdk.channel.BeyondWindowPolicy} governs a message older than the
 * window, treated here as a fresh arrival.
 */
package com.flezha.examples.corrections;
