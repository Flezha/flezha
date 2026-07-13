package com.flezha.sdk.state;

/**
 * {@link Clock} serving the event time carried by the input itself: an
 * Operation declaring it MUST override {@code Operation.eventTime(Input)},
 * and {@code ctx.now()} returns what that extractor reports.
 *
 * <p>Rust binds the extractor at compile time
 * ({@code EventTimeFrom<F: TimeExtractor<Input>>}); Java's erasure precludes
 * that, so a missing override fails Operative registration instead.
 *
 * <p>This is the clock the unbounded late-input correction rests on — the
 * event time it reports is what the controller maps back to an input offset.
 */
public final class EventTimeFrom implements Clock {}
