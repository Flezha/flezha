package com.flezha.sdk.state;

/**
 * {@link Clock} serving the event time carried by the input itself: an
 * Procedure declaring it MUST override {@code Procedure.eventTime(Input)},
 * and {@code ctx.now()} returns what that extractor reports.
 *
 * <p>Rust binds the extractor at compile time
 * ({@code EventTimeFrom<F: TimeExtractor<Input>>}); Java's erasure precludes
 * that, so a missing override fails Operative registration instead.
 *
 * <p>This is the clock the stream late-input correction rests on — the
 * event time it reports is what the controller maps back to an input offset.
 */
public final class EventTimeFrom implements Clock {}
