package com.flezha.sdk.state;

/**
 * Sealed, type-level clock declaration: an Operation names one via its
 * {@code Clock} type parameter, and the <b>runtime</b> evaluates it once per
 * invocation and serves the value from {@code ctx.now()}.
 *
 * <p>These are markers, not clocks you call. That is the whole point of the
 * determinism contract: an Operation must not be able to read time itself, or a
 * replay would compute different outputs from the same inputs. The only way to
 * observe time inside {@code run} is {@code ctx.now()}.
 *
 * <ul>
 *   <li>{@link WallClock} (default) — epoch milliseconds sampled at dispatch entry.
 *   <li>{@link EventTimeFrom} — the event time the Operation's
 *       {@code eventTime(Input)} extractor reports for the current input.
 *   <li>{@link LogicalClock} — the durable change seq, re-armed to
 *       {@code terminal + 1} on restore.
 * </ul>
 */
public sealed interface Clock permits WallClock, LogicalClock, EventTimeFrom {}
