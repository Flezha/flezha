package com.flezha.sdk.state;

/**
 * The default {@link Clock}: {@code ctx.now()} serves epoch milliseconds
 * sampled once at dispatch entry, before {@code run} executes.
 */
public final class WallClock implements Clock {}
