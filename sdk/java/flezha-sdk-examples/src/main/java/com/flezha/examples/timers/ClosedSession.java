package com.flezha.examples.timers;

/** What a closed session reports downstream. */
public record ClosedSession(String user, int clicks, long closedAt) {}
