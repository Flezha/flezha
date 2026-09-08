package com.flezha.sdk;

import javax.annotation.Nonnull;

/**
 * The default {@link Logger}: writes {@code [flezha] <message>} to standard
 * error at or above its level. The level comes from {@code FLEZHA_LOG_LEVEL},
 * spelled and defaulted as in the other five SDKs.
 *
 * <p>Standard error is read at every write rather than captured, so a host that
 * replaces {@code System.err} is honoured.
 */
public final class ConsoleLogger implements Logger {

    /** Selects the default sink's level. {@code debug} | {@code info} | {@code warn} | {@code error}. */
    public static final String FLEZHA_LOG_LEVEL = "FLEZHA_LOG_LEVEL";

    private static final String PREFIX = "[flezha] ";

    private final LogLevel level;

    public ConsoleLogger(LogLevel level) {
        this.level = level;
    }

    /** The sink the SDK starts with, at the level {@code FLEZHA_LOG_LEVEL} names. */
    @Nonnull
    public static ConsoleLogger fromEnv() {
        return new ConsoleLogger(LogLevel.from(System.getenv(FLEZHA_LOG_LEVEL)));
    }

    @Override
    public void log(LogLevel at, String format, Object... args) {
        if (at.compareTo(level) < 0) {
            return;
        }
        try {
            System.err.println(PREFIX + Logger.render(format, args));
        } catch (Throwable ignored) {
            // A logger must never take the process down. The case that reaches
            // here is a standard error the host has replaced or closed during
            // shutdown, where an unhandled failure would turn a clean drain
            // into a crash.
        }
    }
}
