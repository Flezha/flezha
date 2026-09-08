package com.flezha.sdk;

import java.util.Locale;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * The four diagnostic levels every Flezha SDK carries, least severe first.
 * {@link Enum#compareTo} is the severity order, so a sink filters with it
 * rather than with a table of its own.
 */
public enum LogLevel {
    DEBUG,
    INFO,
    WARN,
    ERROR;

    /**
     * The level named by {@code name}, case-insensitively, or {@link #WARN}
     * when it names none: the default the other five SDKs share.
     */
    @Nonnull
    public static LogLevel from(@Nullable String name) {
        return from(name, WARN);
    }

    /**
     * The level named by {@code name}, case-insensitively. Unset, blank and
     * unrecognised all read as {@code fallback}, the same rule
     * {@code FlezhaEnv.parsePositive} applies to its own knob, so a name
     * nobody meant to set never silences a runner.
     */
    @Nonnull
    public static LogLevel from(@Nullable String name, LogLevel fallback) {
        if (name == null || name.isBlank()) {
            return fallback;
        }
        try {
            return valueOf(name.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            return fallback;
        }
    }
}
