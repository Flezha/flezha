package com.flezha.sdk.operative;

import java.util.List;
import java.util.Objects;

/**
 * Thrown by {@link ContinuousOperativeRegistry#registerContinuous(Class)} when a ContinuousOperative's
 * declared topology violates a wiring constraint (entry-channel-type,
 * type-equality between channel payload and producer/consumer Operation,
 * unwired channel, multi-producer, static-mode default-impl detection).
 *
 * <p>Carries channel name and expected/actual type info so the executor
 * can diagnose the misconfiguration without re-running with a debugger.
 */
public class ContinuousOperativeConfigurationException extends RuntimeException {

    private final String channel;
    private final Class<?> expected;
    private final Class<?> actual;

    public ContinuousOperativeConfigurationException(String message) {
        super(message);
        this.channel = null;
        this.expected = null;
        this.actual = null;
    }

    public ContinuousOperativeConfigurationException(String channel, Class<?> expected, Class<?> actual, String message) {
        super(message);
        this.channel = channel;
        this.expected = expected;
        this.actual = actual;
    }

    public ContinuousOperativeConfigurationException(List<String> errors) {
        super("operative topology validation failed: " + Objects.requireNonNull(errors));
        this.channel = null;
        this.expected = null;
        this.actual = null;
    }

    /**
     * Factory for the v1 "this Clock isn't wired yet" rejection. Operations
     * declaring {@code LogicalClock} or {@code EventTimeFrom} fail ContinuousOperative
     * registration with this exception until a follow-up wires them.
     */
    public static ContinuousOperativeConfigurationException clockNotImplementedYet(String clockTypeName) {
        return new ContinuousOperativeConfigurationException("Clock not implemented yet: " + clockTypeName);
    }

    /**
     * Factory raised at {@code registerContinuous} time when a channel is declared
     * with {@code alsoToPluginWith(...)} more than once. Only one plugin sink
     * per channel is allowed; the first call registers it, the second fails.
     */
    public static ContinuousOperativeConfigurationException duplicatePluginSink(String channel) {
        return new ContinuousOperativeConfigurationException("channel `" + channel + "` was declared with `alsoToPluginWith(...)` "
                + "more than once; only one plugin sink per channel is allowed");
    }

    /**
     * Factory raised when a Operation's resolved {@code mailboxCapacity()} (after
     * applying the {@code FLEZHA_MAILBOX_CAPACITY} env fallback) is zero.
     * Capacity must be in {@code 1..=1_048_576}.
     */
    public static ContinuousOperativeConfigurationException mailboxCapacityZero(String operation) {
        return new ContinuousOperativeConfigurationException(
                "operation `" + operation + "` declared mailboxCapacity = 0; capacity must be in 1..=1_048_576");
    }

    /**
     * Factory raised when a Operation's resolved {@code mailboxCapacity()} is
     * above {@code 1_048_576}. Capacity must be in {@code 1..=1_048_576}.
     */
    public static ContinuousOperativeConfigurationException mailboxCapacityTooLarge(String operation, long value) {
        return new ContinuousOperativeConfigurationException(
                "operation `" + operation + "` declared mailboxCapacity = " + value + "; capacity must be in 1..=1_048_576");
    }

    public String channel() {
        return channel;
    }

    public Class<?> expected() {
        return expected;
    }

    public Class<?> actual() {
        return actual;
    }
}
