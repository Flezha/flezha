package com.flezha.sdk.relay;

import java.util.List;
import java.util.Objects;

/**
 * Thrown by {@link RelayRegistry#registerRelay(Class)} when a Relay's
 * declared topology violates a wiring constraint (entry-channel-type,
 * type-equality between channel payload and producer/consumer Shard,
 * unwired channel, multi-producer, static-mode default-impl detection).
 *
 * <p>Carries channel name and expected/actual type info so the executor
 * can diagnose the misconfiguration without re-running with a debugger.
 */
public class RelayConfigurationException extends RuntimeException {

    private final String channel;
    private final Class<?> expected;
    private final Class<?> actual;

    public RelayConfigurationException(String message) {
        super(message);
        this.channel = null;
        this.expected = null;
        this.actual = null;
    }

    public RelayConfigurationException(String channel, Class<?> expected, Class<?> actual, String message) {
        super(message);
        this.channel = channel;
        this.expected = expected;
        this.actual = actual;
    }

    public RelayConfigurationException(List<String> errors) {
        super("relay topology validation failed: " + Objects.requireNonNull(errors));
        this.channel = null;
        this.expected = null;
        this.actual = null;
    }

    /**
     * Factory for the v1 "this Clock isn't wired yet" rejection. Shards
     * declaring {@code LogicalClock} or {@code EventTimeFrom} fail Relay
     * registration with this exception until a follow-up wires them.
     */
    public static RelayConfigurationException clockNotImplementedYet(String clockTypeName) {
        return new RelayConfigurationException("Clock not implemented yet: " + clockTypeName);
    }

    /**
     * Factory raised at {@code registerRelay} time when a channel is declared
     * with {@code alsoToPluginWith(...)} more than once. Only one plugin sink
     * per channel is allowed; the first call registers it, the second fails.
     */
    public static RelayConfigurationException duplicatePluginSink(String channel) {
        return new RelayConfigurationException("channel `" + channel + "` was declared with `alsoToPluginWith(...)` "
                + "more than once; only one plugin sink per channel is allowed");
    }

    /**
     * Factory raised when a Shard's resolved {@code mailboxCapacity()} (after
     * applying the {@code FLEZHA_MAILBOX_CAPACITY} env fallback) is zero.
     * Capacity must be in {@code 1..=1_048_576}.
     */
    public static RelayConfigurationException mailboxCapacityZero(String shard) {
        return new RelayConfigurationException(
                "shard `" + shard + "` declared mailboxCapacity = 0; capacity must be in 1..=1_048_576");
    }

    /**
     * Factory raised when a Shard's resolved {@code mailboxCapacity()} is
     * above {@code 1_048_576}. Capacity must be in {@code 1..=1_048_576}.
     */
    public static RelayConfigurationException mailboxCapacityTooLarge(String shard, long value) {
        return new RelayConfigurationException(
                "shard `" + shard + "` declared mailboxCapacity = " + value + "; capacity must be in 1..=1_048_576");
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
