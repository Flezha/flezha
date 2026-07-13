package com.flezha.sdk.operative;

import java.util.List;
import java.util.Objects;

/**
 * Thrown by {@link UnboundedOperativeRegistry#registerUnbounded(Class)} when an UnboundedOperative's
 * declared topology violates a wiring constraint (entry-channel-type,
 * type-equality between channel payload and producer/consumer Operation,
 * unwired channel, multi-producer, static-mode default-impl detection).
 *
 * <p>Carries channel name and expected/actual type info so the executor
 * can diagnose the misconfiguration without re-running with a debugger.
 */
public class UnboundedOperativeConfigurationException extends RuntimeException {

    private final String channel;
    private final Class<?> expected;
    private final Class<?> actual;

    public UnboundedOperativeConfigurationException(String message) {
        super(message);
        this.channel = null;
        this.expected = null;
        this.actual = null;
    }

    public UnboundedOperativeConfigurationException(
            String channel, Class<?> expected, Class<?> actual, String message) {
        super(message);
        this.channel = channel;
        this.expected = expected;
        this.actual = actual;
    }

    public UnboundedOperativeConfigurationException(List<String> errors) {
        super("operative topology validation failed: " + Objects.requireNonNull(errors));
        this.channel = null;
        this.expected = null;
        this.actual = null;
    }

    /**
     * Raised when an Operation declares {@code EventTimeFrom} but does not
     * override {@code eventTime(Input)}. Rust enforces the extractor with a
     * compile-time {@code TimeExtractor<Input>} bound; Java's erasure precludes
     * that, so the same defect is caught at registration — before any input is
     * served and before anything durable exists.
     */
    public static UnboundedOperativeConfigurationException eventTimeExtractorMissing(String operationName) {
        return new UnboundedOperativeConfigurationException("operation `" + operationName
                + "` declares the EventTimeFrom clock but does not override `eventTime(Input)`; "
                + "an event-time clock has no value to serve without an extractor");
    }

    /**
     * Raised when an Operation declares {@code LogicalClock} with
     * {@code Change = NoChange}. The logical clock serves the durable change
     * seq, and a stateless Operation has none. Carries the Rust error name
     * {@code UnservableClock}.
     */
    public static UnboundedOperativeConfigurationException unservableClock(String operationName) {
        return new UnboundedOperativeConfigurationException("UnservableClock: operation `" + operationName
                + "` declares LogicalClock with Change = NoChange; there is no durable change seq to serve");
    }

    /**
     * Raised when {@code checkpointAck()} is declared without
     * {@code effectivelyOnce()}. Checkpointing withholds the consumed input
     * offset until a boundary — and without the tier there is no offset to
     * withhold.
     */
    public static UnboundedOperativeConfigurationException checkpointAckWithoutTier() {
        return new UnboundedOperativeConfigurationException(
                "CheckpointAckWithoutTier: `checkpointAck()` requires `effectivelyOnce()`; "
                        + "without the tier there is no consumed offset to withhold until a checkpoint");
    }

    /**
     * Raised when {@code correctionWindow(...)} is declared without a
     * prerequisite it cannot work without: the tier (a correction rewinds a
     * consumed offset), an {@code EventTimeFrom} clock (there is no event time
     * to compare), or keyed channels (a corrected re-emit must overwrite rather
     * than duplicate).
     */
    public static UnboundedOperativeConfigurationException correctionWindowRequires(String missing) {
        return new UnboundedOperativeConfigurationException("`correctionWindow(...)` requires " + missing
                + "; a correction rewinds the Segment to a baseline before the late input's event time "
                + "and replays the slice, which is impossible without it");
    }

    /**
     * Raised when an effectively-once Operative has a plugin-bound channel that
     * is not {@code .keyed()}. The tier's replay re-emits outputs, and only a
     * keyed sink can absorb that as an idempotent upsert.
     */
    public static UnboundedOperativeConfigurationException tierChannelNotKeyed(String channel) {
        return new UnboundedOperativeConfigurationException("channel `" + channel
                + "` is plugin-bound on an `effectivelyOnce()` operative but is not declared `.keyed()`; "
                + "the tier re-emits outputs on replay, which only a keyed sink can absorb idempotently");
    }

    /**
     * Raised when a channel is declared {@code .keyed()} without a plugin wire.
     * A key rides as a {@code flz.key} stamp on a plugin frame, so a pure
     * inter-operation channel has nowhere to carry it.
     */
    public static UnboundedOperativeConfigurationException keyedChannelWithoutPluginWire(String channel) {
        return new UnboundedOperativeConfigurationException("channel `" + channel
                + "` is declared `.keyed()` but has no plugin wire; a key rides on a plugin frame's "
                + "`flz.key` stamp, so it has nowhere to go on a pure inter-operation channel");
    }

    /**
     * Factory raised at {@code registerUnbounded} time when a channel is declared
     * with {@code alsoToPluginWith(...)} more than once. Only one plugin sink
     * per channel is allowed; the first call registers it, the second fails.
     */
    public static UnboundedOperativeConfigurationException duplicatePluginSink(String channel) {
        return new UnboundedOperativeConfigurationException(
                "channel `" + channel + "` was declared with `alsoToPluginWith(...)` "
                        + "more than once; only one plugin sink per channel is allowed");
    }

    /**
     * Factory raised when a Operation's resolved {@code mailboxCapacity()} (after
     * applying the {@code FLEZHA_MAILBOX_CAPACITY} env fallback) is zero.
     * Capacity must be in {@code 1..=1_048_576}.
     */
    public static UnboundedOperativeConfigurationException mailboxCapacityZero(String operation) {
        return new UnboundedOperativeConfigurationException(
                "operation `" + operation + "` declared mailboxCapacity = 0; capacity must be in 1..=1_048_576");
    }

    /**
     * Factory raised when a Operation's resolved {@code mailboxCapacity()} is
     * above {@code 1_048_576}. Capacity must be in {@code 1..=1_048_576}.
     */
    public static UnboundedOperativeConfigurationException mailboxCapacityTooLarge(String operation, long value) {
        return new UnboundedOperativeConfigurationException("operation `" + operation + "` declared mailboxCapacity = "
                + value + "; capacity must be in 1..=1_048_576");
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
