package com.flezha.sdk.procedure;

/**
 * Recoverable error thrown by {@link Procedure#run} or {@link ProcedureCtx} emit
 * methods. The runtime catches this, logs against
 * {@code (operativeInstance, procedure, entity)}, discards any partial emits, and
 * continues dispatching subsequent inputs on the same Entity.
 */
public class ProcedureException extends Exception {

    public ProcedureException(String message) {
        super(message);
    }

    public ProcedureException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Thrown when {@code ctx.emitWithMeta(...)} is called on a channel
     * declared as inter-procedure. Per-channel reserved-key semantics apply only
     * to plugin-bound channels.
     */
    public static final class MetadataNotAllowedOnInterProcedureChannel extends ProcedureException {
        private final String channel;

        public MetadataNotAllowedOnInterProcedureChannel(String channel) {
            super("metadata is only valid on plugin-bound channels; channel `" + channel + "` is inter-procedure");
            this.channel = channel;
        }

        public String channel() {
            return channel;
        }
    }

    /**
     * Thrown when a plain {@code emit} is called on a channel declared
     * {@code .keyed()} by a single-entity Procedure — one with no Entity key
     * to emit under. A keyed Entity's plain emit carries its own key instead;
     * an output whose key differs uses {@code ctx.emitKeyed(...)}.
     */
    public static final class KeyRequiredOnKeyedChannel extends ProcedureException {
        private final String channel;

        public KeyRequiredOnKeyedChannel(String channel) {
            super("channel `" + channel
                    + "` is declared keyed and this procedure is single-entity, so there is no Entity key to emit"
                    + " under; use emitKeyed(key, value)");
            this.channel = channel;
        }

        public String channel() {
            return channel;
        }
    }

    /** Thrown when {@code ctx.emitKeyed(...)} targets a channel not declared {@code .keyed()}. */
    public static final class KeyNotAllowedOnNonKeyedChannel extends ProcedureException {
        private final String channel;

        public KeyNotAllowedOnNonKeyedChannel(String channel) {
            super("channel `" + channel + "` is not declared keyed; use emit(value)");
            this.channel = channel;
        }

        public String channel() {
            return channel;
        }
    }

    /**
     * Thrown when user metadata carries a key in the reserved {@code flz.}
     * namespace. The runtime's identity stamps ({@code flz.entity},
     * {@code flz.seq}, {@code flz.key}) live there, so a user key is rejected
     * rather than dropped — otherwise a stamp could be spoofed or clobbered.
     */
    public static final class ReservedMetadataKey extends ProcedureException {
        private final String key;

        public ReservedMetadataKey(String key) {
            super("metadata key `" + key + "` is reserved: the `" + ProcedureCtx.RESERVED_METADATA_PREFIX
                    + "` namespace belongs to the runtime");
            this.key = key;
        }

        public String key() {
            return key;
        }
    }

    /**
     * Thrown when an {@code emit} or {@code emitWithMeta} call references a
     * channel name that was not declared on this StreamOperative's topology.
     */
    public static final class UnknownChannel extends ProcedureException {
        private final String channel;

        public UnknownChannel(String channel) {
            super("channel `" + channel + "` is not declared on this operative");
            this.channel = channel;
        }

        public String channel() {
            return channel;
        }
    }

    /**
     * Classifies a domain failure as non-retryable: the dispatcher parks the
     * payload to the dead-letter queue immediately, skipping the channel's
     * remaining retry budget. Use for failures that can never succeed on
     * re-run (malformed payload, permanent semantic violation).
     */
    public static final class NonRetryable extends ProcedureException {

        public NonRetryable(String message) {
            super("non-retryable procedure error: " + message);
        }

        public NonRetryable(String message, Throwable cause) {
            super("non-retryable procedure error: " + message, cause);
        }
    }

    /**
     * Thrown when a Procedure whose declared Clock does not derive time from
     * the input registers an event-time timer: without an event-time clock
     * there is no high-water for it to fire against.
     */
    public static final class EventTimeTimerWithoutEventTimeClock extends ProcedureException {

        public EventTimeTimerWithoutEventTimeClock(String procedure, String clock) {
            super("procedure `" + procedure + "` declares clock `" + clock
                    + "`, which does not derive time from the input; an event-time timer needs"
                    + " an EventTimeFrom clock");
        }
    }

    /**
     * Thrown when an Entity already holds {@code maxTimersPerEntity()} pending
     * timers and registers one more. A per-Entity timer set is author-controlled
     * memory multiplied by a stream Entity count; the cap is what keeps a
     * runaway registration a refusal rather than an OOM.
     */
    public static final class TimerCapExceeded extends ProcedureException {

        public TimerCapExceeded(String procedure, int max) {
            super("procedure `" + procedure + "` already holds its maximum of " + max
                    + " pending timers for this Entity; cancel one or raise maxTimersPerEntity()");
        }
    }

    /**
     * Thrown where timers do not apply — a workflow operative's stage (it has its
     * own wake model) and a {@code compensate} hook. The refusal is structural:
     * those contexts carry no timer session at all.
     */
    public static final class TimersUnavailable extends ProcedureException {

        public TimersUnavailable() {
            super("timers are not available in this context; only a stream operative's `run` /"
                    + " `onTimer` may register them");
        }
    }

    /**
     * Thrown when a hybrid channel's user-supplied plugin serializer throws.
     * Consumer fan-out has already completed by the time this surfaces — the
     * inter-procedure consumers received the typed value; only the plugin-wire
     * emission was skipped. The dispatcher thread stays alive.
     */
    public static final class PluginSerializerFailure extends ProcedureException {
        private final String channel;

        public PluginSerializerFailure(String channel, String message, Throwable cause) {
            super("plugin serializer for channel `" + channel + "` failed: " + message, cause);
            this.channel = channel;
        }

        public String channel() {
            return channel;
        }
    }
}
