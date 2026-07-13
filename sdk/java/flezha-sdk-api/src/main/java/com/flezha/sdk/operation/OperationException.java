package com.flezha.sdk.operation;

/**
 * Recoverable error thrown by {@link Operation#run} or {@link OperationCtx} emit
 * methods. The runtime catches this, logs against
 * {@code (operativeInstance, operation, segment)}, discards any partial emits, and
 * continues dispatching subsequent inputs on the same Segment.
 */
public class OperationException extends Exception {

    public OperationException(String message) {
        super(message);
    }

    public OperationException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Thrown when {@code ctx.emitWithMeta(...)} is called on a channel
     * declared as inter-operation. Per-channel reserved-key semantics apply only
     * to plugin-bound channels.
     */
    public static final class MetadataNotAllowedOnInterOperationChannel extends OperationException {
        private final String channel;

        public MetadataNotAllowedOnInterOperationChannel(String channel) {
            super("metadata is only valid on plugin-bound channels; channel `" + channel + "` is inter-operation");
            this.channel = channel;
        }

        public String channel() {
            return channel;
        }
    }

    /**
     * Thrown when a plain {@code emit} is called on a channel declared
     * {@code .keyed()}. The declaration is the single source of truth for
     * whether a key is required — use {@code ctx.emitKeyed(...)}.
     */
    public static final class KeyRequiredOnKeyedChannel extends OperationException {
        private final String channel;

        public KeyRequiredOnKeyedChannel(String channel) {
            super("channel `" + channel + "` is declared keyed; use emitKeyed(key, value)");
            this.channel = channel;
        }

        public String channel() {
            return channel;
        }
    }

    /** Thrown when {@code ctx.emitKeyed(...)} targets a channel not declared {@code .keyed()}. */
    public static final class KeyNotAllowedOnNonKeyedChannel extends OperationException {
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
     * namespace. The runtime's identity stamps ({@code flz.segment},
     * {@code flz.seq}, {@code flz.key}) live there, so a user key is rejected
     * rather than dropped — otherwise a stamp could be spoofed or clobbered.
     */
    public static final class ReservedMetadataKey extends OperationException {
        private final String key;

        public ReservedMetadataKey(String key) {
            super("metadata key `" + key + "` is reserved: the `" + OperationCtx.RESERVED_METADATA_PREFIX
                    + "` namespace belongs to the runtime");
            this.key = key;
        }

        public String key() {
            return key;
        }
    }

    /**
     * Thrown when an {@code emit} or {@code emitWithMeta} call references a
     * channel name that was not declared on this UnboundedOperative's topology.
     */
    public static final class UnknownChannel extends OperationException {
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
    public static final class NonRetryable extends OperationException {

        public NonRetryable(String message) {
            super("non-retryable operation error: " + message);
        }

        public NonRetryable(String message, Throwable cause) {
            super("non-retryable operation error: " + message, cause);
        }
    }

    /**
     * Thrown when a hybrid channel's user-supplied plugin serializer throws.
     * Consumer fan-out has already completed by the time this surfaces — the
     * inter-operation consumers received the typed value; only the plugin-wire
     * emission was skipped. The dispatcher thread stays alive.
     */
    public static final class PluginSerializerFailure extends OperationException {
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
