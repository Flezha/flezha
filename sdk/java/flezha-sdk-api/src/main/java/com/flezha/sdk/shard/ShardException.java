package com.flezha.sdk.shard;

/**
 * Recoverable error thrown by {@link Shard#run} or {@link ShardCtx} emit
 * methods. The runtime catches this, logs against
 * {@code (relayInstance, shard, partition)}, discards any partial emits, and
 * continues dispatching subsequent inputs on the same Partition.
 */
public class ShardException extends Exception {

    public ShardException(String message) {
        super(message);
    }

    public ShardException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Thrown when {@code ctx.emitWithMeta(...)} is called on a channel
     * declared as inter-shard. Per-channel reserved-key semantics apply only
     * to plugin-bound channels.
     */
    public static final class MetadataNotAllowedOnInterShardChannel extends ShardException {
        private final String channel;

        public MetadataNotAllowedOnInterShardChannel(String channel) {
            super("metadata is only valid on plugin-bound channels; channel `" + channel + "` is inter-shard");
            this.channel = channel;
        }

        public String channel() {
            return channel;
        }
    }

    /**
     * Thrown when an {@code emit} or {@code emitWithMeta} call references a
     * channel name that was not declared on this Relay's topology.
     */
    public static final class UnknownChannel extends ShardException {
        private final String channel;

        public UnknownChannel(String channel) {
            super("channel `" + channel + "` is not declared on this relay");
            this.channel = channel;
        }

        public String channel() {
            return channel;
        }
    }

    /**
     * Thrown when a hybrid channel's user-supplied plugin serializer throws.
     * Consumer fan-out has already completed by the time this surfaces — the
     * inter-shard consumers received the typed value; only the plugin-wire
     * emission was skipped. The dispatcher thread stays alive.
     */
    public static final class PluginSerializerFailure extends ShardException {
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
