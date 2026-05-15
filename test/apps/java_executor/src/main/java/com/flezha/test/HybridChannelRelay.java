package com.flezha.test;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.relay.Relay;
import com.flezha.sdk.shard.Shard;
import com.flezha.sdk.shard.ShardCtx;
import com.flezha.sdk.shard.ShardException;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Optional;

/**
 * Java mirror of the Rust test app's {@code HybridChannelRelay}.
 * Demonstrates {@code alsoToPluginWith(...)}: the producer Shard emits a
 * {@code Long} value that fans out to an inter-shard consumer and is also
 * serialized to bytes for the {@code hybrid_indicators} plugin. The
 * serializer throws on the sentinel {@code 0xDEADBEEF} so an operator can
 * observe the failure-isolation guarantee end-to-end.
 */
public final class HybridChannelRelay implements Relay {

    @Override
    public String factoryId() {
        return "hybrid_channel";
    }

    @Override
    public void channels(ChannelBuilder c) {
        c.input(String.class, "input").into(HybridProducerShard.class);
        c.channel(Long.class, "hybrid_indicator")
                .from(HybridProducerShard.class)
                .alsoToPluginWith("hybrid_indicators", v -> {
                    if (v == 0xDEAD_BEEFL) {
                        throw new RuntimeException("hybrid_indicators: serializer rejected sentinel 0xDEADBEEF");
                    }
                    return ByteBuffer.allocate(4)
                            .order(ByteOrder.BIG_ENDIAN)
                            .putInt(v.intValue())
                            .array();
                })
                .into(HybridConsumerShard.class);
    }

    public static final class HybridProducerShard
            implements Shard<String, HybridProducerShard.State, Long, NoChange, WallClock, Void> {

        @Override
        public String name() {
            return "hybrid_producer_shard";
        }

        @Override
        public Class<String> inputType() {
            return String.class;
        }

        @Override
        public Class<Long> outputType() {
            return Long.class;
        }

        @Override
        public Class<NoChange> changeType() {
            return NoChange.class;
        }

        @Override
        public Class<WallClock> clockType() {
            return WallClock.class;
        }

        @Override
        public Class<Void> configType() {
            return Void.class;
        }

        @Override
        public State initState(Optional<String> shardKey, Optional<RestoreInitPayload> initPayload) {
            return new State();
        }

        @Override
        public void run(String input, StateHandle<State, NoChange> state, ShardCtx ctx) throws ShardException {
            long n;
            try {
                n = Long.parseLong(input);
            } catch (NumberFormatException e) {
                n = 0L;
            }
            try {
                ctx.emit(n);
            } catch (ShardException.PluginSerializerFailure e) {
                System.err.println("hybrid_producer_shard: serializer failed on `" + e.channel() + "`: "
                        + e.getMessage());
            }
        }

        public record State() {}
    }

    public static final class HybridConsumerShard
            implements Shard<Long, HybridConsumerShard.State, Void, NoChange, WallClock, Void> {

        @Override
        public String name() {
            return "hybrid_consumer_shard";
        }

        @Override
        public Class<Long> inputType() {
            return Long.class;
        }

        @Override
        public Class<Void> outputType() {
            return Void.class;
        }

        @Override
        public Class<NoChange> changeType() {
            return NoChange.class;
        }

        @Override
        public Class<WallClock> clockType() {
            return WallClock.class;
        }

        @Override
        public Class<Void> configType() {
            return Void.class;
        }

        @Override
        public State initState(Optional<String> shardKey, Optional<RestoreInitPayload> initPayload) {
            return new State();
        }

        @Override
        public void run(Long input, StateHandle<State, NoChange> state, ShardCtx ctx) {
            System.out.println("hybrid_consumer_shard: received " + input);
        }

        public record State() {}
    }
}
