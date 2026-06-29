package com.flezha.test;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operation.Operation;
import com.flezha.sdk.operation.OperationCtx;
import com.flezha.sdk.operation.OperationException;
import com.flezha.sdk.operative.ContinuousOperative;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Optional;

/**
 * Java mirror of the Rust test app's {@code HybridChannelContinuousOperative}.
 * Demonstrates {@code alsoToPluginWith(...)}: the producer Operation emits a
 * {@code Long} value that fans out to an inter-operation consumer and is also
 * serialized to bytes for the {@code hybrid_indicators} plugin. The
 * serializer throws on the sentinel {@code 0xDEADBEEF} so an operator can
 * observe the failure-isolation guarantee end-to-end.
 */
public final class HybridChannelContinuousOperative implements ContinuousOperative {

    @Override
    public String factoryId() {
        return "hybrid_channel";
    }

    @Override
    public void channels(ChannelBuilder c) {
        c.input(String.class, "input").into(HybridProducerOperation.class);
        c.channel(Long.class, "hybrid_indicator")
                .from(HybridProducerOperation.class)
                .alsoToPluginWith("hybrid_indicators", v -> {
                    if (v == 0xDEAD_BEEFL) {
                        throw new RuntimeException("hybrid_indicators: serializer rejected sentinel 0xDEADBEEF");
                    }
                    return ByteBuffer.allocate(4)
                            .order(ByteOrder.BIG_ENDIAN)
                            .putInt(v.intValue())
                            .array();
                })
                .into(HybridConsumerOperation.class);
    }

    public static final class HybridProducerOperation
            implements Operation<String, HybridProducerOperation.State, Long, NoChange, WallClock, Void> {

        @Override
        public String name() {
            return "hybrid_producer_operation";
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
        public State initState(Optional<String> operationKey, Optional<RestoreInitPayload> initPayload) {
            return new State();
        }

        @Override
        public void run(String input, StateHandle<State, NoChange> state, OperationCtx ctx) throws OperationException {
            long n;
            try {
                n = Long.parseLong(input);
            } catch (NumberFormatException e) {
                n = 0L;
            }
            try {
                ctx.emit(n);
            } catch (OperationException.PluginSerializerFailure e) {
                System.err.println("hybrid_producer_operation: serializer failed on `" + e.channel() + "`: "
                        + e.getMessage());
            }
        }

        public record State() {}
    }

    public static final class HybridConsumerOperation
            implements Operation<Long, HybridConsumerOperation.State, Void, NoChange, WallClock, Void> {

        @Override
        public String name() {
            return "hybrid_consumer_operation";
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
        public State initState(Optional<String> operationKey, Optional<RestoreInitPayload> initPayload) {
            return new State();
        }

        @Override
        public void run(Long input, StateHandle<State, NoChange> state, OperationCtx ctx) {
            System.out.println("hybrid_consumer_operation: received " + input);
        }

        public record State() {}
    }
}
