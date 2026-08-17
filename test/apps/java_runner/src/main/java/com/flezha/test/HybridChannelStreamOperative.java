package com.flezha.test;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.operative.StreamOperative;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Optional;

/**
 * Java mirror of the Rust test app's {@code HybridChannelStreamOperative}.
 * Demonstrates {@code alsoToPluginWith(...)}: the producer Procedure emits a
 * {@code Long} value that fans out to an inter-procedure consumer and is also
 * serialized to bytes for the {@code hybrid_indicators} plugin. The
 * serializer throws on the sentinel {@code 0xDEADBEEF} so an operator can
 * observe the failure-isolation guarantee end-to-end.
 */
public final class HybridChannelStreamOperative implements StreamOperative {

    @Override
    public String factoryId() {
        return "hybrid_channel";
    }

    @Override
    public void channels(ChannelBuilder c) {
        var sHybridProducerProcedure = c.input(String.class, "input").into(HybridProducerProcedure.class);
        sHybridProducerProcedure.named("hybrid_indicator").sink("hybrid_indicators", v -> { if (v == 0xDEAD_BEEFL) { throw new RuntimeException("hybrid_indicators: serializer rejected sentinel 0xDEADBEEF"); } return ByteBuffer.allocate(4) .order(ByteOrder.BIG_ENDIAN) .putInt(v.intValue()) .array(); }).into(HybridConsumerProcedure.class);
    }

    public static final class HybridProducerProcedure
            implements Procedure<String, HybridProducerProcedure.State, Long, NoChange, WallClock, Void> {

        @Override
        public String name() {
            return "hybrid_producer_procedure";
        }

        @Override
        public State initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
            return new State();
        }

        @Override
        public void run(String input, StateHandle<State, NoChange> state, ProcedureCtx ctx) throws ProcedureException {
            long n;
            try {
                n = Long.parseLong(input);
            } catch (NumberFormatException e) {
                n = 0L;
            }
            try {
                ctx.emit(n);
            } catch (ProcedureException.PluginSerializerFailure e) {
                System.err.println("hybrid_producer_procedure: serializer failed on `" + e.channel() + "`: "
                        + e.getMessage());
            }
        }

        public record State() {}
    }

    public static final class HybridConsumerProcedure
            implements Procedure<Long, HybridConsumerProcedure.State, Void, NoChange, WallClock, Void> {

        @Override
        public String name() {
            return "hybrid_consumer_procedure";
        }

        @Override
        public State initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
            return new State();
        }

        @Override
        public void run(Long input, StateHandle<State, NoChange> state, ProcedureCtx ctx) {
            System.out.println("hybrid_consumer_procedure: received " + input);
        }

        public record State() {}
    }
}
