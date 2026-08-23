package com.flezha.test;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.StreamOperative;
import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.Stateless;
import com.flezha.sdk.state.WallClock;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * Mirror of the Rust test app's {@code MixedOperative}: one {@code @Procedure}
 * sugar procedure ({@link MixedMacroEntry}) wired to one hand-written
 * {@code Procedure} in an explicit {@code channels()} body. Declared explicitly
 * rather than collected, exactly as the Rust twin is.
 */
public final class MixedStreamOperative implements StreamOperative {

    @Override
    public String factoryId() {
        return "mixed_operative";
    }

    @Override
    public void channels(ChannelBuilder c) {
        c.input(String.class, "input")
                .into(MixedMacroEntryProcedure.class)
                .named("hop")
                .into(MixedHandDownstream.class)
                .named("kafka")
                .sink("test_kafka");
    }

    public static final class MixedHandDownstream
            implements Procedure<String, Stateless, byte[], NoChange, WallClock, Void> {

        @Override
        public String name() {
            return "mixed_hand_downstream";
        }

        @Override
        public Stateless initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
            return new Stateless();
        }

        @Override
        public void run(String input, StateHandle<Stateless, NoChange> state, ProcedureCtx ctx)
                throws ProcedureException {
            ctx.emit(input.getBytes(StandardCharsets.UTF_8));
        }
    }
}
