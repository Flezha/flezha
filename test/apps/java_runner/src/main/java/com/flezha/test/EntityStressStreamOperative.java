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
import java.util.List;
import java.util.Optional;

/**
 * Mirror of the Rust test app's {@code EntityStressOperative}: four chained
 * static-entity procedures over eight keys. The first fans one input out to
 * every key; the rest collapse it back onto {@code p0}, so one input walks the
 * widest Entity graph the runtime has to schedule.
 */
public final class EntityStressStreamOperative implements StreamOperative {

    static final List<String> STRESS_KEYS = List.of("p0", "p1", "p2", "p3", "p4", "p5", "p6", "p7");

    @Override
    public String factoryId() {
        return "entity_stress";
    }

    @Override
    public void channels(ChannelBuilder c) {
        c.input(String.class, "input")
                .into(StressProcedureA.class)
                .named("a_to_b")
                .into(StressProcedureB.class)
                .named("b_to_c")
                .into(StressProcedureC.class)
                .named("c_to_d")
                .into(StressProcedureD.class)
                .sink("echo");
    }

    /** Shared stateless surface of the four stress procedures; only the fan-out differs. */
    abstract static class StressProcedure<O> implements Procedure<String, Stateless, O, NoChange, WallClock, Void> {

        @Override
        public Stateless initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
            return new Stateless();
        }

        @Override
        public List<String> entityKeys() {
            return STRESS_KEYS;
        }

        @Override
        public boolean entitiesForOverridden() {
            return true;
        }
    }

    /** Fans every input out to all eight Entities. */
    public static final class StressProcedureA extends StressProcedure<String> {

        @Override
        public String name() {
            return "stress_a";
        }

        @Override
        public List<String> entitiesFor(String input) {
            return STRESS_KEYS;
        }

        @Override
        public void run(String input, StateHandle<Stateless, NoChange> state, ProcedureCtx ctx)
                throws ProcedureException {
            ctx.emit(input);
        }
    }

    /** Collapses the fan-out back onto {@code p0}. */
    public static final class StressProcedureB extends StressProcedure<String> {

        @Override
        public String name() {
            return "stress_b";
        }

        @Override
        public List<String> entitiesFor(String input) {
            return List.of("p0");
        }

        @Override
        public void run(String input, StateHandle<Stateless, NoChange> state, ProcedureCtx ctx)
                throws ProcedureException {
            ctx.emit(input);
        }
    }

    /** Second single-key hop. */
    public static final class StressProcedureC extends StressProcedure<String> {

        @Override
        public String name() {
            return "stress_c";
        }

        @Override
        public List<String> entitiesFor(String input) {
            return List.of("p0");
        }

        @Override
        public void run(String input, StateHandle<Stateless, NoChange> state, ProcedureCtx ctx)
                throws ProcedureException {
            ctx.emit(input);
        }
    }

    /** Terminal hop: hands the bytes to the {@code entity_stress_echo} plugin. */
    public static final class StressProcedureD extends StressProcedure<byte[]> {

        @Override
        public String name() {
            return "stress_d";
        }

        @Override
        public List<String> entitiesFor(String input) {
            return List.of("p0");
        }

        @Override
        public void run(String input, StateHandle<Stateless, NoChange> state, ProcedureCtx ctx)
                throws ProcedureException {
            ctx.emit(input.getBytes(StandardCharsets.UTF_8));
        }
    }
}
