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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Mirror of the Rust test app's {@code StaticEntityOperative}: a static-entity
 * entry procedure routing by the input's first character, hopping into a
 * stateless downstream procedure that hands the bytes to the {@code test_kafka}
 * plugin.
 */
public final class StaticEntityStreamOperative implements StreamOperative {

    @Override
    public String factoryId() {
        return "static_entity";
    }

    @Override
    public void channels(ChannelBuilder c) {
        c.input(String.class, "input")
                .into(StaticEntryProcedure.class)
                .named("hop")
                .into(StaticDownstreamProcedure.class)
                .named("kafka")
                .sink("test_kafka");
    }

    public static final class StaticEntryProcedure
            implements Procedure<String, StaticEntryProcedure.State, String, StoreInputChange, WallClock, Void> {

        public record State(List<String> seen) {}

        @Override
        public String name() {
            return "static_entry_procedure";
        }

        @Override
        public List<String> entityKeys() {
            return List.of("a", "b", "c", "d");
        }

        @Override
        public boolean entitiesForOverridden() {
            return true;
        }

        @Override
        public List<String> entitiesFor(String input) {
            return List.of(StaticEntityStreamOperative.partition(input));
        }

        @Override
        public State initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
            return new State(new ArrayList<>());
        }

        @Override
        public void applyChange(State state, StoreInputChange change) {
            state.seen().add(change.text());
        }

        @Override
        public List<Class<?>> kryoTypes() {
            return List.of(State.class, StoreInputChange.class, ArrayList.class);
        }

        @Override
        public void run(String input, StateHandle<State, StoreInputChange> state, ProcedureCtx ctx)
                throws ProcedureException {
            state.apply(new StoreInputChange(input));
            ctx.emit(input);
        }
    }

    public static final class StaticDownstreamProcedure
            implements Procedure<String, Stateless, byte[], NoChange, WallClock, Void> {

        @Override
        public String name() {
            return "static_downstream_procedure";
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

    /** The static entity key an input routes to: its first character, {@code d} otherwise. */
    static String partition(String input) {
        char first = input.isEmpty() ? ' ' : input.charAt(0);
        return switch (first) {
            case 'a' -> "a";
            case 'b' -> "b";
            case 'c' -> "c";
            default -> "d";
        };
    }
}
