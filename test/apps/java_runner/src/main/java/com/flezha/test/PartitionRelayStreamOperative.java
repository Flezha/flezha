package com.flezha.test;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.StreamOperative;
import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Mirror of the Rust test app's {@code PartitionRelayOperative}: the
 * partitioned-operatives e2e's upstream — keyed on the wiring (so it can be
 * partitioned at all) AND emitting keyed (so a partitioned forward target can
 * resolve the owning member).
 */
public final class PartitionRelayStreamOperative implements StreamOperative {

    @Override
    public String factoryId() {
        return "partition_relay";
    }

    @Override
    public void channels(ChannelBuilder c) {
        // The plugin channel is named `input` because a forwarded frame keeps
        // its producing channel's name in metadata, and that is what selects
        // the receiving operative's channel — the downstream fixture's entry
        // channel. The entry channel here is `in` so the two names cannot
        // collide inside this class.
        c.input(String.class, "in")
                .keyBy(DynamicEntityStreamOperative::key)
                .into(PartitionRelayProcedure.class)
                .named("input")
                .keyed()
                .sink("relay_out");
    }

    public static final class PartitionRelayProcedure
            implements Procedure<String, PartitionRelayProcedure.State, String, StoreInputChange, WallClock, Void> {

        public record State(List<String> seen) {}

        @Override
        public String name() {
            return "partition_relay_procedure";
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
            ctx.emitKeyed(DynamicEntityStreamOperative.key(input), input);
        }
    }
}
