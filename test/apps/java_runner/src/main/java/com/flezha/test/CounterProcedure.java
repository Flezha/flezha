package com.flezha.test;

import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.util.List;
import java.util.Optional;

/**
 * Java equivalent of the Rust SDK's {@code CounterProcedure}. Each input is a
 * {@code Long} delta; the Procedure records a {@link CounterChange} via
 * {@code state.apply}, and the {@code applyChange} default mutates
 * {@code state.value}.
 */
public class CounterProcedure
        implements Procedure<Long, CounterProcedure.State, Void, CounterChange, WallClock, Void> {

    public static final class State {
        public long value;
    }

    @Override
    public String name() {
        return "counter_procedure";
    }

    @Override
    public State initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
        return new State();
    }

    @Override
    public void applyChange(State state, CounterChange change) {
        state.value += change.delta();
    }

    @Override
    public List<Class<?>> kryoTypes() {
        return List.of(State.class, CounterChange.class);
    }

    @Override
    public void run(Long input, StateHandle<State, CounterChange> state, ProcedureCtx ctx)
            throws ProcedureException {
        state.apply(new CounterChange(input));
    }
}
