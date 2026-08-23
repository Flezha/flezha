package com.flezha.test;

import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.util.List;
import java.util.Optional;

/** Procedure backing {@link CoalescingCounterStreamOperative}: folds one {@link TickInput}. */
public final class TickProcedure
        implements Procedure<TickInput, TickProcedure.State, Void, TickChange, WallClock, Void> {

    public static final class State {
        public long last;
    }

    @Override
    public String name() {
        return "tick_procedure";
    }

    @Override
    public List<Class<?>> kryoTypes() {
        return List.of(State.class, TickChange.class);
    }

    @Override
    public State initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
        return new State();
    }

    @Override
    public void applyChange(State state, TickChange change) {
        state.last = change.delta();
    }

    @Override
    public void run(TickInput input, StateHandle<State, TickChange> state, ProcedureCtx ctx)
            throws ProcedureException {
        state.apply(new TickChange(input.interval(), input.ts(), input.delta()));
    }
}
