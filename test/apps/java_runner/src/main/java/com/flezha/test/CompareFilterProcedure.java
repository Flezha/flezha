package com.flezha.test;

import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.util.Optional;

public final class CompareFilterProcedure
        implements Procedure<String, CompareFilterProcedure.State, String, NoChange, WallClock, Void> {

    @Override
    public String name() {
        return "compare_filter_procedure";
    }

    @Override
    public State initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
        return new State("default", false);
    }

    @Override
    public void run(String input, StateHandle<State, NoChange> state, ProcedureCtx ctx) throws ProcedureException {
        if (state.get().comparable().equals(input)) {
            ctx.emit(input);
        }
    }

    public record State(String comparable, boolean inverted) {
    }
}
