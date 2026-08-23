package com.flezha.test;

import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class StoreInputProcedure
        implements Procedure<String, StoreInputProcedure.State, String, StoreInputChange, WallClock, Void> {

    @Override
    public String name() {
        return "store_input_procedure";
    }

    @Override
    public State initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
        return new State(new ArrayList<>(), new ArrayList<>());
    }

    @Override
    public void applyChange(State state, StoreInputChange change) {
        state.textInputs().add(change.text());
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

    public record State(List<byte[]> binaryInputs, List<String> textInputs) {
    }
}
