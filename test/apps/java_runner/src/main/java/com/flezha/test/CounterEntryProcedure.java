package com.flezha.test;

import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.Stateless;
import com.flezha.sdk.state.WallClock;
import java.util.Optional;

/**
 * Java equivalent of the Rust SDK's {@code CounterEntryProcedure}: parses the
 * entry channel's decimal text into the delta {@link CounterProcedure} folds.
 */
public class CounterEntryProcedure
        implements Procedure<String, Stateless, Long, NoChange, WallClock, Void> {

    @Override
    public String name() {
        return "counter_entry_procedure";
    }

    @Override
    public Stateless initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
        return new Stateless();
    }

    @Override
    public void run(String input, StateHandle<Stateless, NoChange> state, ProcedureCtx ctx)
            throws ProcedureException {
        long delta;
        try {
            delta = Long.parseLong(input);
        } catch (NumberFormatException e) {
            delta = 0L;
        }
        ctx.emit(delta);
    }
}
