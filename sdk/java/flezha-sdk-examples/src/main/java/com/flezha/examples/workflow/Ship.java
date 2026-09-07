package com.flezha.examples.workflow;

import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.StateHandle;
import javax.annotation.Nonnull;

/** The last step. Nothing after it, so it needs no undo. */
public final class Ship extends Step {

    @Override
    @Nonnull
    public String name() {
        return "ship";
    }

    @Override
    public void run(@Nonnull String charged, @Nonnull StateHandle<Handle, NoChange> state, @Nonnull ProcedureCtx ctx)
            throws ProcedureException {
        ctx.emit(charged + "|shipped");
    }
}
