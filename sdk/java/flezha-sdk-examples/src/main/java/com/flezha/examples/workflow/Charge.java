package com.flezha.examples.workflow;

import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.StateHandle;
import java.util.List;
import javax.annotation.Nonnull;

/** Takes the money, and knows how to refund it. */
public final class Charge extends Step {

    @Override
    @Nonnull
    public String name() {
        return "charge";
    }

    @Override
    public void run(@Nonnull String reserved, @Nonnull StateHandle<Handle, NoChange> state, @Nonnull ProcedureCtx ctx)
            throws ProcedureException {
        ctx.emit(reserved + "|charged");
    }

    @Override
    public boolean compensateOverridden() {
        return true;
    }

    @Override
    public void compensate(
            @Nonnull List<String> completedOutput,
            @Nonnull StateHandle<Handle, NoChange> state,
            @Nonnull ProcedureCtx ctx) {
        System.out.println("refunding order");
    }
}
