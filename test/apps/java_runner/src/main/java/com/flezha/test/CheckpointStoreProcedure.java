package com.flezha.test;

import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.state.StateHandle;

/**
 * Mirror of the Rust test app's {@code CheckpointStoreProcedure}: the
 * {@link TierStoreProcedure} store acknowledging only at an explicit checkpoint
 * — a {@code commit:} input closes the interval, every other input folds into it.
 */
public final class CheckpointStoreProcedure extends KeyedStoreProcedure {

    @Override
    public String name() {
        return "checkpoint_store_procedure";
    }

    @Override
    public void run(String input, StateHandle<TierStoreState, TierStoreChange> state, ProcedureCtx ctx)
            throws ProcedureException {
        String[] kv = split(input);
        if ("commit".equals(kv[0])) {
            ctx.checkpoint();
            return;
        }
        state.apply(new TierStoreChange(kv[0], kv[1]));
        ctx.emitKeyed(kv[0], kv[0] + "=" + kv[1]);
    }
}
