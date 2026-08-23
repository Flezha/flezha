package com.flezha.test;

import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.state.StateHandle;

/**
 * Mirror of the Rust test app's {@code BenchCheckpointProcedure}: the same keyed
 * upsert as {@link CheckpointStoreProcedure} with no {@code commit:} input — the
 * round-trip bench drives the interval from a declared cadence instead.
 */
public final class BenchCheckpointProcedure extends KeyedStoreProcedure {

    @Override
    public String name() {
        return "bench_checkpoint_procedure";
    }

    @Override
    public void run(String input, StateHandle<TierStoreState, TierStoreChange> state, ProcedureCtx ctx)
            throws ProcedureException {
        String[] kv = split(input);
        state.apply(new TierStoreChange(kv[0], kv[1]));
        ctx.emitKeyed(kv[0], kv[0] + "=" + kv[1]);
    }
}
