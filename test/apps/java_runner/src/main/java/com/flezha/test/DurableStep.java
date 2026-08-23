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
 * Shared base for the crash-resume e2e's durable {@code String -> String} steps
 * (§7c.3). A concrete proc declares only {@link #name()} (and, for a saga step,
 * overrides {@link #compensate} + {@link #compensateOverridden}). {@code run}
 * commits one idempotent {@link Insert} Change keyed by the proc name — so a
 * Model-A resume re-run over restored state is a no-op — and passes the input
 * on. Mirrors the Rust test app's {@code durable_step!} macro.
 */
public abstract class DurableStep
        implements Procedure<String, SeenState, String, Insert, WallClock, Void> {

    // A durable operative must register its Change type for the Kryo codec.
    @Override
    public List<Class<?>> kryoTypes() {
        return List.of(Insert.class, SeenState.class);
    }

    @Override
    public SeenState initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
        return new SeenState();
    }

    @Override
    public void applyChange(SeenState state, Insert change) {
        state.items.add(change.key());
    }

    @Override
    public void run(String input, StateHandle<SeenState, Insert> state, ProcedureCtx ctx)
            throws ProcedureException {
        state.apply(new Insert(name()));
        ctx.emit(input);
    }
}
