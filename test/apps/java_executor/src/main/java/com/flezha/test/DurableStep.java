package com.flezha.test;

import com.flezha.sdk.operation.Operation;
import com.flezha.sdk.operation.OperationCtx;
import com.flezha.sdk.operation.OperationException;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.util.List;
import java.util.Optional;

/**
 * Shared base for the crash-resume e2e's durable {@code String -> String} steps
 * (§7c.3). A concrete op declares only {@link #name()} (and, for a saga step,
 * overrides {@link #compensate} + {@link #compensateOverridden}). {@code run}
 * commits one idempotent {@link Insert} Change keyed by the op name — so a
 * Model-A resume re-run over restored state is a no-op — and passes the input
 * on. Mirrors the Rust test app's {@code durable_step!} macro.
 */
public abstract class DurableStep
        implements Operation<String, SeenState, String, Insert, WallClock, Void> {

    @Override
    public Class<String> inputType() {
        return String.class;
    }

    @Override
    public Class<String> outputType() {
        return String.class;
    }

    @Override
    public Class<Insert> changeType() {
        return Insert.class;
    }

    @Override
    public Class<WallClock> clockType() {
        return WallClock.class;
    }

    @Override
    public Class<Void> configType() {
        return Void.class;
    }

    // A durable operative must register its Change type for the Kryo codec.
    @Override
    public List<Class<?>> kryoTypes() {
        return List.of(Insert.class);
    }

    @Override
    public SeenState initState(Optional<String> operationKey, Optional<RestoreInitPayload> initPayload) {
        return new SeenState();
    }

    @Override
    public void applyChange(SeenState state, Insert change) {
        state.items.add(change.key());
    }

    @Override
    public void run(String input, StateHandle<SeenState, Insert> state, OperationCtx ctx)
            throws OperationException {
        state.apply(new Insert(name()));
        ctx.emit(input);
    }
}
