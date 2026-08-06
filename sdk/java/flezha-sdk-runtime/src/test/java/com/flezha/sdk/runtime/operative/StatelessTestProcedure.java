package com.flezha.sdk.runtime.operative;

import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.WallClock;
import java.util.Optional;

/**
 * Test-only base providing the persistence-related accessors with default
 * stateless values ({@link NoChange}, {@link WallClock}, {@link Void}) so the
 * runtime-module test fixtures stay terse.
 */
public abstract class StatelessTestProcedure<Input, State, Output>
        implements Procedure<Input, State, Output, NoChange, WallClock, Void> {

    @Override
    public Class<NoChange> changeType() {
        return NoChange.class;
    }

    @Override
    public Class<WallClock> clockType() {
        return WallClock.class;
    }

    @Override
    public Class<Void> configType() {
        return Void.class;
    }

    /**
     * Default fresh-start initializer that ignores the restore payload — most
     * test procedures don't care about restore. Subclasses override the variant
     * they need.
     */
    @Override
    public State initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
        return initState(entityKey);
    }

    /** Convenience hook for subclasses that don't care about the restore payload. */
    protected abstract State initState(Optional<String> entityKey);
}
