package com.flezha.sdk.runtime.operative;

import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.WallClock;
import java.util.Optional;

/**
 * Test-only base pinning the persistence-related type arguments to their
 * stateless values ({@link NoChange}, {@link WallClock}, {@link Void}) so the
 * runtime-module test fixtures declare only what varies. A subclass that pins
 * {@code Input} / {@code State} / {@code Output} concretely needs no type-token
 * override at all — the interface defaults read all six off this clause.
 */
public abstract class StatelessTestProcedure<Input, State, Output>
        implements Procedure<Input, State, Output, NoChange, WallClock, Void> {

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
