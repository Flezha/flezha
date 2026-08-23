package com.flezha.test;

import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.WallClock;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Shared base for the {@code key:value} upsert procedures: {@link TierStoreProcedure},
 * {@link CheckpointStoreProcedure} and {@link BenchCheckpointProcedure} differ only
 * in {@code name()} and {@code run}. Mirrors the Rust test app's three procedures
 * over one {@code TierStoreState} / {@code TierStoreChange} pair.
 */
public abstract class KeyedStoreProcedure
        implements Procedure<String, TierStoreState, String, TierStoreChange, WallClock, Void> {

    @Override
    public TierStoreState initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
        return new TierStoreState();
    }

    @Override
    public void applyChange(TierStoreState state, TierStoreChange change) {
        state.values.put(change.key(), change.value());
    }

    @Override
    public List<Class<?>> kryoTypes() {
        return List.of(TierStoreState.class, TierStoreChange.class, TreeMap.class);
    }

    /** Split {@code "key:value"}; a value-less input keys on the whole string. */
    static String[] split(String input) {
        int at = input.indexOf(':');
        return at < 0 ? new String[] {input, ""} : new String[] {input.substring(0, at), input.substring(at + 1)};
    }
}
