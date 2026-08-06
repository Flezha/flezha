package com.flezha.test;

import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * The dynamic-mode consumer of {@link DynamicEntityUnboundedOperative}. It
 * declares no keying member of any kind — the edge's {@code keyBy} is what makes
 * it dynamic — and applies a Change per input so the controller writes a WAL
 * file per lazily-created Entity. {@code entityIdleTtl} stays on the procedure:
 * only the KEY moved to the wiring.
 */
public final class DynamicEntityProcedure
        implements Procedure<String, DynamicEntityProcedure.State, String, StoreInputChange, WallClock, Void> {

    @Override
    public String name() {
        return "dynamic_entity_procedure";
    }

    @Override
    public Class<String> inputType() {
        return String.class;
    }

    @Override
    public Class<String> outputType() {
        return String.class;
    }

    @Override
    public Class<StoreInputChange> changeType() {
        return StoreInputChange.class;
    }

    @Override
    public Class<WallClock> clockType() {
        return WallClock.class;
    }

    @Override
    public Class<Void> configType() {
        return Void.class;
    }

    @Override
    public Optional<java.time.Duration> entityIdleTtl() {
        String ms = System.getenv("DYN_TEST_TTL_MS");
        return ms == null || ms.isBlank()
                ? Optional.empty()
                : Optional.of(java.time.Duration.ofMillis(Long.parseLong(ms)));
    }

    @Override
    public State initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
        return new State(new ArrayList<>());
    }

    @Override
    public void applyChange(State state, StoreInputChange change) {
        state.seen().add(change.text());
    }

    @Override
    public List<Class<?>> kryoTypes() {
        return List.of(State.class, StoreInputChange.class, ArrayList.class);
    }

    @Override
    public void run(String input, StateHandle<State, StoreInputChange> state, ProcedureCtx ctx)
            throws ProcedureException {
        state.apply(new StoreInputChange(input));
    }

    public record State(List<String> seen) {}
}
