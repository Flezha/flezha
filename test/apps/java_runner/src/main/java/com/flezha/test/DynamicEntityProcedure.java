package com.flezha.test;

import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * The dynamic-mode consumer of {@link DynamicEntityStreamOperative}. It
 * declares no keying member of any kind — the edge's {@code keyBy} is what makes
 * it dynamic — and applies a Change per input so the controller writes a WAL
 * file per lazily-created Entity. {@code entityIdleTtl} stays on the procedure:
 * only the KEY moved to the wiring. The TTL comes from {@code DYN_TEST_TTL_MS}
 * (the e2e sets 200 ms to exercise reap + delete; the bench leaves it unset).
 */
public final class DynamicEntityProcedure
        implements Procedure<String, DynamicEntityProcedure.State, byte[], StoreInputChange, WallClock, Void> {

    @Override
    public String name() {
        return "dynamic_entity_procedure";
    }

    @Override
    public Optional<Duration> entityIdleTtl() {
        return TestEnv.optLong("DYN_TEST_TTL_MS").map(Duration::ofMillis);
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
        ctx.emit(input.getBytes(StandardCharsets.UTF_8));
    }

    public record State(List<String> seen) {}
}
