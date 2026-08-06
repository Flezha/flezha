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
 * Bridges the controller's {@code String} entry channel into the {@link CounterProcedure}
 * by parsing the input as a {@code long} and recording a {@link CounterChange}.
 *
 * <p>Entry channels are restricted to {@code byte[] | String} in v1; this
 * adapter Procedure exists so {@code StatefulCounterUnboundedOperative} can be
 * wired through an entry channel without changing the entry-channel type policy.
 */
public final class IntInputAdapterProcedure
        implements Procedure<String, CounterProcedure.State, Void, CounterChange, WallClock, Void> {

    @Override
    public String name() {
        return "int_input_adapter";
    }

    @Override
    public Class<String> inputType() {
        return String.class;
    }

    @Override
    public Class<Void> outputType() {
        return Void.class;
    }

    @Override
    public Class<CounterChange> changeType() {
        return CounterChange.class;
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
    public CounterProcedure.State initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
        return new CounterProcedure.State();
    }

    @Override
    public void applyChange(CounterProcedure.State state, CounterChange change) {
        state.value += change.delta();
    }

    @Override
    public List<Class<?>> kryoTypes() {
        return List.of(CounterProcedure.State.class, CounterChange.class);
    }

    @Override
    public void run(String input, StateHandle<CounterProcedure.State, CounterChange> state, ProcedureCtx ctx)
            throws ProcedureException {
        long delta;
        try {
            delta = Long.parseLong(input);
        } catch (NumberFormatException e) {
            return;
        }
        state.apply(new CounterChange(delta));
    }
}
