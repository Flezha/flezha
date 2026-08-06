package com.flezha.test;

import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

public final class DoNothingProcedure
        implements Procedure<String, DoNothingProcedure.State, byte[], NoChange, WallClock, Void> {

    @Override
    public String name() {
        return "do_nothing_procedure";
    }

    @Override
    public Class<String> inputType() {
        return String.class;
    }

    @Override
    public Class<byte[]> outputType() {
        return byte[].class;
    }

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

    @Override
    public State initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
        return new State();
    }

    @Override
    public void run(String input, StateHandle<State, NoChange> state, ProcedureCtx ctx) throws ProcedureException {
        ctx.emit(input.getBytes(StandardCharsets.UTF_8));
    }

    public record State() {
    }
}
