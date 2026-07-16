package com.flezha.test;

import com.flezha.sdk.operation.Operation;
import com.flezha.sdk.operation.OperationCtx;
import com.flezha.sdk.operation.OperationException;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

public final class DoNothingOperation
        implements Operation<String, DoNothingOperation.State, byte[], NoChange, WallClock, Void> {

    @Override
    public String name() {
        return "do_nothing_operation";
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
    public State initState(Optional<String> operationKey, Optional<RestoreInitPayload> initPayload) {
        return new State();
    }

    @Override
    public void run(String input, StateHandle<State, NoChange> state, OperationCtx ctx) throws OperationException {
        ctx.emit(input.getBytes(StandardCharsets.UTF_8));
    }

    public record State() {
    }
}
