package com.flezha.test;

import com.flezha.sdk.operation.Operation;
import com.flezha.sdk.operation.OperationCtx;
import com.flezha.sdk.operation.OperationException;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.util.Optional;

public final class CompareFilterOperation
        implements Operation<String, CompareFilterOperation.State, String, NoChange, WallClock, Void> {

    @Override
    public String name() {
        return "compare_filter_operation";
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
        return new State("default", false);
    }

    @Override
    public void run(String input, StateHandle<State, NoChange> state, OperationCtx ctx) throws OperationException {
        if (state.get().comparable().equals(input)) {
            // matched — filter passes the input through (no-op here).
        }
    }

    public record State(String comparable, boolean inverted) {
    }
}
