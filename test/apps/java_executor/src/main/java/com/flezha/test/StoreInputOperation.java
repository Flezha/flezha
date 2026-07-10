package com.flezha.test;

import com.flezha.sdk.operation.Operation;
import com.flezha.sdk.operation.OperationCtx;
import com.flezha.sdk.operation.OperationException;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class StoreInputOperation
        implements Operation<String, StoreInputOperation.State, String, StoreInputChange, WallClock, Void> {

    @Override
    public String name() {
        return "store_input_operation";
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
    public State initState(Optional<String> operationKey, Optional<RestoreInitPayload> initPayload) {
        return new State(new ArrayList<>(), new ArrayList<>());
    }

    @Override
    public void applyChange(State state, StoreInputChange change) {
        state.textInputs().add(change.text());
    }

    @Override
    public List<Class<?>> kryoTypes() {
        return List.of(State.class, StoreInputChange.class, ArrayList.class);
    }

    @Override
    public void run(String input, StateHandle<State, StoreInputChange> state, OperationCtx ctx)
            throws OperationException {
        state.apply(new StoreInputChange(input));
    }

    public record State(List<byte[]> binaryInputs, List<String> textInputs) {
    }
}
