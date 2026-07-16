package com.flezha.test;

import com.flezha.sdk.operation.Operation;
import com.flezha.sdk.operation.OperationCtx;
import com.flezha.sdk.operation.OperationException;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.util.List;
import java.util.Optional;

/**
 * Java equivalent of the Rust SDK's {@code CounterOperation}. Each input is a
 * {@code Long} delta; the Operation records a {@link CounterChange} via
 * {@code state.apply}, and the {@code applyChange} default mutates
 * {@code state.value}.
 */
public final class CounterOperation
        implements Operation<Long, CounterOperation.State, Void, CounterChange, WallClock, Void> {

    public static final class State {
        public long value;
    }

    @Override
    public String name() {
        return "counter_operation";
    }

    @Override
    public Class<Long> inputType() {
        return Long.class;
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
    public State initState(Optional<String> operationKey, Optional<RestoreInitPayload> initPayload) {
        return new State();
    }

    @Override
    public void applyChange(State state, CounterChange change) {
        state.value += change.delta();
    }

    @Override
    public List<Class<?>> kryoTypes() {
        return List.of(State.class, CounterChange.class);
    }

    @Override
    public void run(Long input, StateHandle<State, CounterChange> state, OperationCtx ctx)
            throws OperationException {
        state.apply(new CounterChange(input));
    }
}
