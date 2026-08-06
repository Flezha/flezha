package com.flezha.test;

import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.state.Change;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Hand-written twin of {@link RollingWindow}: the exact {@code Procedure} /
 * {@code Change} impl the sugar expands to (variant names differ; behaviour
 * does not).
 */
public final class RollingWindowTwin
        implements Procedure<WindowInput, RollingWindowTwin.State, Double, RollingWindowTwin.WindowChange, WallClock, Void> {

    static final int N = 3;

    public static final class State {
        public Map<String, Deque<Double>> window = new HashMap<>();
    }

    public sealed interface WindowChange extends Change {
        record PushBack(String key, double value) implements WindowChange {}

        record PopFront(String key, int n) implements WindowChange {}
    }

    @Override
    public String name() {
        return "rolling_window_twin";
    }

    @Override
    public Class<WindowInput> inputType() {
        return WindowInput.class;
    }

    @Override
    public Class<Double> outputType() {
        return Double.class;
    }

    @Override
    public Class<WindowChange> changeType() {
        return WindowChange.class;
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
    public void applyChange(State state, WindowChange change) {
        if (change instanceof WindowChange.PushBack c) {
            state.window.computeIfAbsent(c.key(), k -> new ArrayDeque<>()).addLast(c.value());
        } else if (change instanceof WindowChange.PopFront c) {
            Deque<Double> d = state.window.computeIfAbsent(c.key(), k -> new ArrayDeque<>());
            for (int i = 0; i < Math.min(c.n(), d.size()); i++) {
                d.pollFirst();
            }
        }
    }

    @Override
    public List<Class<?>> kryoTypes() {
        return List.of(State.class, WindowChange.class, WindowChange.PushBack.class, WindowChange.PopFront.class);
    }

    @Override
    public void run(WindowInput input, StateHandle<State, WindowChange> state, ProcedureCtx ctx) throws ProcedureException {
        state.apply(new WindowChange.PushBack(input.key(), input.value()));
        Deque<Double> w = state.get().window.get(input.key());
        if (w.size() > N) {
            state.apply(new WindowChange.PopFront(input.key(), w.size() - N));
        }
        if (w.size() < N) {
            return;
        }
        double sum = 0.0;
        for (double v : w) {
            sum += v;
        }
        ctx.emit(sum / N);
    }
}
