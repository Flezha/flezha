package com.flezha.test;

import com.flezha.sdk.authoring.Operation;
import java.util.Deque;
import java.util.Optional;

/**
 * Canonical keyed-collection reference operation (authoring-contract §3 / §6): a
 * rolling per-key window. Uses {@code @Journal(MAP_DEQUE)} + an
 * {@code Optional<Output>} warmup gate. Twin: {@link RollingWindowTwin}.
 */
@Operation(id = "java_rolling_window")
public class RollingWindow {
    static final int N = 3;

    public Optional<Double> run(WindowInput input, RollingWindowStateHandle state) {
        state.pushBackWindow(input.key(), input.value());
        Deque<Double> w = state.get().window.get(input.key());
        if (w.size() > N) {
            state.popFrontWindow(input.key(), w.size() - N);
        }
        if (w.size() < N) {
            return Optional.empty();
        }
        double sum = 0.0;
        for (double v : w) {
            sum += v;
        }
        return Optional.of(sum / N);
    }
}
