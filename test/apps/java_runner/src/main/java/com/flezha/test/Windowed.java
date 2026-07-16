package com.flezha.test;

import com.flezha.sdk.authoring.InitState;
import com.flezha.sdk.authoring.Operation;
import com.flezha.sdk.authoring.SegmentFor;
import java.util.List;
import java.util.Optional;

/**
 * Static-segmented operation whose rolling-window length is encoded in the
 * segment key, supplied via {@code @InitState} (the macro default
 * {@code new State()} cannot derive it). Mirrors the market {@code sma}/{@code ohlc}
 * operations and is the Java parity twin of the Rust {@code Windowed} sugar operation.
 */
@Operation(id = "java_windowed", keys = {"win_2", "win_3"})
public class Windowed {

    @SegmentFor
    public List<String> segmentFor(WindowInput input) {
        return List.of("win_2", "win_3");
    }

    @InitState
    public WindowedState initState(Optional<String> operationKey) {
        WindowedState s = new WindowedState();
        s.window = operationKey.equals(Optional.of("win_2")) ? 2 : 3;
        return s;
    }

    public Optional<Double> run(WindowInput input, WindowedStateHandle state) {
        int window = state.get().window;
        state.pushCloses(input.key(), input.value());
        List<Double> c = state.get().closes.get(input.key());
        if (c.size() > window) {
            state.drainClosesFront(input.key(), c.size() - window);
        }
        if (c.size() < window) {
            return Optional.empty();
        }
        double sum = 0.0;
        for (double v : c) {
            sum += v;
        }
        return Optional.of(sum / window);
    }
}
