package com.flezha.test;

import com.flezha.sdk.authoring.InitState;
import com.flezha.sdk.authoring.PartitionFor;
import com.flezha.sdk.authoring.Shard;
import java.util.List;
import java.util.Optional;

/**
 * Static-partitioned shard whose rolling-window length is encoded in the shard
 * key, supplied via {@code @InitState} (the macro default {@code new State()}
 * cannot derive it). Mirrors the market {@code sma}/{@code ohlc} shards and is
 * the Java parity twin of the Rust {@code Windowed} sugar shard.
 */
@Shard(id = "java_windowed", keys = {"win_2", "win_3"})
public class Windowed {

    @PartitionFor
    public List<String> partition(WindowInput input) {
        return List.of("win_2", "win_3");
    }

    @InitState
    public WindowedState initState(Optional<String> shardKey) {
        WindowedState s = new WindowedState();
        s.window = shardKey.equals(Optional.of("win_2")) ? 2 : 3;
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
