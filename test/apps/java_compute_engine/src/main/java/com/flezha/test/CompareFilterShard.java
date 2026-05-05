package com.flezha.test;

import com.flezha.sdk.shard.Shard;
import com.flezha.sdk.shard.ShardCtx;
import com.flezha.sdk.shard.ShardException;
import java.util.Optional;

public final class CompareFilterShard implements Shard<String, CompareFilterShard.State, String> {

    @Override
    public String name() {
        return "compare_filter_shard";
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
    public State initState(Optional<String> shardKey) {
        return new State("default", false);
    }

    @Override
    public void run(String input, State state, ShardCtx ctx) throws ShardException {
        // Comparison logic mirrors the Rust shape — no emit; the State carries
        // the comparison result for tests that read state via ProcessorState.
        if (state.comparable().equals(input)) {
            // matched — equivalent to the legacy pipeToAgent(input) path.
        }
    }

    public record State(String comparable, boolean inverted) {
    }
}
