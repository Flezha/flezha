package com.flezha.test;

import com.flezha.sdk.shard.Shard;
import com.flezha.sdk.shard.ShardCtx;
import com.flezha.sdk.shard.ShardException;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.util.Optional;

public final class CompareFilterShard
        implements Shard<String, CompareFilterShard.State, String, NoChange, WallClock, Void> {

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
    public State initState(Optional<String> shardKey, Optional<RestoreInitPayload> initPayload) {
        return new State("default", false);
    }

    @Override
    public void run(String input, StateHandle<State, NoChange> state, ShardCtx ctx) throws ShardException {
        if (state.get().comparable().equals(input)) {
            // matched — filter passes the input through (no-op here).
        }
    }

    public record State(String comparable, boolean inverted) {
    }
}
