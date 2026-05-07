package com.flezha.test;

import com.flezha.sdk.shard.Shard;
import com.flezha.sdk.shard.ShardCtx;
import com.flezha.sdk.shard.ShardException;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

public final class DoNothingShard
        implements Shard<String, DoNothingShard.State, byte[], NoChange, WallClock, Void> {

    @Override
    public String name() {
        return "do_nothing_shard";
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
    public State initState(Optional<String> shardKey, Optional<RestoreInitPayload> initPayload) {
        return new State();
    }

    @Override
    public void run(String input, StateHandle<State, NoChange> state, ShardCtx ctx) throws ShardException {
        ctx.emit(input.getBytes(StandardCharsets.UTF_8));
    }

    public record State() {
    }
}
