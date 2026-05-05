package com.flezha.test;

import com.flezha.sdk.shard.Shard;
import com.flezha.sdk.shard.ShardCtx;
import com.flezha.sdk.shard.ShardException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

public final class DoNothingShard implements Shard<String, DoNothingShard.State, byte[]> {

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
    public State initState(Optional<String> shardKey) {
        return new State();
    }

    @Override
    public void run(String input, State state, ShardCtx ctx) throws ShardException {
        ctx.emit(input.getBytes(StandardCharsets.UTF_8));
    }

    public record State() {
    }
}
