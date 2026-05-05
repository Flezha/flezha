package com.flezha.test;

import com.flezha.sdk.shard.Shard;
import com.flezha.sdk.shard.ShardCtx;
import com.flezha.sdk.shard.ShardException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class StoreInputShard implements Shard<String, StoreInputShard.State, String> {

    @Override
    public String name() {
        return "store_input_shard";
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
        return new State(new ArrayList<>(), new ArrayList<>());
    }

    @Override
    public void run(String input, State state, ShardCtx ctx) throws ShardException {
        state.textInputs().add(input);
    }

    public record State(List<byte[]> binaryInputs, List<String> textInputs) {
    }
}
