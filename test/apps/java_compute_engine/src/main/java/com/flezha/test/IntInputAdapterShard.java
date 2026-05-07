package com.flezha.test;

import com.flezha.sdk.shard.Shard;
import com.flezha.sdk.shard.ShardCtx;
import com.flezha.sdk.shard.ShardException;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.util.List;
import java.util.Optional;

/**
 * Bridges the agent's {@code String} entry channel into the {@link CounterShard}
 * by parsing the input as a {@code long} and recording a {@link CounterChange}.
 *
 * <p>Entry channels are restricted to {@code byte[] | String} in v1; this
 * adapter Shard exists so {@code StatefulCounterRelay} can be wired through
 * an entry channel without changing the entry-channel type policy.
 */
public final class IntInputAdapterShard
        implements Shard<String, CounterShard.State, Void, CounterChange, WallClock, Void> {

    @Override
    public String name() {
        return "int_input_adapter";
    }

    @Override
    public Class<String> inputType() {
        return String.class;
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
    public CounterShard.State initState(Optional<String> shardKey, Optional<RestoreInitPayload> initPayload) {
        return new CounterShard.State();
    }

    @Override
    public void applyChange(CounterShard.State state, CounterChange change) {
        state.value += change.delta();
    }

    @Override
    public List<Class<?>> kryoTypes() {
        return List.of(CounterShard.State.class, CounterChange.class);
    }

    @Override
    public void run(String input, StateHandle<CounterShard.State, CounterChange> state, ShardCtx ctx)
            throws ShardException {
        long delta;
        try {
            delta = Long.parseLong(input);
        } catch (NumberFormatException e) {
            return;
        }
        state.apply(new CounterChange(delta));
    }
}
