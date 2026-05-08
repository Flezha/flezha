package com.flezha.test;

import com.flezha.sdk.shard.Shard;
import com.flezha.sdk.shard.ShardCtx;
import com.flezha.sdk.shard.ShardException;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.util.List;
import java.util.Optional;

/** Shard backing {@link CoalescingCounterRelay}: parses {@code "interval:ts:delta"}. */
public final class TickShard
        implements Shard<String, TickShard.State, Void, TickChange, WallClock, Void> {

    public static final class State {
        public long value;
    }

    @Override public String name() { return "tick_shard"; }
    @Override public Class<String> inputType() { return String.class; }
    @Override public Class<Void> outputType() { return Void.class; }
    @Override public Class<TickChange> changeType() { return TickChange.class; }
    @Override public Class<WallClock> clockType() { return WallClock.class; }
    @Override public Class<Void> configType() { return Void.class; }
    @Override public List<Class<?>> kryoTypes() { return List.of(State.class, TickChange.class); }

    @Override
    public State initState(Optional<String> shardKey, Optional<RestoreInitPayload> initPayload) {
        return new State();
    }

    @Override
    public void applyChange(State state, TickChange change) {
        state.value += change.delta();
    }

    @Override
    public void run(String input, StateHandle<State, TickChange> state, ShardCtx ctx)
            throws ShardException {
        String[] parts = input.split(":", 3);
        if (parts.length != 3) {
            return;
        }
        try {
            int interval = Integer.parseInt(parts[0]);
            long ts = Long.parseLong(parts[1]);
            long delta = Long.parseLong(parts[2]);
            state.apply(new TickChange(interval, ts, delta));
        } catch (NumberFormatException ignored) {
            // drop malformed input
        }
    }
}
