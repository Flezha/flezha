package com.flezha.sdk.runtime.relay;

import com.flezha.sdk.shard.Shard;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.WallClock;
import java.util.Optional;

/**
 * Test-only base providing the persistence-related accessors with default
 * stateless values ({@link NoChange}, {@link WallClock}, {@link Void}) so the
 * runtime-module test fixtures stay terse.
 */
public abstract class StatelessTestShard<Input, State, Output>
        implements Shard<Input, State, Output, NoChange, WallClock, Void> {

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

    /**
     * Default fresh-start initializer that ignores the restore payload — most
     * test shards don't care about restore. Subclasses override the variant
     * they need.
     */
    @Override
    public State initState(Optional<String> shardKey, Optional<RestoreInitPayload> initPayload) {
        return initState(shardKey);
    }

    /** Convenience hook for subclasses that don't care about the restore payload. */
    protected abstract State initState(Optional<String> shardKey);
}
