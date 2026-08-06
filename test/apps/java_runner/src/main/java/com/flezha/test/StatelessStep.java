package com.flezha.test;

import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.WallClock;
import java.util.Optional;

/**
 * Shared base for the crash-resume e2e's stateless terminal {@code String} steps
 * (§7c.3). A concrete proc declares only {@link #name()} and {@code run} (which
 * blocks or fails the chain). Mirrors the Rust test app's {@code stateless_step!}
 * macro.
 */
public abstract class StatelessStep
        implements Procedure<String, StatelessStep.State, String, NoChange, WallClock, Void> {

    public record State() {}

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
    public State initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
        return new State();
    }
}
