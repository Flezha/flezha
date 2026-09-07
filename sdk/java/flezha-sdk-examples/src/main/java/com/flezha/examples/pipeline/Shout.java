package com.flezha.examples.pipeline;

import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.util.Optional;
import javax.annotation.Nonnull;

/**
 * The second stage. Its input is whatever the first stage emitted, so the declared types must
 * agree: {@code Normalize} emits a {@code String} and this consumes one.
 *
 * <p>Nothing here indicates a position in a chain. From this class's perspective messages simply
 * arrive, which is the same contract the first stage has.
 */
public final class Shout implements Procedure<String, Shout.None, String, NoChange, WallClock, Void> {

    public record None() {}

    @Override
    @Nonnull
    public String name() {
        return "shout";
    }

    @Override
    @Nonnull
    public None initState(@Nonnull Optional<String> entityKey, @Nonnull Optional<RestoreInitPayload> initPayload) {
        return new None();
    }

    @Override
    public void run(@Nonnull String input, @Nonnull StateHandle<None, NoChange> state, @Nonnull ProcedureCtx ctx)
            throws ProcedureException {
        ctx.emit(input.toUpperCase() + "!");
    }
}
