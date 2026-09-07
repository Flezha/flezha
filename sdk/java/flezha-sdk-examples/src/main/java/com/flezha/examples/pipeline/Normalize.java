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
 * The first stage: normalises the raw line before any later stage observes it.
 *
 * <p>Deliberately trivial, so that the wiring rather than the logic is what the example teaches. A
 * stage has no knowledge of what follows it, which is what allows one to be reordered or reused in
 * another graph without modification.
 */
public final class Normalize implements Procedure<String, Normalize.None, String, NoChange, WallClock, Void> {

    public record None() {}

    @Override
    @Nonnull
    public String name() {
        return "normalize";
    }

    @Override
    @Nonnull
    public None initState(@Nonnull Optional<String> entityKey, @Nonnull Optional<RestoreInitPayload> initPayload) {
        return new None();
    }

    @Override
    public void run(@Nonnull String input, @Nonnull StateHandle<None, NoChange> state, @Nonnull ProcedureCtx ctx)
            throws ProcedureException {
        ctx.emit(input.trim().toLowerCase());
    }
}
