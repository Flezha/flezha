package com.flezha.examples.local;

import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.util.Optional;
import javax.annotation.Nonnull;

/** One line in, one emit per word out. */
public final class Split implements Procedure<String, Void, String, NoChange, WallClock, Void> {

    @Override
    @Nonnull
    public String name() {
        return "split";
    }

    @Override
    public Void initState(@Nonnull Optional<String> entityKey, @Nonnull Optional<RestoreInitPayload> initPayload) {
        return null;
    }

    @Override
    public void run(@Nonnull String line, @Nonnull StateHandle<Void, NoChange> state, @Nonnull ProcedureCtx ctx)
            throws ProcedureException {
        for (String word : line.split("\\s+")) {
            if (!word.isEmpty()) {
                ctx.emit(word.toLowerCase());
            }
        }
    }
}
