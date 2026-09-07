package com.flezha.examples.local;

import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.state.Change;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.util.Optional;
import javax.annotation.Nonnull;

/** The running count for one word. Each distinct word is its own entity. */
public final class Tally implements Procedure<String, Tally.Count, String, Tally.Seen, WallClock, Void> {

    public static final class Count {
        public long seen;
    }

    /** One occurrence of the word this entity is. */
    public record Seen() implements Change {}

    @Override
    @Nonnull
    public String name() {
        return "tally";
    }

    @Override
    @Nonnull
    public Count initState(@Nonnull Optional<String> entityKey, @Nonnull Optional<RestoreInitPayload> initPayload) {
        return new Count();
    }

    @Override
    public void applyChange(@Nonnull Count state, @Nonnull Seen change) {
        state.seen++;
    }

    @Override
    public void run(@Nonnull String word, @Nonnull StateHandle<Count, Seen> state, @Nonnull ProcedureCtx ctx)
            throws ProcedureException {
        state.apply(new Seen());
        ctx.emit(word + " " + state.get().seen);
    }
}
