package com.flezha.examples.agent;

import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.state.Change;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;

/** One turn of the conversation. */
public final class ReplyProcedure
        implements Procedure<String, ReplyProcedure.Conversation, String, ReplyProcedure.Turn, WallClock, Void> {

    /** The whole conversation. Being durable is what makes the agent resumable. */
    public static final class Conversation {
        public List<String> turns = new ArrayList<>();
    }

    public record Turn(String who, String text) implements Change {}

    @Override
    @Nonnull
    public String name() {
        return "reply";
    }

    /** The default covers Turn and Conversation. ArrayList is inside the state, so add it. */
    @Override
    @Nonnull
    public List<Class<?>> kryoTypes() {
        return List.of(Turn.class, Conversation.class, ArrayList.class);
    }

    @Override
    public void applyChange(@Nonnull Conversation state, @Nonnull Turn turn) {
        state.turns.add(turn.who() + ": " + turn.text());
    }

    @Override
    @Nonnull
    public Conversation initState(
            @Nonnull Optional<String> entityKey, @Nonnull Optional<RestoreInitPayload> initPayload) {
        return new Conversation();
    }

    @Override
    public void run(@Nonnull String message, @Nonnull StateHandle<Conversation, Turn> state, @Nonnull ProcedureCtx ctx)
            throws ProcedureException {
        state.apply(new Turn("user", message));

        // A real agent calls a model here, with state.get().turns as the history. Keeping the
        // reply a function of that history is what lets a failed turn be re-run safely.
        String answer = "you said: " + message + " (turn " + state.get().turns.size() + ")";

        state.apply(new Turn("agent", answer));
        ctx.emit(answer);
    }
}
