package com.flezha.examples;

import com.flezha.sdk.bounded.BoundedOperative;
import com.flezha.sdk.bounded.LoopMode;
import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operation.Operation;
import com.flezha.sdk.operation.OperationCtx;
import com.flezha.sdk.operation.OperationException;
import com.flezha.sdk.state.Change;
import com.flezha.sdk.state.CoalesceKey;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * <b>06 — a long-lived agent: one durable iteration per message, suspended in
 * between.</b>
 *
 * <p>A conversational agent is not a stream and not a one-shot job. It wakes on a
 * message, does a turn's worth of work, and then does nothing — perhaps for days.
 * Holding a runner thread open across that idle time is pure waste; forgetting
 * the conversation is worse.
 *
 * <p>{@link LoopMode#PER_INPUT} runs one iteration of the chain per inbox message
 * and commits at each iteration boundary. When the messages stop, the operative
 * <b>parks</b>: it releases every runner resource and stays registered, holding
 * its journal. The next message — or a {@code wakeAfter} timer — revives it at the
 * iteration it left off.
 *
 * <p>Suspension is not completion. A parked operative has not finished; it is
 * waiting. Its conversation state is durable, so a controller restart or a
 * runner crash while it sleeps costs nothing.
 *
 * <p>The other loop mode, {@link LoopMode#CHAINED}, seeds each iteration from the
 * previous one's output instead of from an inbox message — a refinement loop that
 * runs until it converges (an empty handoff means "done").
 */
public final class E06_DurableChatLoop {

    public static final class ChatOperative implements BoundedOperative {
        @Override
        public String factoryId() {
            return "chat_agent";
        }

        /** One iteration per inbound message. */
        @Override
        public LoopMode loopMode() {
            return LoopMode.PER_INPUT;
        }

        /**
         * Wake on a timer even with no message — for a nudge, a summary, a
         * follow-up. Omit it and the agent wakes only when spoken to.
         */
        @Override
        public Optional<Duration> wakeAfter() {
            return Optional.of(Duration.ofHours(24));
        }

        @Override
        public void operations(ChannelBuilder c) {
            c.input(String.class, "message").into(Reply.class);
            c.channel(String.class, "reply").from(Reply.class).codec().toPlugin("chat_sink");
        }
    }

    /** The whole conversation. Durable: it is what makes the agent resumable. */
    public static final class Conversation {
        public List<String> turns = new ArrayList<>();
    }

    public record Turn(String who, String text) implements Change {
        @Override
        public Optional<CoalesceKey> coalesceKey() {
            return Optional.empty();
        }
    }

    public static final class Reply implements Operation<String, Conversation, String, Turn, WallClock, Void> {

        @Override
        public String name() {
            return "reply";
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
        public Class<Turn> changeType() {
            return Turn.class;
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
        public List<Class<?>> kryoTypes() {
            return List.of(Turn.class, Conversation.class, ArrayList.class);
        }

        @Override
        public void applyChange(Conversation state, Turn turn) {
            state.turns.add(turn.who() + ": " + turn.text());
        }

        @Override
        public Conversation initState(Optional<String> operationKey, Optional<RestoreInitPayload> initPayload) {
            return new Conversation();
        }

        @Override
        public void run(String message, StateHandle<Conversation, Turn> state, OperationCtx ctx)
                throws OperationException {
            state.apply(new Turn("user", message));

            // A real agent would call a model here, with state.get().turns as its
            // history. The reply is a deterministic function of that history, which
            // is what lets the iteration be re-run safely if it fails mid-turn.
            String answer =
                    "you said: " + message + " (turn " + state.get().turns.size() + ")";

            state.apply(new Turn("agent", answer));
            ctx.emit(answer);
        }
    }

    private E06_DurableChatLoop() {}
}
