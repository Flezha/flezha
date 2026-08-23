package com.flezha.test;

import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.state.Change;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * The procedures the durable chat loops share ({@code chat_loop_workflow},
 * {@code stateless_loop_workflow}, {@code chat_rewind_workflow} and the bench
 * loop), mirroring the Rust test app's {@code ChatTurn} / {@code ChatReply} /
 * {@code ChatReplyUndoable}.
 */
public final class ChatSteps {

    /** The turn whose payload BLOCKS while the release env is unset. */
    static final String CRASH_TURN = "mid-crash";

    private ChatSteps() {}

    /**
     * Commits the turn to Entity state and passes it on. The change is keyed by
     * the TURN, so the WAL record count is the number of DISTINCT turns that ran
     * — a re-run of a committed iteration would show up.
     */
    public static final class Turn extends DurableStep {

        @Override
        public String name() {
            return "chat_turn";
        }

        @Override
        public void run(String input, StateHandle<SeenState, Insert> state, ProcedureCtx ctx)
                throws ProcedureException {
            if (CRASH_TURN.equals(input)) {
                WorkflowE2eGate.blockUntilReleased();
            }
            state.apply(new Insert(input));
            ctx.emit(input);
        }
    }

    /** The stateless terminal step: emits {@code reply:<input>}. */
    public static final class Reply extends StatelessStep {

        @Override
        public String name() {
            return "chat_reply";
        }

        @Override
        public void run(String input, StateHandle<State, NoChange> state, ProcedureCtx ctx)
                throws ProcedureException {
            ctx.emit("reply:" + input);
        }
    }

    /** What {@link ReplyUndoable} has said, newest last — the undo handle. */
    public static final class ReplyLog {
        public final List<String> said = new ArrayList<>();
    }

    /** Appends a turn to a {@link ReplyLog}, or pops the last one on a rewind. */
    public record ReplyChange(String turn, boolean retract) implements Change {

        static ReplyChange said(String turn) {
            return new ReplyChange(turn, false);
        }

        static ReplyChange retracted() {
            return new ReplyChange("", true);
        }
    }

    /**
     * The replying procedure a rewind can undo. A rewind's compensation is
     * rebuilt from the journal and is handed NO retained outputs, so the hook
     * reads the last thing it said out of Entity State, retracts it, and pops it
     * — unwinding in reverse iteration order retracts the turns in reverse.
     */
    public static final class ReplyUndoable
            implements Procedure<String, ReplyLog, String, ReplyChange, WallClock, Void> {

        @Override
        public String name() {
            return "chat_reply_undoable";
        }

        @Override
        public ReplyLog initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
            return new ReplyLog();
        }

        @Override
        public void applyChange(ReplyLog state, ReplyChange change) {
            if (change.retract()) {
                if (!state.said.isEmpty()) {
                    state.said.remove(state.said.size() - 1);
                }
            } else {
                state.said.add(change.turn());
            }
        }

        @Override
        public List<Class<?>> kryoTypes() {
            return List.of(ReplyLog.class, ReplyChange.class, ArrayList.class);
        }

        @Override
        public void run(String input, StateHandle<ReplyLog, ReplyChange> state, ProcedureCtx ctx)
                throws ProcedureException {
            state.apply(ReplyChange.said(input));
            ctx.emit("reply:" + input);
        }

        @Override
        public boolean compensateOverridden() {
            return true;
        }

        @Override
        public void compensate(
                List<String> completedOutput, StateHandle<ReplyLog, ReplyChange> state, ProcedureCtx ctx)
                throws ProcedureException {
            List<String> said = state.get().said;
            if (said.isEmpty()) {
                return;
            }
            String turn = said.get(said.size() - 1);
            state.apply(ReplyChange.retracted());
            ctx.emit("retract:" + turn);
        }
    }
}
