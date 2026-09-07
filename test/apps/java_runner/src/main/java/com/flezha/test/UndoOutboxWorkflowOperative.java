package com.flezha.test;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.operative.WorkflowOperative;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Mirror of the Rust test app's {@code undo_outbox_workflow}: the compensation
 * half of the transactional outbox. Proc UA commits, emits its forward output on
 * a hybrid plugin channel, and declares a {@code compensate} hook that EMITS an
 * undo record — plugin-bound, so it is staged and rides a {@code compensated}
 * journal entry rather than the wire. Proc C fails immediately, so the chain
 * always rolls back; the hook BLOCKS until the release env is set, so on the
 * first runner the undo is emitted-but-never-journaled when the SIGKILL lands.
 */
public final class UndoOutboxWorkflowOperative implements WorkflowOperative {

    @Override
    public String factoryId() {
        return "undo_outbox_workflow";
    }

    @Override
    public void procedures(ChannelBuilder c) {
        c.input(String.class, "in")
                .into(UndoOutboxA.class)
                .named("mid")
                .codec()
                .sink("undo_sink", s -> ("out:" + s).getBytes(StandardCharsets.UTF_8))
                .into(UndoC.class);
    }

    /** Proc UA: durable + compensable; the hook blocks, then emits the undo record. */
    public static final class UndoOutboxA extends DurableStep {

        @Override
        public String name() {
            return "undo_outbox_a";
        }

        @Override
        public boolean compensateOverridden() {
            return true;
        }

        @Override
        public void compensate(List<String> completedOutput, StateHandle<SeenState, Insert> state, ProcedureCtx ctx)
                throws ProcedureException {
            WorkflowE2eGate.blockUntilReleased();
            ctx.emit("undone");
        }
    }

    /** Proc C: fails immediately, so the chain always rolls back. */
    public static final class UndoC extends StatelessStep {

        @Override
        public String name() {
            return "undo_outbox_c";
        }

        @Override
        public void run(String input, StateHandle<State, NoChange> state, ProcedureCtx ctx)
                throws ProcedureException {
            throw new ProcedureException("undo_outbox_c always fails");
        }
    }
}
