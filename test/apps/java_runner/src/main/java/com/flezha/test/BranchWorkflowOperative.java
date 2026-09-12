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
 * Mirror of the Rust test app's {@code branch_workflow}: the procedure TREE.
 *
 * <pre>
 *     BranchA ─┬─▶ BranchB ─▶ BranchD
 *              └─▶ BranchC
 * </pre>
 *
 * Every procedure commits durable state and compensates by emitting an undo
 * record, so both tree behaviours are observable at one sink: crash-resume
 * (BranchD blocks, so the respawn re-runs D alone and the sibling's {@code c:}
 * marker appears exactly once) and compensation (with {@code FLEZHA_BRANCH_FAIL}
 * armed BranchD fails, and the rollback unwinds the two branches before the root
 * they share).
 */
public final class BranchWorkflowOperative implements WorkflowOperative {

    static final String BRANCH_FAIL_ENV = "FLEZHA_BRANCH_FAIL";

    @Override
    public String factoryId() {
        return "branch_workflow";
    }

    /**
     * Fail fast: the compensation scenario is about the rollback order, not the
     * ladder that precedes it.
     */
    @Override
    public int retryAttempts() {
        return 0;
    }

    @Override
    public void procedures(ChannelBuilder c) {
        // One channel, two consumers: THE fan-out. Both branches read exactly
        // what BranchA released on it (held handle, wired twice).
        var mid = c.input(String.class, "in")
                .into(BranchA.class)
                .named("mid")
                .sink("branch_sink", s -> ("a:" + s).getBytes(StandardCharsets.UTF_8));
        mid.into(BranchB.class)
                .named("bmid")
                .sink("branch_sink", s -> ("b:" + s).getBytes(StandardCharsets.UTF_8))
                .into(BranchD.class)
                .named("d_out")
                .sink("branch_sink");
        mid.into(BranchC.class).named("c_out").sink("branch_sink");
    }

    /** The shared root. */
    public static final class BranchA extends DurableStep {

        @Override
        public String name() {
            return "branch_a";
        }

        @Override
        public boolean compensateOverridden() {
            return true;
        }

        @Override
        public void compensate(List<String> completedOutput, StateHandle<SeenState, Insert> state, ProcedureCtx ctx)
                throws ProcedureException {
            ctx.emit("undo-a");
        }
    }

    /** The deep branch's upper half. */
    public static final class BranchB extends DurableStep {

        @Override
        public String name() {
            return "branch_b";
        }

        @Override
        public boolean compensateOverridden() {
            return true;
        }

        @Override
        public void compensate(List<String> completedOutput, StateHandle<SeenState, Insert> state, ProcedureCtx ctx)
                throws ProcedureException {
            ctx.emit("undo-b");
        }
    }

    /** The sibling leaf: emits straight to the sink, and its undo does too. */
    public static final class BranchC extends DurableStep {

        @Override
        public String name() {
            return "branch_c";
        }

        @Override
        public void run(String input, StateHandle<SeenState, Insert> state, ProcedureCtx ctx)
                throws ProcedureException {
            state.apply(new Insert(name()));
            ctx.emit("c:" + input);
        }

        @Override
        public boolean compensateOverridden() {
            return true;
        }

        @Override
        public void compensate(List<String> completedOutput, StateHandle<SeenState, Insert> state, ProcedureCtx ctx)
                throws ProcedureException {
            ctx.emit("undo-c");
        }
    }

    /** The deep leaf under BranchB: blocks (crash-resume), or fails (compensation). */
    public static final class BranchD extends StatelessStep {

        @Override
        public String name() {
            return "branch_d";
        }

        @Override
        public void run(String input, StateHandle<State, NoChange> state, ProcedureCtx ctx)
                throws ProcedureException {
            if (System.getenv(BRANCH_FAIL_ENV) != null) {
                throw new ProcedureException("branch_d failed");
            }
            WorkflowE2eGate.blockUntilReleased();
            ctx.emit("d:" + input);
        }
    }
}
