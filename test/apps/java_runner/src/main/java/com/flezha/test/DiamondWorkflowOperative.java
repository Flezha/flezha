package com.flezha.test;

import com.flezha.sdk.operative.WorkflowOperative;
import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.StateHandle;
import java.nio.charset.StandardCharsets;

/**
 * The fan-in diamond for the crash-resume e2e, the
 * Java mirror of the Rust test app's {@code diamond_workflow}:
 *
 * <pre>
 *     DiamA ─┬─▶ DiamB1 ─▶ DiamB2(blocks) ─┬─▶ DiamJoin
 *            └─▶ DiamC ────────────────────┘
 * </pre>
 *
 * The join accumulates BOTH producers' outputs and is dispatched exactly once,
 * after both commit. DiamB2 blocks BEFORE its durable change + emit — one
 * level below its sibling arm, because siblings commit at one level barrier —
 * so a SIGKILL lands with A, B1 and C committed and B2 mid-run: the respawn
 * re-runs B2 alone, and the join runs once with C's JOURNALED handoff banked
 * and B2's fresh output appended.
 */
public final class DiamondWorkflowOperative implements WorkflowOperative {

    @Override
    public String factoryId() {
        return "diamond_workflow";
    }

    @Override
    public void procedures(ChannelBuilder c) {
        // Held handle wired twice = fan-out; two streams naming DiamJoin
        // (unnamed, same class) = the fan-in.
        var fan = c.input(String.class, "in")
                .into(DiamA.class)
                .named("dfan")
                .sink("diamond_sink", s -> ("a:" + s).getBytes(StandardCharsets.UTF_8));
        fan.into(DiamB1.class)
                .named("db1")
                .into(DiamB2.class)
                .named("db2")
                .sink("diamond_sink", s -> s.getBytes(StandardCharsets.UTF_8))
                .into(DiamJoin.class);
        fan.into(DiamC.class)
                .named("dc")
                .sink("diamond_sink", s -> s.getBytes(StandardCharsets.UTF_8))
                .into(DiamJoin.class)
                .named("j_out")
                .sink("diamond_sink");
    }

    /** The shared root: commits durable state and fans out unchanged. */
    public static final class DiamA extends DurableStep {
        @Override
        public String name() {
            return "diam_a";
        }
    }

    /** The pass-through upper half of the blocked arm. */
    public static final class DiamB1 extends DurableStep {
        @Override
        public String name() {
            return "diam_b1";
        }
    }

    /**
     * The blocked producer: parks BEFORE its durable change + emit, so the
     * kill window is deterministic — B2 is always the uncommitted producer
     * while the sibling arm committed.
     */
    public static final class DiamB2 extends DurableStep {
        @Override
        public String name() {
            return "diam_b2";
        }

        @Override
        public void run(String input, StateHandle<SeenState, Insert> state, ProcedureCtx ctx)
                throws ProcedureException {
            WorkflowE2eGate.blockUntilReleased();
            state.apply(new Insert(name()));
            ctx.emit("b:" + input);
        }
    }

    /** The fast producer: committed before the SIGKILL lands. */
    public static final class DiamC extends DurableStep {
        @Override
        public String name() {
            return "diam_c";
        }

        @Override
        public void run(String input, StateHandle<SeenState, Insert> state, ProcedureCtx ctx)
                throws ProcedureException {
            state.apply(new Insert(name()));
            ctx.emit("c:" + input);
        }
    }

    /** The join: one {@code j:}-tagged record per accumulated contribution. */
    public static final class DiamJoin extends StatelessStep {
        @Override
        public String name() {
            return "diam_join";
        }

        @Override
        public void run(String input, StateHandle<State, NoChange> state, ProcedureCtx ctx)
                throws ProcedureException {
            ctx.emit("j:" + input);
        }
    }
}
