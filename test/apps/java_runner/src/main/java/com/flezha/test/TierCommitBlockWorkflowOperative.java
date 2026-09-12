package com.flezha.test;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.operative.WorkflowOperative;
import java.nio.charset.StandardCharsets;

/**
 * Mirror of the Rust test app's {@code tier_commit_block_workflow}: the
 * effectively-once workflow fixture. Proc A commits durable state AND emits
 * {@code committed:<input>} under a stable key on a keyed hybrid channel
 * (captured into the atomic effect record); proc B blocks until the resumed
 * runner sets the release env. A SIGKILL lands with A's effect record durable
 * and B unfinished; the resume re-runs A (idempotent change, deterministic
 * re-emit) and completes.
 */
public final class TierCommitBlockWorkflowOperative implements WorkflowOperative {

    @Override
    public String factoryId() {
        return "tier_commit_block_workflow";
    }

    @Override
    public void procedures(ChannelBuilder c) {
        c.effectivelyOnce();
        c.input(String.class, "in")
                .into(TierCommitA.class)
                .named("mid")
                .keyed()
                .sink("tier_sink", s -> s.getBytes(StandardCharsets.UTF_8))
                .into(CommitBlockWorkflowOperative.BlockB.class);
    }

    /** Proc A: commits durable state and re-emits deterministically under a stable key. */
    public static final class TierCommitA extends DurableStep {

        @Override
        public String name() {
            return "tier_commit_a";
        }

        @Override
        public void run(String input, StateHandle<SeenState, Insert> state, ProcedureCtx ctx)
                throws ProcedureException {
            state.apply(new Insert(name()));
            ctx.emitKeyed("job", "committed:" + input);
        }
    }
}
