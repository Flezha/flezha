package com.flezha.test;

import com.flezha.sdk.bounded.BoundedOperative;
import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.StateHandle;

/**
 * Durable structured bounded operative for the crash-resume e2e (§7c.3), the
 * Java mirror of the Rust test app's {@code commit_block_bounded}. Proc A commits
 * idempotent Entity state to the WAL, then proc B blocks on
 * {@code FLEZHA_BOUNDED_E2E_RELEASE} — so a SIGKILL of the first runner lands
 * with proc A's state already durable. The respawned runner (env set) restores
 * the committed state (crash-resume) and runs to completion.
 */
public final class CommitBlockBoundedOperative implements BoundedOperative {

    @Override
    public String factoryId() {
        return "commit_block_bounded";
    }

    @Override
    public void procedures(ChannelBuilder c) {
        var sCommitA = c.input(String.class, "in").into(CommitA.class);
        sCommitA.named("mid").codec().into(BlockB.class);
    }

    /** Proc A: commits idempotent durable Entity state, then hands input to proc B. */
    public static final class CommitA extends DurableStep {
        @Override
        public String name() {
            return "commit_a";
        }
    }

    /**
     * Proc B: blocks a pool worker until the resumed runner sets the release
     * env, then the chain reaches quiescence (status {@code ok}). No emit —
     * completion is observed controller-side via the finished-job record.
     */
    public static final class BlockB extends StatelessStep {
        @Override
        public String name() {
            return "block_b";
        }

        @Override
        public void run(String input, StateHandle<State, NoChange> state, ProcedureCtx ctx)
                throws ProcedureException {
            BoundedE2eGate.blockUntilReleased();
        }
    }
}
