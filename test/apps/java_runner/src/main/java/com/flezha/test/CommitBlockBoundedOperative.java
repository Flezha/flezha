package com.flezha.test;

import com.flezha.sdk.bounded.BoundedOperative;
import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operation.OperationCtx;
import com.flezha.sdk.operation.OperationException;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.StateHandle;

/**
 * Durable structured bounded operative for the crash-resume e2e (§7c.3), the
 * Java mirror of the Rust test app's {@code commit_block_bounded}. Op A commits
 * idempotent Segment state to the WAL, then op B blocks on
 * {@code FLEZHA_BOUNDED_E2E_RELEASE} — so a SIGKILL of the first runner lands
 * with op A's state already durable. The respawned runner (env set) restores
 * the committed state (crash-resume) and runs to completion.
 */
public final class CommitBlockBoundedOperative implements BoundedOperative {

    @Override
    public String factoryId() {
        return "commit_block_bounded";
    }

    @Override
    public void operations(ChannelBuilder c) {
        var sCommitA = c.input(String.class, "in").into(CommitA.class);
        sCommitA.named("mid").codec().into(BlockB.class);
    }

    /** Op A: commits idempotent durable Segment state, then hands input to op B. */
    public static final class CommitA extends DurableStep {
        @Override
        public String name() {
            return "commit_a";
        }
    }

    /**
     * Op B: blocks a pool worker until the resumed runner sets the release
     * env, then the chain reaches quiescence (status {@code ok}). No emit —
     * completion is observed controller-side via the finished-job record.
     */
    public static final class BlockB extends StatelessStep {
        @Override
        public String name() {
            return "block_b";
        }

        @Override
        public void run(String input, StateHandle<State, NoChange> state, OperationCtx ctx)
                throws OperationException {
            BoundedE2eGate.blockUntilReleased();
        }
    }
}
