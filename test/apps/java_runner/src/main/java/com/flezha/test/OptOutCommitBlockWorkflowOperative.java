package com.flezha.test;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.WorkflowOperative;
import java.nio.charset.StandardCharsets;

/**
 * Mirror of the Rust test app's {@code optout_commit_block}: the opt-out twin of
 * {@link CommitBlockWorkflowOperative} with a live mid-chain signal — proc A's
 * output also reaches a plugin sink, so the crash-resume e2e can observe the
 * mid-run point without durable state (there is none). A crash + resume re-runs
 * the whole chain from the top, so the sink sees proc A's marker once per run
 * and no journal/WAL file exists.
 */
public final class OptOutCommitBlockWorkflowOperative implements WorkflowOperative {

    @Override
    public String factoryId() {
        return "optout_commit_block";
    }

    @Override
    public boolean persistenceEnabled() {
        return false;
    }

    @Override
    public void procedures(ChannelBuilder c) {
        c.input(String.class, "in")
                .into(CommitBlockWorkflowOperative.CommitA.class)
                .sink("mid", s -> ("a-ran:" + s).getBytes(StandardCharsets.UTF_8))
                .into(CommitBlockWorkflowOperative.BlockB.class);
    }
}
