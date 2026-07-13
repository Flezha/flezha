package com.flezha.test;

import com.flezha.sdk.bounded.BoundedOperative;
import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operation.OperationCtx;
import com.flezha.sdk.operation.OperationException;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.StateHandle;
import java.util.List;

/**
 * Durable saga bounded operative for the crash-mid-compensation e2e (§7c.3), the
 * Java mirror of the Rust test app's {@code saga_block_bounded}. Ops A and B
 * commit durable Segment state and declare compensation hooks; op C fails,
 * triggering reverse-order rollback (compensate B, then A). Op A's
 * {@code compensate} BLOCKS on {@code FLEZHA_BOUNDED_E2E_RELEASE} so a SIGKILL
 * lands mid-compensation — after B's compensation-done marker is durable but
 * before A's. The respawned executor (env set) ships the markers back and
 * finishes only the outstanding hook (A), skipping B.
 */
public final class SagaBlockBoundedOperative implements BoundedOperative {

    @Override
    public String factoryId() {
        return "saga_block_bounded";
    }

    @Override
    public void operations(ChannelBuilder c) {
        c.input(String.class, "in").into(SagaA.class);
        c.channel(String.class, "s1").from(SagaA.class).codec().into(SagaB.class);
        c.channel(String.class, "s2").from(SagaB.class).codec().into(SagaC.class);
    }

    /**
     * Op SA: durable + compensable; its {@code compensate} BLOCKS on the release
     * env so the SIGKILL lands mid-compensation (its compensation-intent is
     * durable, its compensation-done is not until the respawn releases it).
     */
    public static final class SagaA extends DurableStep {
        @Override
        public String name() {
            return "saga_a";
        }

        @Override
        public boolean compensateOverridden() {
            return true;
        }

        @Override
        public void compensate(List<String> completedOutput, StateHandle<SeenState, Insert> state, OperationCtx ctx)
                throws OperationException {
            BoundedE2eGate.blockUntilReleased();
        }
    }

    /**
     * Op SB: durable + compensable with a FAST {@code compensate} (its
     * compensation-done marker lands before the crash), so the respawn MUST skip
     * it rather than re-run it.
     */
    public static final class SagaB extends DurableStep {
        @Override
        public String name() {
            return "saga_b";
        }

        @Override
        public boolean compensateOverridden() {
            return true;
        }

        @Override
        public void compensate(List<String> completedOutput, StateHandle<SeenState, Insert> state, OperationCtx ctx) {
            // Fast, no-op undo — the done marker is what the resume observes.
        }
    }

    /**
     * Op SC: terminal step that always fails, triggering reverse-order rollback
     * of the completed SB then SA. Itself uncompleted ⇒ never compensated.
     */
    public static final class SagaC extends StatelessStep {
        @Override
        public String name() {
            return "saga_c";
        }

        @Override
        public void run(String input, StateHandle<State, NoChange> state, OperationCtx ctx)
                throws OperationException {
            throw new OperationException("saga_c always fails");
        }
    }
}
