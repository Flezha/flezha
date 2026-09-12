package com.flezha.test;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.operative.WorkflowOperative;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Mirror of the Rust test app's {@code retry_workflow}: the per-entity retry
 * ladder. Proc RA commits durable state, forwards on a hybrid channel, and
 * compensates by emitting an undo record; {@code retry_flaky} fails its first
 * {@code FLEZHA_WORKFLOW_RETRY_FAILS} attempts (0 by default — the env is what
 * each scenario arms) and then emits {@code attempt:<n>:<input>}.
 */
public final class RetryWorkflowOperative implements WorkflowOperative {

    @Override
    public String factoryId() {
        return "retry_workflow";
    }

    @Override
    public void procedures(ChannelBuilder c) {
        c.input(String.class, "in")
                .into(RetryA.class)
                .named("mid")
                .sink("retry_sink", s -> ("res:" + s).getBytes(StandardCharsets.UTF_8))
                .into(RetryFlaky.class)
                .named("out")
                .sink("retry_sink");
    }

    /** Proc RA: durable, forwards on the hybrid channel, compensates with an undo record. */
    public static final class RetryA extends DurableStep {

        @Override
        public String name() {
            return "retry_a";
        }

        @Override
        public boolean compensateOverridden() {
            return true;
        }

        @Override
        public void compensate(List<String> completedOutput, StateHandle<SeenState, Insert> state, ProcedureCtx ctx)
                throws ProcedureException {
            ctx.emit("undone");
        }
    }

    /** Fails its first armed attempts, then reports which attempt succeeded. */
    public static final class RetryFlaky extends StatelessStep {

        /**
         * Attempts made in this process — a retry re-runs the procedure in place,
         * so an in-memory counter is the whole ladder's view.
         */
        private static final AtomicInteger ATTEMPTS = new AtomicInteger();

        @Override
        public String name() {
            return "retry_flaky";
        }

        @Override
        public void run(String input, StateHandle<State, NoChange> state, ProcedureCtx ctx)
                throws ProcedureException {
            int attempt = ATTEMPTS.incrementAndGet();
            if (attempt <= TestEnv.longVar("FLEZHA_WORKFLOW_RETRY_FAILS", 0)) {
                throw new ProcedureException("flaky attempt " + attempt);
            }
            ctx.emit("attempt:" + attempt + ":" + input);
        }
    }
}
