package com.flezha.sdk.runtime.workflow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.runtime.operative.StatelessTestProcedure;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.workflow.LoopMode;
import com.flezha.sdk.workflow.WorkflowOperative;
import com.flezha.sdk.workflow.WorkflowOperativeConfigurationException;
import com.flezha.sdk.workflow.WorkflowOperativeCtx;
import com.flezha.sdk.workflow.WorkflowOperativeException;
import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Registration-time refusals of {@link WorkflowSpec#build} — the authoring
 * validation half of the retired driver suite. Execution semantics (barriers,
 * loops, retries, sagas) are core-owned now; what stays host-side is the
 * refusal of a shape that could not run.
 */
class WorkflowSpecValidationTest {

    @Test
    @DisplayName("a structured chain resolves its topological order")
    void structuredChainResolves() {
        WorkflowSpec spec = WorkflowSpec.build(LinearWorkflow.class);
        assertThat(spec.isRunSugar()).isFalse();
        assertThat(spec.chain()).containsExactly(0, 1);
        assertThat(spec.loopMode()).isEqualTo(LoopMode.ONCE);
    }

    @Test
    @DisplayName("a loop on run-sugar is a contradiction, not a no-op")
    void loopWithoutChainIsRefused() {
        assertThatThrownBy(() -> WorkflowSpec.build(LoopingRunSugar.class))
                .isInstanceOf(WorkflowOperativeConfigurationException.class)
                .hasMessageContaining("loop");
    }

    @Test
    @DisplayName("more than one attempt with a zero backoff would hot-spin and is refused")
    void zeroRetryBackoffIsRefused() {
        assertThatThrownBy(() -> WorkflowSpec.build(ZeroBackoffWorkflow.class))
                .isInstanceOf(WorkflowOperativeConfigurationException.class)
                .hasMessageContaining("backoff");
    }

    @Test
    @DisplayName("a durable chain requires a codec on every inter-proc channel")
    void durableChainWithoutCodecIsRefused() {
        assertThatThrownBy(() -> WorkflowSpec.build(DurableNoCodecWorkflow.class))
                .isInstanceOf(WorkflowOperativeConfigurationException.class)
                .hasMessageContaining("codec");
    }

    @Test
    @DisplayName("a durable CHAINED loop whose terminal proc has no codec'd output cannot re-seed")
    void chainedLoopTerminalWithoutCodecIsRefused() {
        assertThatThrownBy(() -> WorkflowSpec.build(ChainedNoTerminalCodecWorkflow.class))
                .isInstanceOf(WorkflowOperativeConfigurationException.class)
                .hasMessageContaining("terminal");
    }

    @Test
    @DisplayName("an entry-channel edge closure is refused — the workflow seed names no channel")
    void entryEdgeClosureIsRefused() {
        assertThatThrownBy(() -> WorkflowSpec.build(EntryClosureWorkflow.class))
                .isInstanceOf(WorkflowOperativeConfigurationException.class)
                .hasMessageContaining("MergeFromEntryChannel");
    }

    @Test
    @DisplayName("a cycle is refused — a bounded topology is a DAG")
    void cycleIsRefused() {
        assertThatThrownBy(() -> WorkflowSpec.build(CyclicWorkflow.class))
                .isInstanceOf(WorkflowOperativeConfigurationException.class)
                .hasMessageContaining("cycle");
    }

    @Test
    @DisplayName("an operative whose only exit is an external stop is refused")
    void externalStopOnlyIsRefused() {
        assertThatThrownBy(() -> WorkflowSpec.build(NeverEndingWorkflow.class))
                .isInstanceOf(WorkflowOperativeConfigurationException.class)
                .hasMessageContaining("terminable");
    }

    // ------------------------------------------------------------------
    // fixtures
    // ------------------------------------------------------------------

    public static final class EchoProc extends StatelessTestProcedure<String, Object, String> {
        @Override
        public String name() {
            return "echo";
        }

        @Override
        protected Object initState(Optional<String> entityKey) {
            return new Object();
        }

        @Override
        public void run(String input, StateHandle<Object, NoChange> state, ProcedureCtx ctx)
                throws com.flezha.sdk.procedure.ProcedureException {
            ctx.emit(input);
        }
    }

    public static final class SinkProc extends StatelessTestProcedure<String, Object, Void> {
        @Override
        public String name() {
            return "sink";
        }

        @Override
        protected Object initState(Optional<String> entityKey) {
            return new Object();
        }

        @Override
        public void run(String input, StateHandle<Object, NoChange> state, ProcedureCtx ctx) {}
    }

    public static final class LinearWorkflow implements WorkflowOperative {
        @Override
        public String factoryId() {
            return "vb_linear";
        }

        @Override
        public boolean persistenceEnabled() {
            return false;
        }

        @Override
        public void procedures(ChannelBuilder c) {
            c.input(String.class, "seed").into(EchoProc.class).into(SinkProc.class);
        }
    }

    public static final class LoopingRunSugar implements WorkflowOperative {
        @Override
        public String factoryId() {
            return "vb_loop_sugar";
        }

        @Override
        public LoopMode loopMode() {
            return LoopMode.PER_INPUT;
        }

        @Override
        public void run(WorkflowOperativeCtx ctx, String args) throws WorkflowOperativeException {
            ctx.complete(args);
        }
    }

    public static final class ZeroBackoffWorkflow implements WorkflowOperative {
        @Override
        public String factoryId() {
            return "vb_zero_backoff";
        }

        @Override
        public Duration retryBackoff() {
            return Duration.ZERO;
        }

        @Override
        public void run(WorkflowOperativeCtx ctx, String args) throws WorkflowOperativeException {
            ctx.complete(args);
        }
    }

    public static final class DurableNoCodecWorkflow implements WorkflowOperative {
        @Override
        public String factoryId() {
            return "vb_no_codec";
        }

        @Override
        public void procedures(ChannelBuilder c) {
            c.input(String.class, "seed").into(EchoProc.class).into(SinkProc.class);
        }
    }

    public static final class ChainedNoTerminalCodecWorkflow implements WorkflowOperative {
        @Override
        public String factoryId() {
            return "vb_chained_no_terminal";
        }

        @Override
        public LoopMode loopMode() {
            return LoopMode.CHAINED;
        }

        @Override
        public void procedures(ChannelBuilder c) {
            c.input(String.class, "seed").into(SinkProc.class);
        }
    }

    public static final class EntryClosureWorkflow implements WorkflowOperative {
        @Override
        public String factoryId() {
            return "vb_entry_closure";
        }

        @Override
        public boolean persistenceEnabled() {
            return false;
        }

        @Override
        public void procedures(ChannelBuilder c) {
            c.input(String.class, "seed").map(String.class, String::toUpperCase).into(SinkProc.class);
        }
    }

    public static final class CyclicWorkflow implements WorkflowOperative {
        @Override
        public String factoryId() {
            return "vb_cycle";
        }

        @Override
        public boolean persistenceEnabled() {
            return false;
        }

        @Override
        public void procedures(ChannelBuilder c) {
            var a = c.input(String.class, "seed").into(EchoProc.class);
            var b = a.into(EchoProc.class, "echo_b");
            b.into(EchoProc.class);
        }
    }

    public static final class NeverEndingWorkflow implements WorkflowOperative {
        @Override
        public String factoryId() {
            return "vb_never_ending";
        }

        @Override
        public boolean selfTerminating() {
            return false;
        }

        @Override
        public void run(WorkflowOperativeCtx ctx, String args) throws WorkflowOperativeException {
            ctx.complete(new byte[0]);
        }
    }
}
