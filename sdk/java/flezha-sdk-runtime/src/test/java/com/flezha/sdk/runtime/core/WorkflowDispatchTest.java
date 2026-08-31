package com.flezha.sdk.runtime.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.sdk.runtime.state.KryoCodec;
import com.flezha.sdk.runtime.workflow.WorkflowOperativeClassRegistry;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Workflow activations: the run-sugar body and its failure classification, the
 * one edge output a structured stage releases, the transform each consumer
 * applies at delivery, and compensation.
 */
class WorkflowDispatchTest extends DispatchTestBase {

    @Test
    @DisplayName("a run-sugar body emits and recvs through the session and reports WorkflowDone")
    void workflowRunSugar() {
        WorkflowOperativeClassRegistry.registerClass(GreetWorkflow.class);
        session.inbox.add("ping".getBytes(StandardCharsets.UTF_8));

        ActivationOutcome out = outcome(command(
                "greet_workflow",
                0,
                Optional.empty(),
                new ActivationKind.WorkflowRun("bob".getBytes(StandardCharsets.UTF_8))));
        assertThat(out).isInstanceOf(ActivationOutcome.WorkflowDone.class);
        ActivationOutcome.WorkflowDone done = (ActivationOutcome.WorkflowDone) out;
        assertThat(done.completed()).isTrue();
        assertThat(new String(done.result(), StandardCharsets.UTF_8)).isEqualTo("done:bob");
        assertThat(session.emitted.stream()
                        .map(b -> new String(b, StandardCharsets.UTF_8))
                        .toList())
                .containsExactly("hi bob", "ping");
    }

    @Test
    @DisplayName("a throwing run-sugar body fails the activation with its classification")
    void workflowRunSugarFailure() {
        WorkflowOperativeClassRegistry.registerClass(ThrowingWorkflow.class);
        ActivationOutcome out =
                outcome(command("throwing_workflow", 0, Optional.empty(), new ActivationKind.WorkflowRun(new byte[0])));
        assertThat(out).isInstanceOf(ActivationOutcome.Failed.class);
        WorkError error = ((ActivationOutcome.Failed) out).error();
        assertThat(error.message()).contains("nope");
        assertThat(error.nonRetryable()).isTrue();
    }

    @Test
    @DisplayName("a workflow stage releases ONE untransformed edge output, whatever the consumer count")
    void workflowStageReleasesOneEdgeOutputPerValue() {
        WorkflowOperativeClassRegistry.registerClass(FanOutWorkflow.class);
        ItemEffect effect = soleEffect(work("fanout_workflow", 0, Optional.empty(), 0, input(0, "v", 1)));

        // The workflow fold discards the consumer and the driver fans the
        // released batch out itself, so `wf_fan`'s two consumers must not
        // double it.
        assertThat(effect.outputs()).hasSize(1);
        Output released = effect.outputs().get(0);
        assertThat(released.target()).isInstanceOf(OutputTarget.Edge.class);
        assertThat(((OutputTarget.Edge) released.target()).channel()).isEqualTo(1);
        assertThat(released.key()).isEmpty();
        assertThat(released.eventTime()).isEmpty();
        assertThat(new KryoCodec(List.of(), false).deserialize(released.payload(), Object.class))
                .as("the released value is raw: the consumer edge's map belongs to the consumer")
                .isEqualTo("mid:v");
    }

    @Test
    @DisplayName("each workflow consumer applies its own edge transform exactly once, at delivery")
    void workflowConsumerAppliesItsEdgeOnce() {
        WorkflowOperativeClassRegistry.registerClass(FanOutWorkflow.class);
        ItemEffect stage = soleEffect(work("fanout_workflow", 0, Optional.empty(), 0, input(0, "v", 1)));
        byte[] released = stage.outputs().get(0).payload();

        // The one released value reaches BOTH children — the driver's fan-out —
        // and each evaluates its own edge in valuesForEntity.
        soleEffect(work(
                "fanout_workflow",
                1,
                Optional.empty(),
                0,
                new WorkItem.Input(1, released, List.of(), OptionalLong.empty(), false, 2)));
        soleEffect(work(
                "fanout_workflow",
                2,
                Optional.empty(),
                0,
                new WorkItem.Input(1, released, List.of(), OptionalLong.empty(), false, 3)));

        assertThat(RECORDED)
                .as("the mapped edge applies once (never producer-side AND again here)")
                .containsExactly("=mapped:mid:v", "=mid:v");
    }

    @Test
    @DisplayName("CompensateRun decodes the recorded outputs and collects the undo emits")
    void compensateRunUndoesWithDecodedOutputs() {
        WorkflowOperativeClassRegistry.registerClass(SagaWorkflow.class);
        ItemEffect forward = soleEffect(work("saga_workflow", 0, Optional.empty(), 0, input(0, "job", 1)));
        assertThat(forward.outputs()).hasSize(1);

        ActivationOutcome out = outcome(command(
                "saga_workflow",
                0,
                Optional.empty(),
                new ActivationKind.CompensateRun(
                        List.of(forward.outputs().get(0).payload()))));
        List<ItemEffect> effects = ((ActivationOutcome.Work) out).effects();
        assertThat(effects).hasSize(1);
        assertThat(effects.get(0).verdict()).isInstanceOf(Verdict.Ok.class);
        assertThat(effects.get(0).outputs())
                .as("the undo emit rides the effect")
                .hasSize(1);
        assertThat(RECORDED).containsExactly("compensated:fwd:job");
    }
}
