package com.flezha.sdk.runtime.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.sdk.runtime.operative.StreamOperativeClassRegistry;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Routing a payload to its Entity: the fused transform chain, the keys a
 * KeyRequest answers, static and dynamic entity modes, merged entry channels
 * and the Edge outputs an inter-procedure emit becomes.
 */
class EntityRoutingTest extends DispatchTestBase {

    @Test
    @DisplayName("a transform chain is one fused closure; KeyRequest answers the key of the MAPPED value")
    void transformChainAndKeyRequest() {
        StreamOperativeClassRegistry.registerClass(DynamicChainOperative.class);

        ActivationOutcome prepared = outcome(command(
                "dyn_chain",
                0,
                Optional.empty(),
                new ActivationKind.KeyRequest(
                        0,
                        List.of("AAPL:3".getBytes(StandardCharsets.UTF_8), "#c".getBytes(StandardCharsets.UTF_8)),
                        true,
                        false)));
        List<PrepareResult> answers = ((ActivationOutcome.Prepared) prepared).answers();
        assertThat(answers).hasSize(2);
        assertThat(((PrepareResult.Ok) answers.get(0)).answer().keys()).containsExactly("AAPL");
        assertThat(((PrepareResult.Ok) answers.get(1)).answer().keys())
                .as("the filtered value yields no key")
                .isEmpty();

        ItemEffect effect = soleEffect(work("dyn_chain", 0, Optional.of("AAPL"), 0, input(0, "AAPL:3", 1)));
        assertThat(effect.verdict()).isInstanceOf(Verdict.Ok.class);
        assertThat(RECORDED).containsExactly("AAPL=AAPLx3");
        assertThat(TRAVERSALS)
                .as("prepare + work each traverse the ONE fused closure once per payload")
                .hasValue(3);

        soleEffect(work("dyn_chain", 0, Optional.of("MSFT"), 0, input(0, "MSFT:7", 2)));
        assertThat(RECORDED).containsExactly("AAPL=AAPLx3", "MSFT=MSFTx7");
    }

    @Test
    @DisplayName("a filtered-out payload consumes its offset with no invocation")
    void filteredPayloadStillAcks() {
        StreamOperativeClassRegistry.registerClass(DynamicChainOperative.class);
        ItemEffect effect = soleEffect(
                work("dyn_chain", 0, Optional.of("AAPL"), 0, input(0, "#comment", 1, OptionalLong.of(4), false)));
        assertThat(effect.verdict()).isInstanceOf(Verdict.Ok.class);
        assertThat(effect.consumedOffset()).hasValue(4);
        assertThat(RECORDED).isEmpty();
    }

    @Test
    @DisplayName("flatMap fans one payload across keys; each Entity receives only its own slot")
    void fanOutKeysEveryYieldedValue() {
        StreamOperativeClassRegistry.registerClass(FanOutOperative.class);

        ActivationOutcome prepared = outcome(command(
                "fan_out",
                0,
                Optional.empty(),
                new ActivationKind.KeyRequest(0, List.of("AAPL:3".getBytes(StandardCharsets.UTF_8)), true, false)));
        assertThat(((PrepareResult.Ok) ((ActivationOutcome.Prepared) prepared)
                                .answers()
                                .get(0))
                        .answer()
                        .keys())
                .containsExactly("AAPL@1m", "AAPL@5m");

        soleEffect(work("fan_out", 0, Optional.of("AAPL@5m"), 0, input(0, "AAPL:3", 1)));
        assertThat(RECORDED).containsExactly("AAPL@5m=AAPL@5m");
    }

    @Test
    @DisplayName("static mode: entitiesFor routes, and each Entity runs its OWN Procedure instance")
    void staticEntitiesUsePerEntityInstances() {
        StreamOperativeClassRegistry.registerClass(StaticOperative.class);

        soleEffect(work("static_op", 0, Optional.of("a"), 0, input(0, "a:1", 1)));
        soleEffect(work("static_op", 0, Optional.of("a"), 0, input(0, "a:2", 2)));
        soleEffect(work("static_op", 0, Optional.of("b"), 0, input(0, "b:1", 3)));

        assertThat(RECORDED).containsExactly("a#1=a:1", "a#2=a:2", "b#1=b:1");

        ActivationOutcome prepared = outcome(command(
                "static_op",
                0,
                Optional.empty(),
                new ActivationKind.KeyRequest(0, List.of("b:9".getBytes(StandardCharsets.UTF_8)), true, false)));
        assertThat(((PrepareResult.Ok) ((ActivationOutcome.Prepared) prepared)
                                .answers()
                                .get(0))
                        .answer()
                        .keys())
                .containsExactly("b");
    }

    @Test
    @DisplayName("merged entry channels each apply their own mapping at delivery")
    void mergedInputAppliesEdgeMapping() {
        StreamOperativeClassRegistry.registerClass(MergeOperative.class);
        soleEffect(work("merge_op", 0, Optional.empty(), 0, input(0, "x", 1)));
        soleEffect(work("merge_op", 0, Optional.empty(), 0, input(1, "y", 2)));
        assertThat(RECORDED).containsExactly("=A:x", "=B:y");
    }

    @Test
    @DisplayName("an inter-proc emit becomes per-consumer Edge outputs carrying the post-transform value")
    void interProcEmitBecomesEdgeOutputs() {
        StreamOperativeClassRegistry.registerClass(PipelineOperative.class);
        ItemEffect effect = soleEffect(work("pipeline_op", 0, Optional.empty(), 0, input(0, "v", 1)));

        assertThat(effect.outputs()).hasSize(1);
        Output edge = effect.outputs().get(0);
        assertThat(edge.target()).isEqualTo(new OutputTarget.Edge(1, 1));
        assertThat(edge.key()).isEmpty();
        assertThat(edge.eventTime()).isEmpty();

        // The Edge payload round-trips through the consumer's Work dispatch.
        ItemEffect consumed = soleEffect(work(
                "pipeline_op",
                1,
                Optional.empty(),
                0,
                new WorkItem.Input(1, edge.payload(), List.of(), OptionalLong.empty(), false, 2)));
        assertThat(consumed.verdict()).isInstanceOf(Verdict.Ok.class);
        assertThat(RECORDED).containsExactly("=mid:v");
    }

    @Test
    @DisplayName("KI-55: a STREAM inter-proc edge map applies TWICE — pins the defect, not the contract")
    void interProcEdgeMapAppliesTwice() {
        StreamOperativeClassRegistry.registerClass(MappedPipelineOperative.class);
        ItemEffect produced = soleEffect(work("mapped_pipeline_op", 0, Optional.empty(), 0, input(0, "v", 1)));
        Output edge = produced.outputs().get(0);
        soleEffect(work(
                "mapped_pipeline_op",
                1,
                Optional.empty(),
                0,
                new WorkItem.Input(1, edge.payload(), List.of(), OptionalLong.empty(), false, 2)));

        // collectEdgeOutputs applies the chain producer-side and valuesForEntity
        // applies it again at delivery, so a map composes with itself. The
        // contract (sdk-poll-core, "Edge transforms and keys evaluate
        // producer-side") says `=M:mid:v`. This asserts what the code DOES so
        // the suite is green and a fix cannot land silently: invert it to
        // `=M:mid:v` when KI-55 is closed. Every transform elsewhere in the
        // tree sits on an ENTRY channel, where there is no producer-side
        // application, which is why nothing else sees this.
        assertThat(RECORDED).containsExactly("=M:M:mid:v");
    }

    @Test
    @DisplayName("KeyRequest answers the extractor's event time; a failing parse is a per-payload Err")
    void prepareEventTimeAndPerPayloadFailure() {
        StreamOperativeClassRegistry.registerClass(TimerOperative.class);
        ActivationOutcome prepared = outcome(command(
                "timer_op",
                0,
                Optional.empty(),
                new ActivationKind.KeyRequest(
                        0,
                        List.of("1234".getBytes(StandardCharsets.UTF_8), "nope".getBytes(StandardCharsets.UTF_8)),
                        false,
                        true)));
        List<PrepareResult> answers = ((ActivationOutcome.Prepared) prepared).answers();
        assertThat(((PrepareResult.Ok) answers.get(0)).answer().eventTime()).hasValue(1234);
        assertThat(answers.get(1)).isInstanceOf(PrepareResult.Err.class);
    }
}
