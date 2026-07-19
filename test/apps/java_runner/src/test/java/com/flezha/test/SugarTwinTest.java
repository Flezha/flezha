package com.flezha.test;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.generated.GeneratedUnboundedOperatives;
import com.flezha.sdk.runtime.operative.UnboundedOperativeClassRegistry;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.WallClock;
import com.flezha.sdk.testing.OperationTestHarness;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Asserts the {@code @Operation} / {@code @State} / {@code @UnboundedOperative}-generated
 * operatives register and process input identically to the hand-written equivalents, and
 * that the 2-operation reference operative matches the cross-SDK authoring contract
 * (see {@code docs/sdk_authoring_contract.md} §6).
 */
class SugarTwinTest {

    @BeforeEach
    void reset() {
        UnboundedOperativeClassRegistry.reset();
    }

    @Test
    void sugarDoNothingEmitsBytesLikeHandWritten() {
        var sugar = OperationTestHarness.of(new SugarDoNothingOperation());
        sugar.applyInputs(List.of("alpha", "beta"));
        var emits = sugar.recordedEmits();
        assertThat(emits).hasSize(2);
        assertThat((byte[]) emits.get(0)).isEqualTo("alpha".getBytes(StandardCharsets.UTF_8));
        assertThat((byte[]) emits.get(1)).isEqualTo("beta".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void sugarStoreJournalsAndEmits() {
        var h = OperationTestHarness.of(new SugarStoreOperation());
        h.applyInputs(List.of("one", "two"));
        h.expectEmits(String.class, List.of("one", "two"));
        assertThat(h.state().textInputs).containsExactly("one", "two");
        assertThat(h.replayEquals()).isEmpty();
    }

    @Test
    void sugarEntrySegmentsAndJournals() {
        var operation = new SugarEntryOperation();
        assertThat(operation.segmentFor("apple")).containsExactly("a");
        assertThat(operation.segmentFor("zebra")).containsExactly("d");
        assertThat(operation.segmentForOverridden()).isTrue();
        assertThat(operation.operationKeys()).containsExactly("a", "b", "c", "d");

        var h = OperationTestHarness.of(operation);
        h.applyInputs(List.of("apple", "avocado"));
        h.expectEmits(String.class, List.of("apple", "avocado"));
        assertThat(h.state().seen).containsExactly("apple", "avocado");
        assertThat(h.replayEquals()).isEmpty();
    }

    @Test
    void generatedUnboundedOperativesDefine() {
        GeneratedUnboundedOperatives.defineAll(com.flezha.sdk.Flezha.createRunner("sugar_twin_ns"));
        assertThat(com.flezha.sdk.runtime.operative.UnboundedOperativeClassRegistry.factoryIds())
                .contains("java_sugar_do_nothing", "java_sugar_store", "java_sugar_static_segment");
    }

    @Test
    void sugarDynamicGeneratesDynamicSurface() {
        var s = new SugarDynamicOperation();
        assertThat(s.operationKeyOverridden()).isTrue();
        assertThat(s.operationKeys()).isEmpty();
        assertThat(s.operationKey("btc:1")).contains("btc");
        assertThat(s.operationKey("drop")).isEmpty();
    }

    /**
     * Cross-SDK conformance: the reference operative's associated-type mapping
     * matches the contract table (and the Rust expansion of the same operative).
     */
    @Test
    void referenceOperativeMatchesContract() {
        var entry = new SugarEntryOperation();
        assertThat(entry.inputType()).isEqualTo(String.class);
        assertThat(entry.outputType()).isEqualTo(String.class);
        assertThat(entry.changeType()).isEqualTo(SugarEntryStateChange.class);
        assertThat(entry.clockType()).isEqualTo(WallClock.class);
        assertThat(entry.configType()).isEqualTo(Void.class);
        assertThat(entry.operationKeys()).containsExactly("a", "b", "c", "d");

        var down = new SugarDownstreamOperation();
        assertThat(down.inputType()).isEqualTo(String.class);
        assertThat(down.outputType()).isEqualTo(byte[].class);
        assertThat(down.changeType()).isEqualTo(NoChange.class);
        assertThat(down.operationKeys()).isEmpty();
    }

    /**
     * Canonical keyed-collection reference operation (`expressive-state-journaling`):
     * the `@Journal(MAP_DEQUE)` + `Optional<Output>` sugar processes input
     * identically to the hand-written twin and survives `replayEquals`.
     */
    @Test
    void rollingWindowMatchesHandWrittenTwin() {
        List<WindowInput> inputs = List.of(
                new WindowInput("a", 1.0),
                new WindowInput("a", 2.0),
                new WindowInput("a", 3.0),
                new WindowInput("a", 4.0),
                new WindowInput("b", 10.0),
                new WindowInput("b", 20.0),
                new WindowInput("b", 30.0));

        var sugar = OperationTestHarness.of(new RollingWindowOperation());
        sugar.applyInputs(inputs);

        var twin = OperationTestHarness.of(new RollingWindowTwin());
        twin.applyInputs(inputs);

        // Same emits (warmup gate via Optional<Output>).
        assertThat(sugar.recordedEmits()).isEqualTo(twin.recordedEmits());
        assertThat(sugar.recordedEmits()).containsExactly(2.0, 3.0, 20.0);

        // Same per-key window contents (ArrayDeque has identity equals, so
        // compare element-wise).
        assertThat(sugar.state().window.keySet()).isEqualTo(twin.state().window.keySet());
        for (String key : sugar.state().window.keySet()) {
            assertThat(new ArrayList<>(sugar.state().window.get(key)))
                    .isEqualTo(new ArrayList<>(twin.state().window.get(key)));
        }

        // The generated journaling replays deterministically.
        assertThat(sugar.replayEquals()).isEmpty();
    }

    /** Exercises the generated MAP_SET / MAP_VEC mutators + a composed flat field. */
    @Test
    void keyedCollectionsModesJournalAndReplay() {
        List<WindowInput> inputs = List.of(
                new WindowInput("x", 1.0),
                new WindowInput("x", 2.0),
                new WindowInput("x", 3.0),
                new WindowInput("x", 4.0),
                new WindowInput("y", 9.0));

        var h = OperationTestHarness.of(new KeyedCollectionsOperation());
        h.applyInputs(inputs);

        assertThat(h.state().last.get("x")).isEqualTo(4.0);
        assertThat(h.state().last.get("y")).isEqualTo(9.0);
        assertThat(h.state().closes.get("x")).containsExactly(2.0, 3.0, 4.0);
        assertThat(h.state().closes.get("y")).containsExactly(9.0);
        assertThat(h.state().ticks).isEqualTo(5);

        assertThat(h.replayEquals()).isEmpty();
    }

    /**
     * {@code @InitState} maps the static segment key to per-segment state
     * the default {@code new State()} cannot derive (window length from the
     * segment key). Each segment then behaves per its own window, and replay
     * stays deterministic against the captured initial snapshot.
     */
    @Test
    void operationInitMapsSegmentKeyToState() {
        var operation = new WindowedOperation();
        assertThat(operation.initState(Optional.of("win_2"), Optional.empty()).window).isEqualTo(2);
        assertThat(operation.initState(Optional.of("win_3"), Optional.empty()).window).isEqualTo(3);
        // Cold-start / harness default (key absent) falls through to the else arm.
        assertThat(operation.initState(Optional.empty(), Optional.empty()).window).isEqualTo(3);

        List<WindowInput> inputs =
                List.of(new WindowInput("a", 2.0), new WindowInput("a", 4.0), new WindowInput("a", 6.0));

        var w2 = OperationTestHarness.of(operation)
                .withInitialState(operation.initState(Optional.of("win_2"), Optional.empty()));
        w2.applyInputs(inputs);
        w2.expectEmits(Double.class, List.of(3.0, 5.0)); // means of [2,4] then [4,6]
        assertThat(w2.replayEquals()).isEmpty();

        var w3 = OperationTestHarness.of(operation)
                .withInitialState(operation.initState(Optional.of("win_3"), Optional.empty()));
        w3.applyInputs(inputs);
        w3.expectEmits(Double.class, List.of(4.0)); // single mean of [2,4,6]
        assertThat(w3.replayEquals()).isEmpty();
    }
}
