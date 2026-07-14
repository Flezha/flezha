package com.flezha.examples;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.examples.E02_StatefulCounter.CountOperation;
import com.flezha.examples.E02_StatefulCounter.Increment;
import com.flezha.sdk.testing.OperationTestHarness;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * <b>07 — testing an Operation, and proving it is replay-safe.</b>
 *
 * <p>{@code OperationTestHarness} drives an Operation directly — no controller, no
 * transport, no threads. You feed it inputs and assert on the Changes it recorded
 * and the values it emitted.
 *
 * <p>The part that matters most is the pair of <b>determinism gates</b>. Durable
 * state is rebuilt by replaying the change log, and a correction re-runs {@code run}
 * outright — so an Operation that is not a pure function of its inputs will rebuild
 * <i>differently</i> than it ran. That bug does not show up in normal testing. It
 * shows up as corrupt state after a crash, months later.
 *
 * <ul>
 *   <li>{@code replayEquals()} — replaying the change log rebuilds the same STATE.
 *   <li>{@code replayEmitsEqual(inputs)} — re-running produces the same OUTPUT.
 * </ul>
 *
 * <p>Both are needed. An Operation can rebuild identical state while emitting a
 * fresh {@code UUID} or timestamp each run — and a keyed sink's idempotence depends
 * on exactly that not happening.
 *
 * <p>Run these against your own Operations in CI. They are cheap, and they are the
 * only thing standing between "works in tests" and "survives a crash".
 */
class E07_TestingYourOperationTest {

    @Test
    @DisplayName("the Operation records the Changes and emits what we expect")
    void recordsChangesAndEmits() {
        OperationTestHarness<String, E02_StatefulCounter.Counter, String, Increment> harness = OperationTestHarness.of(
                        new CountOperation())
                // Pin the clock so ctx.now() is a constant — otherwise the
                // assertions below would depend on when the test ran.
                .withWallClock(1_700_000_000_000L);

        harness.applyInputs(List.of("a", "b"));

        // Two inputs, two journaled Changes.
        assertThat(harness.recordedChanges())
                .containsExactly(new Increment(1_700_000_000_000L), new Increment(1_700_000_000_000L));

        // ...and the running count came out of the folded state.
        assertThat(harness.recordedEmits()).containsExactly("count=1", "count=2");
    }

    @Test
    @DisplayName("state survives a replay: the change log rebuilds it byte-identically")
    void stateIsReplaySafe() {
        OperationTestHarness<String, E02_StatefulCounter.Counter, String, Increment> harness =
                OperationTestHarness.of(new CountOperation()).withWallClock(1_700_000_000_000L);

        harness.applyInputs(List.of("a", "b", "c"));

        // Empty = the state rebuilt from the change log is byte-identical to the
        // state that ran. A non-empty result carries the diverging bytes.
        assertThat(harness.replayEquals()).isEmpty();
    }

    @Test
    @DisplayName("output survives a replay: re-running emits the same values")
    void outputIsReplaySafe() {
        List<String> inputs = List.of("a", "b", "c");

        OperationTestHarness<String, E02_StatefulCounter.Counter, String, Increment> harness =
                OperationTestHarness.of(new CountOperation()).withWallClock(1_700_000_000_000L);

        harness.applyInputs(inputs);

        // This is what catches an Operation reading System.currentTimeMillis() or
        // UUID.randomUUID() instead of ctx.now() / ctx.stableId().
        assertThat(harness.replayEmitsEqual(inputs)).isEmpty();
    }

    @Test
    @DisplayName("a scripted clock advances one value per invocation")
    void scriptedClock() {
        OperationTestHarness<String, E02_StatefulCounter.Counter, String, Increment> harness =
                OperationTestHarness.of(new CountOperation()).withWallClockScript(1_000L, 2_000L);

        harness.applyInputs(List.of("a", "b"));

        // Useful when the Operation's behaviour depends on time advancing —
        // a window closing, a TTL expiring.
        assertThat(harness.recordedChanges()).containsExactly(new Increment(1_000L), new Increment(2_000L));
    }
}
