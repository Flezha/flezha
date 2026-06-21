package com.flezha.test;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.generated.GeneratedRelays;
import com.flezha.sdk.relay.RelayRegistry;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.WallClock;
import com.flezha.sdk.testing.ShardTestHarness;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Asserts the {@code @Shard} / {@code @State} / {@code @Relay}-generated relays
 * register and process input identically to the hand-written equivalents, and
 * that the 2-shard reference relay matches the cross-SDK authoring contract
 * (see {@code docs/sdk_authoring_contract.md} §6).
 */
class SugarTwinTest {

    @BeforeEach
    void reset() {
        RelayRegistry.reset();
    }

    @Test
    void sugarDoNothingEmitsBytesLikeHandWritten() {
        var sugar = ShardTestHarness.of(new SugarDoNothingShard());
        sugar.applyInputs(List.of("alpha", "beta"));
        var emits = sugar.recordedEmits();
        assertThat(emits).hasSize(2);
        assertThat((byte[]) emits.get(0)).isEqualTo("alpha".getBytes(StandardCharsets.UTF_8));
        assertThat((byte[]) emits.get(1)).isEqualTo("beta".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void sugarStoreJournalsAndEmits() {
        var h = ShardTestHarness.of(new SugarStoreShard());
        h.applyInputs(List.of("one", "two"));
        h.expectEmits(String.class, List.of("one", "two"));
        assertThat(h.state().textInputs).containsExactly("one", "two");
        assertThat(h.replayEquals()).isEmpty();
    }

    @Test
    void sugarEntryPartitionsAndJournals() {
        var shard = new SugarEntryShard();
        assertThat(shard.partitionFor("apple")).containsExactly("a");
        assertThat(shard.partitionFor("zebra")).containsExactly("d");
        assertThat(shard.partitionForOverridden()).isTrue();
        assertThat(shard.shardKeys()).containsExactly("a", "b", "c", "d");

        var h = ShardTestHarness.of(shard);
        h.applyInputs(List.of("apple", "avocado"));
        h.expectEmits(String.class, List.of("apple", "avocado"));
        assertThat(h.state().seen).containsExactly("apple", "avocado");
        assertThat(h.replayEquals()).isEmpty();
    }

    @Test
    void generatedRelaysRegister() {
        GeneratedRelays.registerAll();
        assertThat(RelayRegistry.factoryIds())
                .contains("java_sugar_do_nothing", "java_sugar_store", "java_sugar_static_partition");
    }

    @Test
    void sugarDynamicGeneratesDynamicSurface() {
        var s = new SugarDynamicShard();
        assertThat(s.shardKeyOverridden()).isTrue();
        assertThat(s.shardKeys()).isEmpty();
        assertThat(s.shardKey("btc:1")).contains("btc");
        assertThat(s.shardKey("drop")).isEmpty();
    }

    /**
     * Cross-SDK conformance: the reference relay's associated-type mapping
     * matches the contract table (and the Rust expansion of the same relay).
     */
    @Test
    void referenceRelayMatchesContract() {
        var entry = new SugarEntryShard();
        assertThat(entry.inputType()).isEqualTo(String.class);
        assertThat(entry.outputType()).isEqualTo(String.class);
        assertThat(entry.changeType()).isEqualTo(SugarEntryStateChange.class);
        assertThat(entry.clockType()).isEqualTo(WallClock.class);
        assertThat(entry.configType()).isEqualTo(Void.class);
        assertThat(entry.shardKeys()).containsExactly("a", "b", "c", "d");

        var down = new SugarDownstreamShard();
        assertThat(down.inputType()).isEqualTo(String.class);
        assertThat(down.outputType()).isEqualTo(byte[].class);
        assertThat(down.changeType()).isEqualTo(NoChange.class);
        assertThat(down.shardKeys()).isEmpty();
    }

    /**
     * Canonical keyed-collection reference shard (`expressive-state-journaling`):
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

        var sugar = ShardTestHarness.of(new RollingWindowShard());
        sugar.applyInputs(inputs);

        var twin = ShardTestHarness.of(new RollingWindowTwin());
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

        var h = ShardTestHarness.of(new KeyedCollectionsShard());
        h.applyInputs(inputs);

        assertThat(h.state().last.get("x")).isEqualTo(4.0);
        assertThat(h.state().last.get("y")).isEqualTo(9.0);
        assertThat(h.state().closes.get("x")).containsExactly(2.0, 3.0, 4.0);
        assertThat(h.state().closes.get("y")).containsExactly(9.0);
        assertThat(h.state().ticks).isEqualTo(5);

        assertThat(h.replayEquals()).isEmpty();
    }

    /**
     * {@code @InitState} maps the static partition key to per-partition state
     * the default {@code new State()} cannot derive (window length from the
     * shard key). Each partition then behaves per its own window, and replay
     * stays deterministic against the captured initial snapshot.
     */
    @Test
    void shardInitMapsPartitionKeyToState() {
        var shard = new WindowedShard();
        assertThat(shard.initState(Optional.of("win_2"), Optional.empty()).window).isEqualTo(2);
        assertThat(shard.initState(Optional.of("win_3"), Optional.empty()).window).isEqualTo(3);
        // Cold-start / harness default (key absent) falls through to the else arm.
        assertThat(shard.initState(Optional.empty(), Optional.empty()).window).isEqualTo(3);

        List<WindowInput> inputs =
                List.of(new WindowInput("a", 2.0), new WindowInput("a", 4.0), new WindowInput("a", 6.0));

        var w2 = ShardTestHarness.of(shard).withInitialState(shard.initState(Optional.of("win_2"), Optional.empty()));
        w2.applyInputs(inputs);
        w2.expectEmits(Double.class, List.of(3.0, 5.0)); // means of [2,4] then [4,6]
        assertThat(w2.replayEquals()).isEmpty();

        var w3 = ShardTestHarness.of(shard).withInitialState(shard.initState(Optional.of("win_3"), Optional.empty()));
        w3.applyInputs(inputs);
        w3.expectEmits(Double.class, List.of(4.0)); // single mean of [2,4,6]
        assertThat(w3.replayEquals()).isEmpty();
    }
}
