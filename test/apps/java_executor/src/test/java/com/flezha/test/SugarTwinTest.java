package com.flezha.test;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.generated.GeneratedRelays;
import com.flezha.sdk.relay.RelayRegistry;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.WallClock;
import com.flezha.sdk.testing.ShardTestHarness;
import java.nio.charset.StandardCharsets;
import java.util.List;
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
}
