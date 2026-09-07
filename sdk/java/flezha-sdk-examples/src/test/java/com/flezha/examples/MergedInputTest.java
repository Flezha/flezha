package com.flezha.examples;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.examples.merged.ThermostatOperative;
import com.flezha.sdk.runtime.operative.StreamOperativeTopology;
import com.flezha.sdk.runtime.operative.builder.StreamOperativeTopologyBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** An example that does not register teaches nothing, so prove the merge wires up. */
class MergedInputTest {

    @Test
    @DisplayName("both sources reach the one detector")
    void mergedTopologyRegisters() {
        StreamOperativeTopology topology = StreamOperativeTopologyBuilder.build(new ThermostatOperative());

        assertThat(topology.procedures()).hasSize(1);
        int detector = topology.procedureNameToId().get("detector");
        assertThat(topology.channel(topology.lookupChannelId("readings")).consumerIds())
                .containsExactly(detector);
        assertThat(topology.channel(topology.lookupChannelId("config")).consumerIds())
                .containsExactly(detector);
    }
}
