package com.flezha.examples;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.sdk.runtime.operative.StreamOperativeTopology;
import com.flezha.sdk.runtime.operative.builder.StreamOperativeTopologyBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** The merged-input example must actually register — an example that does not build teaches nothing. */
class E09_MergedInputTest {

    @Test
    @DisplayName("the two-entry merge registers, both sources wired to the one detector")
    void mergedTopologyRegisters() {
        StreamOperativeTopology topology = StreamOperativeTopologyBuilder.build(new E09_MergedInput.Thermostat());

        assertThat(topology.procedures()).hasSize(1);
        int detector = topology.procedureNameToId().get("detector");
        assertThat(topology.channel(topology.lookupChannelId("readings")).consumerIds())
                .containsExactly(detector);
        assertThat(topology.channel(topology.lookupChannelId("config")).consumerIds())
                .containsExactly(detector);
    }
}
