package com.flezha.examples;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.sdk.runtime.operative.UnboundedOperativeTopology;
import com.flezha.sdk.runtime.operative.builder.UnboundedOperativeTopologyBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** The merged-input example must actually register — an example that does not build teaches nothing. */
class E09_MergedInputTest {

    @Test
    @DisplayName("the two-entry merge registers, both sources wired to the one detector")
    void mergedTopologyRegisters() {
        UnboundedOperativeTopology topology = UnboundedOperativeTopologyBuilder.build(new E09_MergedInput.Thermostat());

        assertThat(topology.operations()).hasSize(1);
        int detector = topology.operationNameToId().get("detector");
        assertThat(topology.channel(topology.lookupChannelId("readings")).consumerIds())
                .containsExactly(detector);
        assertThat(topology.channel(topology.lookupChannelId("config")).consumerIds())
                .containsExactly(detector);
    }
}
