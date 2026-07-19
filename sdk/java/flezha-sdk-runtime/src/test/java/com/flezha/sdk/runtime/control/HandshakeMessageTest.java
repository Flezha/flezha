package com.flezha.sdk.runtime.control;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The control-init handshake presents a non-empty {@code stable_id} on EVERY
 * connect — the same field name and shape as the Rust SDK's
 * {@code ControlInitMessage} (the controller resolves a reconnect from the
 * stable id + the existing bindings, so the retired {@code previous_runner_id}
 * field is gone).
 */
class HandshakeMessageTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void handshakePresentsANonEmptyStableId() throws Exception {
        String json = mapper.writeValueAsString(HandshakeMessage.from("ns", List.of("factory_a")));
        assertThat(json).contains("\"stable_id\":\"");
        assertThat(json).doesNotContain("\"stable_id\":\"\"");
        assertThat(json).doesNotContain("previous_runner_id");
    }

    @Test
    void handshakeCarriesTheSuppliedFactorySetVerbatim() throws Exception {
        String json = mapper.writeValueAsString(HandshakeMessage.from("ns", List.of("a", "b")));
        assertThat(json).contains("\"factory_ids\":[\"a\",\"b\"]");
    }

    @Test
    void anEmptyFactorySetIsLegal() throws Exception {
        String json = mapper.writeValueAsString(HandshakeMessage.from("ns", List.of()));
        assertThat(json).contains("\"factory_ids\":[]");
    }
}
