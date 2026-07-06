package com.flezha.sdk.runtime.control;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flezha.sdk.operative.UnboundedOperativeRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The control-init handshake presents a non-empty {@code stable_id} on EVERY
 * connect — the same field name and shape as the Rust SDK's
 * {@code ControlInitMessage} (the controller resolves a reconnect from the
 * stable id + the existing bindings, so the retired {@code previous_executor_id}
 * field is gone).
 */
class HandshakeMessageTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @BeforeEach
    @AfterEach
    void reset() {
        UnboundedOperativeRegistry.reset();
    }

    @Test
    void handshakePresentsANonEmptyStableId() throws Exception {
        String json = mapper.writeValueAsString(HandshakeMessage.from("ns"));
        assertThat(json).contains("\"stable_id\":\"");
        assertThat(json).doesNotContain("\"stable_id\":\"\"");
        assertThat(json).doesNotContain("previous_executor_id");
    }
}
