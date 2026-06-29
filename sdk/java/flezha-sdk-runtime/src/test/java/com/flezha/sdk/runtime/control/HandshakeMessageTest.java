package com.flezha.sdk.runtime.control;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flezha.sdk.operative.ContinuousOperativeRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The control-init handshake announces {@code previous_executor_id} on the
 * initial connect and omits it on reconnect — byte-compatible with the Rust
 * SDK's {@code ControlInitMessage} (controller evicts the stale binding).
 */
class HandshakeMessageTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @BeforeEach
    @AfterEach
    void reset() {
        ContinuousOperativeRegistry.reset();
    }

    @Test
    void initialConnectAnnouncesPreviousExecutorId() throws Exception {
        String json = mapper.writeValueAsString(HandshakeMessage.from("ns", "exec-prev-9"));
        assertThat(json).contains("\"previous_executor_id\":\"exec-prev-9\"");
    }

    @Test
    void reconnectOmitsPreviousExecutorId() throws Exception {
        String json = mapper.writeValueAsString(HandshakeMessage.from("ns"));
        assertThat(json).doesNotContain("previous_executor_id");
    }
}
