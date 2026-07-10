package com.flezha.sdk.runtime.control.handlers;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.proto.CreditGrant;
import com.flezha.sdk.proto.CreditWindowAck;
import com.flezha.sdk.proto.CreditWindowInit;
import com.flezha.sdk.runtime.state.CreditWindow;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Handler-level round-trip for the credit control commands: the init arms
 * the process-global window and returns a decodable {@link CreditWindowAck}
 * echoing the session; the grant replenishes the armed window.
 */
class CreditHandlersTest {

    @BeforeEach
    @AfterEach
    void reset() {
        CreditWindow.disarm();
    }

    @Test
    @DisplayName("CreditWindowInit arms the window and acks with the offered session id")
    void initArmsAndAcks() throws Exception {
        CommandRequest request = CommandRequest.newBuilder()
                .setReferenceId("ref-1")
                .setCreditWindowInit(CreditWindowInit.newBuilder()
                        .setStreamId("session-a")
                        .setWindowBytes(4096)
                        .setLowWatermarkBytes(2048)
                        .build())
                .build();

        List<String> errors = new ArrayList<>();
        byte[] ackBytes = new CreditWindowInitHandler().handle(null, request, errors);

        assertThat(errors).isEmpty();
        assertThat(CreditWindow.currentSession()).isEqualTo("session-a");
        assertThat(CreditWindow.availableBytes()).isEqualTo(4096);

        CreditWindowAck ack = CreditWindowAck.parseFrom(ackBytes);
        assertThat(ack.getStreamId()).isEqualTo("session-a");
    }

    @Test
    @DisplayName("CreditGrant replenishes the armed window; stale sessions are discarded")
    void grantReplenishes() {
        CreditWindow.arm("session-a", 100);
        CreditWindow.acquire(80);

        CommandRequest grant = CommandRequest.newBuilder()
                .setReferenceId("ref-2")
                .setCreditGrant(CreditGrant.newBuilder()
                        .setStreamId("session-a")
                        .setBytes(50)
                        .build())
                .build();

        List<String> errors = new ArrayList<>();
        byte[] out = new CreditGrantHandler().handle(null, grant, errors);
        assertThat(errors).isEmpty();
        assertThat(out).isEmpty();
        assertThat(CreditWindow.availableBytes()).isEqualTo(70);

        CommandRequest stale = CommandRequest.newBuilder()
                .setReferenceId("ref-3")
                .setCreditGrant(CreditGrant.newBuilder()
                        .setStreamId("session-dead")
                        .setBytes(1000)
                        .build())
                .build();

        new CreditGrantHandler().handle(null, stale, new ArrayList<>());
        assertThat(CreditWindow.availableBytes()).isEqualTo(70);
    }
}
