package com.flezha.sdk.runtime.control.handlers;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.proto.RelayMessage;
import com.flezha.sdk.proto.RelayMessageResponse;
import com.flezha.sdk.runtime.ExecutorContext;
import com.flezha.sdk.runtime.config.FlezhaEnv;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Mirror of the Rust SDK's {@code relay-message-handlers} parking
 * regression test (per {@code relay-mvp-rust/tasks.md §5.4}). Asserts the
 * canonical "unsupported" response is returned for every {@code RelayMessage}
 * until {@code relay-message-handlers} ships.
 */
class RelayMessageHandlerTest {

    @Test
    @DisplayName("RelayMessage always returns canonical unsupported error")
    void canonicalUnsupportedError() throws Exception {
        FlezhaEnv.ResolvedConfig cfg = FlezhaEnv.resolve(Map.of());
        ExecutorContext ctx = new ExecutorContext("ns", cfg, 0L);

        CommandRequest request = CommandRequest.newBuilder()
                .setReferenceId("ref")
                .setRelayMessage(RelayMessage.newBuilder()
                        .setRelayId("any")
                        .setPayload(ByteString.copyFromUtf8("hello"))
                        .build())
                .build();

        List<String> errors = new ArrayList<>();
        byte[] reply = new RelayMessageHandler().handle(ctx, request, errors);

        assertThat(errors).isEmpty();
        RelayMessageResponse decoded = RelayMessageResponse.parseFrom(reply);
        assertThat(decoded.getPayload().toByteArray()).isEmpty();
        assertThat(decoded.getError()).isEqualTo("relay does not support handle_message");
    }
}
