package com.flezha.sdk.runtime.control.handlers;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.proto.OperativeMessage;
import com.flezha.sdk.proto.OperativeMessageResponse;
import com.flezha.sdk.runtime.RunnerContext;
import com.flezha.sdk.runtime.config.FlezhaEnv;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Mirror of the Rust SDK's {@code operative-message-handlers} parking
 * regression test (per {@code operative-mvp-rust/boundeds.md §5.4}). Asserts the
 * canonical "unsupported" response is returned for every {@code OperativeMessage}
 * until {@code operative-message-handlers} ships.
 */
class OperativeMessageHandlerTest {

    @Test
    @DisplayName("OperativeMessage always returns canonical unsupported error")
    void canonicalUnsupportedError() throws Exception {
        FlezhaEnv.ResolvedConfig cfg = FlezhaEnv.resolve(Map.of());
        RunnerContext ctx = new RunnerContext("ns", cfg, 0L);

        CommandRequest request = CommandRequest.newBuilder()
                .setReferenceId("ref")
                .setOperativeMessage(OperativeMessage.newBuilder()
                        .setOperativeId("any")
                        .setPayload(ByteString.copyFromUtf8("hello"))
                        .build())
                .build();

        List<String> errors = new ArrayList<>();
        byte[] reply = new OperativeMessageHandler().handle(ctx, request, errors);

        assertThat(errors).isEmpty();
        OperativeMessageResponse decoded = OperativeMessageResponse.parseFrom(reply);
        assertThat(decoded.getPayload().toByteArray()).isEmpty();
        assertThat(decoded.getError()).isEqualTo("operative does not support handle_message");
    }
}
