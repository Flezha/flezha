package com.flezha.sdk.runtime.control.handlers;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.proto.ProcessorMessage;
import com.flezha.sdk.proto.ProcessorMessageResponse;
import com.flezha.sdk.runtime.ComputeEngineContext;
import com.flezha.sdk.runtime.config.FlezhaEnv;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Mirror of the Rust SDK's {@code processor-message-handlers} parking
 * regression test (per {@code relay-mvp-rust/tasks.md §5.4}). Asserts the
 * canonical "unsupported" response is returned for every {@code ProcessorMessage}
 * until {@code relay-message-handlers} ships.
 */
class ProcessorMessageHandlerTest {

    @Test
    @DisplayName("ProcessorMessage always returns canonical unsupported error")
    void canonicalUnsupportedError() throws Exception {
        FlezhaEnv.ResolvedConfig cfg = FlezhaEnv.resolve(Map.of());
        ComputeEngineContext ctx = new ComputeEngineContext("ns", cfg, 0L);

        CommandRequest request = CommandRequest.newBuilder()
                .setReferenceId("ref")
                .setProcessorMessage(ProcessorMessage.newBuilder()
                        .setProcessorId("any")
                        .setPayload(ByteString.copyFromUtf8("hello"))
                        .build())
                .build();

        List<String> errors = new ArrayList<>();
        byte[] reply = new ProcessorMessageHandler().handle(ctx, request, errors);

        assertThat(errors).isEmpty();
        ProcessorMessageResponse decoded = ProcessorMessageResponse.parseFrom(reply);
        assertThat(decoded.getPayload().toByteArray()).isEmpty();
        assertThat(decoded.getError()).isEqualTo("processor does not support handle_message");
    }
}
