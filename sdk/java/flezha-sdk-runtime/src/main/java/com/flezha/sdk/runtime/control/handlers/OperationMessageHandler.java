package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.proto.OperationMessageResponse;
import com.flezha.sdk.runtime.RunnerContext;
import com.flezha.sdk.runtime.control.CommandHandler;
import com.google.protobuf.ByteString;
import java.util.List;

/**
 * v1 parking: the new {@code Operation} interface has no {@code handleMessage}
 * method. Until {@code operative-message-handlers} ships per-Operation
 * {@code handleMessage}, this handler responds unconditionally with the
 * canonical "unsupported" error so the controller can throttle to a single WARN
 * per-operative rather than spamming on every routed message.
 *
 * <p>Mirrors the Rust SDK's identical parking shape.
 */
public final class OperationMessageHandler implements CommandHandler {

    private static final String UNSUPPORTED_ERROR = "operative does not support handle_message";

    @Override
    public byte[] handle(RunnerContext ctx, CommandRequest request, List<String> errors) {
        OperationMessageResponse response = OperationMessageResponse.newBuilder()
                .setPayload(ByteString.EMPTY)
                .setError(UNSUPPORTED_ERROR)
                .build();
        return response.toByteArray();
    }
}
