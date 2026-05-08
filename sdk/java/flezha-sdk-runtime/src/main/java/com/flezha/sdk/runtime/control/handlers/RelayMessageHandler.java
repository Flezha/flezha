package com.flezha.sdk.runtime.control.handlers;

import com.google.protobuf.ByteString;
import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.proto.RelayMessageResponse;
import com.flezha.sdk.runtime.ExecutorContext;
import com.flezha.sdk.runtime.control.CommandHandler;
import java.util.List;

/**
 * v1 parking: the new {@code Shard} interface has no {@code handleMessage}
 * method. Until {@code relay-message-handlers} ships per-Shard
 * {@code handleMessage}, this handler responds unconditionally with the
 * canonical "unsupported" error so the controller can throttle to a single WARN
 * per-relay rather than spamming on every routed message.
 *
 * <p>Mirrors the Rust SDK's identical parking shape.
 */
public final class RelayMessageHandler implements CommandHandler {

    private static final String UNSUPPORTED_ERROR = "relay does not support handle_message";

    @Override
    public byte[] handle(ExecutorContext ctx, CommandRequest request, List<String> errors) {
        RelayMessageResponse response = RelayMessageResponse.newBuilder()
                .setPayload(ByteString.EMPTY)
                .setError(UNSUPPORTED_ERROR)
                .build();
        return response.toByteArray();
    }
}
