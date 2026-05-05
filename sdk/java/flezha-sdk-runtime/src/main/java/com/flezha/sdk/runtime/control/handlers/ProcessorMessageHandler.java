package com.flezha.sdk.runtime.control.handlers;

import com.google.protobuf.ByteString;
import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.proto.ProcessorMessageResponse;
import com.flezha.sdk.runtime.ComputeEngineContext;
import com.flezha.sdk.runtime.control.CommandHandler;
import java.util.List;

/**
 * v1 parking: the new {@code Shard} interface has no {@code handleMessage}
 * method. Until {@code relay-message-handlers} ships per-Shard
 * {@code handleMessage}, this handler responds unconditionally with the
 * canonical "unsupported" error so the agent can throttle to a single WARN
 * per-relay rather than spamming on every routed message.
 *
 * <p>Mirrors the Rust SDK's identical parking shape.
 */
public final class ProcessorMessageHandler implements CommandHandler {

    private static final String UNSUPPORTED_ERROR = "processor does not support handle_message";

    @Override
    public byte[] handle(ComputeEngineContext ctx, CommandRequest request, List<String> errors) {
        ProcessorMessageResponse response = ProcessorMessageResponse.newBuilder()
                .setPayload(ByteString.EMPTY)
                .setError(UNSUPPORTED_ERROR)
                .build();
        return response.toByteArray();
    }
}
