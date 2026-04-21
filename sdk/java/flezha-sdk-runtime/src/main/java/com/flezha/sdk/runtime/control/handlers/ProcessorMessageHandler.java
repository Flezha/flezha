package com.flezha.sdk.runtime.control.handlers;

import com.google.protobuf.ByteString;
import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.proto.ProcessorMessage;
import com.flezha.sdk.proto.ProcessorMessageResponse;
import com.flezha.sdk.runtime.ComputeEngineContext;
import com.flezha.sdk.runtime.control.CommandHandler;
import com.flezha.sdk.runtime.processor.ProcessorInstance;
import java.util.List;

/**
 * Handler for {@link ProcessorMessage} control commands. Locates the
 * processor by id and invokes {@code handleMessage}; wraps the return value
 * in a {@link ProcessorMessageResponse}. Factories that do not override
 * {@code handleMessage} throw from the default implementation, and the
 * exception is converted to a non-empty {@code error} field without failing
 * the command.
 */
public final class ProcessorMessageHandler implements CommandHandler {

    @Override
    public byte[] handle(ComputeEngineContext ctx, CommandRequest request, List<String> errors) {
        ProcessorMessage cmd = request.getProcessorMessage();
        String processorId = cmd.getProcessorId();

        ProcessorInstance<?, ?, ?> instance = ctx.processorRegistry().get(processorId);
        if (instance == null) {
            errors.add("Processor with id " + processorId + " not found");
            return new byte[0];
        }

        byte[] payloadBytes = cmd.getPayload().toByteArray();
        ProcessorMessageResponse response;
        try {
            byte[] reply = instance.handleMessage(payloadBytes);
            response = ProcessorMessageResponse.newBuilder()
                    .setPayload(ByteString.copyFrom(reply != null ? reply : new byte[0]))
                    .setError("")
                    .build();
        } catch (Exception e) {
            String message = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
            response = ProcessorMessageResponse.newBuilder()
                    .setPayload(ByteString.EMPTY)
                    .setError(message)
                    .build();
        }

        return response.toByteArray();
    }
}
