package com.flezha.sdk.runtime.control;

import com.google.protobuf.ByteString;
import com.flezha.sdk.native_.NativeTransport;
import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.proto.CommandResponse;
import com.flezha.sdk.runtime.ComputeEngineContext;
import com.flezha.sdk.runtime.control.handlers.AddProcessorHandler;
import com.flezha.sdk.runtime.control.handlers.AssignComputeEngineIdHandler;
import com.flezha.sdk.runtime.control.handlers.ComputeEngineDisconnectHandler;
import com.flezha.sdk.runtime.control.handlers.ConnectSocketHandler;
import com.flezha.sdk.runtime.control.handlers.MigrationDeserializationHandler;
import com.flezha.sdk.runtime.control.handlers.MigrationSerializationHandler;
import com.flezha.sdk.runtime.control.handlers.ProcessorApplyStateHandler;
import com.flezha.sdk.runtime.control.handlers.ProcessorMessageHandler;
import com.flezha.sdk.runtime.control.handlers.ProcessorStateHandler;
import com.flezha.sdk.runtime.control.handlers.RemProcessorHandler;
import com.flezha.sdk.runtime.framing.Frame;
import com.flezha.sdk.runtime.framing.FrameEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decodes a {@link CommandRequest} from a control-plane {@link Frame},
 * dispatches to the registered handler, and writes the {@link CommandResponse}
 * back to the control stream.
 */
public final class CommandRouter {

    private static final Logger LOG = LoggerFactory.getLogger(CommandRouter.class);

    private final ComputeEngineContext ctx;
    private final long controlStreamId;
    private final Map<CommandRequest.CommandCase, CommandHandler> handlers;

    public CommandRouter(@Nonnull ComputeEngineContext ctx, long controlStreamId) {
        this.ctx = ctx;
        this.controlStreamId = controlStreamId;
        // Map.of has 10-entry overloads; switch to Map.ofEntries past that.
        this.handlers = Map.ofEntries(
                Map.entry(CommandRequest.CommandCase.ASSIGN_COMPUTE_ENGINE_ID, new AssignComputeEngineIdHandler()),
                Map.entry(CommandRequest.CommandCase.CONNECT_SOCKET, new ConnectSocketHandler()),
                Map.entry(CommandRequest.CommandCase.ADD_PROCESSOR, new AddProcessorHandler()),
                Map.entry(CommandRequest.CommandCase.REM_PROCESSOR, new RemProcessorHandler()),
                Map.entry(CommandRequest.CommandCase.PROCESSOR_STATE, new ProcessorStateHandler()),
                Map.entry(CommandRequest.CommandCase.MIGRATION_SERIALIZATION, new MigrationSerializationHandler()),
                Map.entry(CommandRequest.CommandCase.MIGRATION_DESERIALIZATION, new MigrationDeserializationHandler()),
                Map.entry(CommandRequest.CommandCase.COMPUTE_ENGINE_DISCONNECT, new ComputeEngineDisconnectHandler()),
                Map.entry(CommandRequest.CommandCase.PROCESSOR_MESSAGE, new ProcessorMessageHandler()),
                Map.entry(CommandRequest.CommandCase.PROCESSOR_APPLY_STATE, new ProcessorApplyStateHandler()));
    }

    public void route(@Nonnull Frame frame) {
        CommandRequest request;
        try {
            request = CommandRequest.parseFrom(frame.message());
        } catch (Exception e) {
            LOG.error("Failed to decode CommandRequest: {}", e.toString());
            return;
        }

        CommandHandler handler = handlers.get(request.getCommandCase());
        if (handler == null) {
            LOG.warn("No handler for command case: {}", request.getCommandCase());
            return;
        }

        List<String> errors = new ArrayList<>();
        byte[] rawData;
        try {
            rawData = handler.handle(ctx, request, errors);
        } catch (Exception e) {
            LOG.error("Command handler threw for {}: {}", request.getCommandCase(), e.toString());
            errors.add(e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
            rawData = new byte[0];
        }

        CommandResponse response = CommandResponse.newBuilder()
                .setReferenceId(request.getReferenceId())
                .setStatus(errors.isEmpty() ? "success" : "error")
                .addAllErrors(errors)
                .setRawData(ByteString.copyFrom(rawData))
                .build();

        byte[] frameBytes = FrameEncoder.build(
                frame.payloadId(),
                response.toByteArray(),
                Map.of());

        int writeStatus = NativeTransport.streamWrite(
                ctx.nativeHandle(), controlStreamId, frameBytes);
        if (writeStatus != 0) {
            LOG.error("Failed to write CommandResponse (status={})", writeStatus);
        }

        if (handler.closeAfterResponse()) {
            NativeTransport.streamClose(ctx.nativeHandle(), controlStreamId);
        }
    }
}
