package com.flezha.sdk.runtime.control;

import com.flezha.sdk.native_.NativeTransport;
import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.proto.CommandResponse;
import com.flezha.sdk.runtime.ExecutorContext;
import com.flezha.sdk.runtime.control.handlers.AddRelayHandler;
import com.flezha.sdk.runtime.control.handlers.AssignExecutorIdHandler;
import com.flezha.sdk.runtime.control.handlers.CompactPartitionHandler;
import com.flezha.sdk.runtime.control.handlers.ConnectSocketHandler;
import com.flezha.sdk.runtime.control.handlers.ExecutorDisconnectHandler;
import com.flezha.sdk.runtime.control.handlers.MigrationDeserializationHandler;
import com.flezha.sdk.runtime.control.handlers.MigrationSerializationHandler;
import com.flezha.sdk.runtime.control.handlers.PartitionApplyStateHandler;
import com.flezha.sdk.runtime.control.handlers.PartitionMailboxStatusHandler;
import com.flezha.sdk.runtime.control.handlers.PartitionStateHandler;
import com.flezha.sdk.runtime.control.handlers.PingHandler;
import com.flezha.sdk.runtime.control.handlers.RelayMessageHandler;
import com.flezha.sdk.runtime.control.handlers.RemRelayHandler;
import com.flezha.sdk.runtime.control.handlers.RestoreInitStateHandler;
import com.flezha.sdk.runtime.framing.Frame;
import com.flezha.sdk.runtime.framing.FrameEncoder;
import com.google.protobuf.ByteString;
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

    private final ExecutorContext ctx;
    private final long controlStreamId;
    private final Map<CommandRequest.CommandCase, CommandHandler> handlers;

    public CommandRouter(@Nonnull ExecutorContext ctx, long controlStreamId) {
        this.ctx = ctx;
        this.controlStreamId = controlStreamId;
        java.util.Map<CommandRequest.CommandCase, CommandHandler> mut = new java.util.LinkedHashMap<>();
        mut.put(CommandRequest.CommandCase.ASSIGN_EXECUTOR_ID, new AssignExecutorIdHandler());
        mut.put(CommandRequest.CommandCase.CONNECT_SOCKET, new ConnectSocketHandler());
        mut.put(CommandRequest.CommandCase.ADD_RELAY, new AddRelayHandler());
        mut.put(CommandRequest.CommandCase.REM_RELAY, new RemRelayHandler());
        mut.put(CommandRequest.CommandCase.PARTITION_STATE, new PartitionStateHandler());
        mut.put(CommandRequest.CommandCase.MIGRATION_SERIALIZATION, new MigrationSerializationHandler());
        mut.put(CommandRequest.CommandCase.MIGRATION_DESERIALIZATION, new MigrationDeserializationHandler());
        mut.put(CommandRequest.CommandCase.EXECUTOR_DISCONNECT, new ExecutorDisconnectHandler());
        mut.put(CommandRequest.CommandCase.RELAY_MESSAGE, new RelayMessageHandler());
        mut.put(CommandRequest.CommandCase.PARTITION_APPLY_STATE, new PartitionApplyStateHandler());
        // State-persistence handlers added by relay-state-persistence-java.
        // The matching CommandCase enum constants land in the protobuf
        // codegen once the .proto regenerates; the lookups are resolved
        // reflectively to keep this file resilient to a stale codegen jar.
        registerCase(mut, "RESTORE_INIT_STATE", new RestoreInitStateHandler());
        registerCase(mut, "COMPACT_PARTITION", new CompactPartitionHandler());
        registerCase(mut, "PARTITION_MAILBOX_STATUS", new PartitionMailboxStatusHandler());
        // Cold-restart end-to-end health probe (fix-overwatch-restart-storm).
        // Resolved reflectively because the protobuf codegen jar may lag the
        // .proto file in a partial-deploy window.
        registerCase(mut, "PING", new PingHandler());
        this.handlers = java.util.Collections.unmodifiableMap(mut);
    }

    private static void registerCase(
            java.util.Map<CommandRequest.CommandCase, CommandHandler> map, String enumName, CommandHandler handler) {
        try {
            CommandRequest.CommandCase c = CommandRequest.CommandCase.valueOf(enumName);
            map.put(c, handler);
        } catch (IllegalArgumentException ignored) {
            // protobuf codegen is older than this SDK build — handler will
            // not register; the controller never sends the variant against an
            // older codegen anyway.
        }
    }

    public void route(@Nonnull Frame frame) {
        CommandRequest request;
        try {
            request = CommandRequest.parseFrom(frame.message());
        } catch (Exception e) {
            LOG.error("Failed to decode CommandRequest: {}", e.toString());
            return;
        }

        // SDK-initiated CommandRequest variants do not solicit a
        // CommandResponse — they're controller->SDK acks for the SDK's own
        // graceful-shutdown notifications, or buggy notifications the
        // SDK is the sender of. Both short-circuit the response write.
        switch (request.getCommandCase()) {
            case EXECUTOR_SHUTDOWN_ACK:
                GracefulShutdown.signalAck(request.getReferenceId());
                return;
            case EXECUTOR_SHUTDOWN_NOTIFICATION:
                LOG.warn(
                        "Received unexpected ExecutorShutdownNotification (ref={}); ignoring — "
                                + "SDK is the SENDER, not the receiver.",
                        request.getReferenceId());
                return;
            default:
                break;
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

        byte[] frameBytes = FrameEncoder.build(frame.payloadId(), response.toByteArray(), Map.of());

        int writeStatus = NativeTransport.streamWrite(ctx.nativeHandle(), controlStreamId, frameBytes);
        if (writeStatus != 0) {
            LOG.error("Failed to write CommandResponse (status={})", writeStatus);
        }

        if (handler.closeAfterResponse()) {
            NativeTransport.streamClose(ctx.nativeHandle(), controlStreamId);
        }
    }
}
