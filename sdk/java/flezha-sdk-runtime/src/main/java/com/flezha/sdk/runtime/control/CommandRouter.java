package com.flezha.sdk.runtime.control;

import com.flezha.sdk.native_.NativeTransport;
import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.proto.CommandResponse;
import com.flezha.sdk.runtime.RunnerContext;
import com.flezha.sdk.runtime.control.handlers.AddOperativeHandler;
import com.flezha.sdk.runtime.control.handlers.AssignRunnerIdHandler;
import com.flezha.sdk.runtime.control.handlers.BoundedStopHandler;
import com.flezha.sdk.runtime.control.handlers.CompactEntityHandler;
import com.flezha.sdk.runtime.control.handlers.ConnectSocketHandler;
import com.flezha.sdk.runtime.control.handlers.CreditGrantHandler;
import com.flezha.sdk.runtime.control.handlers.CreditWindowInitHandler;
import com.flezha.sdk.runtime.control.handlers.EntityApplyStateHandler;
import com.flezha.sdk.runtime.control.handlers.EntityMailboxStatusHandler;
import com.flezha.sdk.runtime.control.handlers.EntityStateHandler;
import com.flezha.sdk.runtime.control.handlers.MigrationDeserializationHandler;
import com.flezha.sdk.runtime.control.handlers.MigrationSerializationHandler;
import com.flezha.sdk.runtime.control.handlers.OperativeMessageHandler;
import com.flezha.sdk.runtime.control.handlers.ParkPayloadAckHandler;
import com.flezha.sdk.runtime.control.handlers.PingHandler;
import com.flezha.sdk.runtime.control.handlers.RemOperativeHandler;
import com.flezha.sdk.runtime.control.handlers.RestoreInitStateHandler;
import com.flezha.sdk.runtime.control.handlers.RunnerDisconnectHandler;
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

    private final RunnerContext ctx;
    private final long controlStreamId;
    private final Map<CommandRequest.CommandCase, CommandHandler> handlers;

    public CommandRouter(@Nonnull RunnerContext ctx, long controlStreamId) {
        this.ctx = ctx;
        this.controlStreamId = controlStreamId;
        java.util.Map<CommandRequest.CommandCase, CommandHandler> mut = new java.util.LinkedHashMap<>();
        mut.put(CommandRequest.CommandCase.ASSIGN_RUNNER_ID, new AssignRunnerIdHandler());
        mut.put(CommandRequest.CommandCase.CONNECT_SOCKET, new ConnectSocketHandler());
        mut.put(CommandRequest.CommandCase.ADD_OPERATIVE, new AddOperativeHandler());
        mut.put(CommandRequest.CommandCase.REM_OPERATIVE, new RemOperativeHandler());
        mut.put(CommandRequest.CommandCase.ENTITY_STATE, new EntityStateHandler());
        mut.put(CommandRequest.CommandCase.MIGRATION_SERIALIZATION, new MigrationSerializationHandler());
        mut.put(CommandRequest.CommandCase.MIGRATION_DESERIALIZATION, new MigrationDeserializationHandler());
        mut.put(CommandRequest.CommandCase.RUNNER_DISCONNECT, new RunnerDisconnectHandler());
        mut.put(CommandRequest.CommandCase.OPERATIVE_MESSAGE, new OperativeMessageHandler());
        mut.put(CommandRequest.CommandCase.ENTITY_APPLY_STATE, new EntityApplyStateHandler());
        // State-persistence handlers added by operative-state-persistence.
        // The matching CommandCase enum constants land in the protobuf
        // codegen once the .proto regenerates; the lookups are resolved
        // reflectively to keep this file resilient to a stale codegen jar.
        registerCase(mut, "RESTORE_INIT_STATE", new RestoreInitStateHandler());
        registerCase(mut, "COMPACT_ENTITY", new CompactEntityHandler());
        registerCase(mut, "ENTITY_MAILBOX_STATUS", new EntityMailboxStatusHandler());
        // Cold-restart end-to-end health probe (fix-overwatch-restart-storm).
        // Resolved reflectively because the protobuf codegen jar may lag the
        // .proto file in a partial-deploy window.
        registerCase(mut, "PING", new PingHandler());
        // Cooperative external stop for a bounded operative (bounded-operative-functionality).
        registerCase(mut, "BOUNDED_STOP", new BoundedStopHandler());
        // Credit-grant flow control: the init arms the sender window and acks
        // in its CommandResponse; grants replenish it.
        registerCase(mut, "CREDIT_WINDOW_INIT", new CreditWindowInitHandler());
        registerCase(mut, "CREDIT_GRANT", new CreditGrantHandler());
        // Dead-letter park durability ack: wakes the dispatch thread parked
        // in DlqPark.parkBlocking.
        registerCase(mut, "PARK_PAYLOAD_ACK", new ParkPayloadAckHandler());
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
            case RUNNER_SHUTDOWN_ACK:
                // The bounded driver blocks on these at its proc / iteration barriers;
                // they carry the originating request's reference_id so the right
                // waiter wakes.
            case PROC_COMMIT_RESPONSE:
            case ITER_COMMIT_RESPONSE:
                AckRegistry.complete(request.getReferenceId());
                return;
            case SPAWN_BOUNDED_RESPONSE:
                // Procedure-requested spawn resolved: wake the requester's
                // SpawnHandle with the outcome the controller settled on.
                com.flezha.sdk.proto.SpawnBoundedResponse spawn = request.getSpawnBoundedResponse();
                if (spawn.getError().isEmpty()) {
                    AckRegistry.complete(request.getReferenceId(), spawn.getOutcome());
                } else {
                    AckRegistry.fail(
                            request.getReferenceId(),
                            new com.flezha.sdk.procedure.SpawnException.Refused(spawn.getError()));
                }
                return;
            case RUNNER_SHUTDOWN_NOTIFICATION:
                LOG.warn(
                        "Received unexpected RunnerShutdownNotification (ref={}); ignoring — "
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
        CommandHandler.Reply reply;
        try {
            reply = handler.reply(ctx, request, errors);
        } catch (Exception e) {
            LOG.error("Command handler threw for {}: {}", request.getCommandCase(), e.toString());
            errors.add(e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
            reply = new CommandHandler.Reply(new byte[0], Map.of());
        }

        CommandResponse response = CommandResponse.newBuilder()
                .setReferenceId(request.getReferenceId())
                .setStatus(errors.isEmpty() ? "success" : "error")
                .addAllErrors(errors)
                .setRawData(ByteString.copyFrom(reply.rawData()))
                .build();

        byte[] frameBytes = FrameEncoder.build(frame.payloadId(), response.toByteArray(), reply.metadata());

        int writeStatus = NativeTransport.streamWrite(ctx.nativeHandle(), controlStreamId, frameBytes);
        if (writeStatus != 0) {
            LOG.error("Failed to write CommandResponse (status={})", writeStatus);
        }

        if (handler.closeAfterResponse()) {
            NativeTransport.streamClose(ctx.nativeHandle(), controlStreamId);
        }
    }
}
