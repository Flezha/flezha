package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.runtime.ComputeEngineContext;
import com.flezha.sdk.runtime.control.CommandHandler;
import com.flezha.sdk.runtime.relay.RelayInstance;
import java.util.List;

/**
 * Handler for {@code RemProcessor}. Removes the {@link RelayInstance} from
 * the registry; pending mailbox items on its partitions are dropped without
 * being drained (matches Rust SDK v1 — bounded mailboxes / drain-on-remove
 * is tracked in a future change).
 */
public final class RemProcessorHandler implements CommandHandler {

    @Override
    public byte[] handle(ComputeEngineContext ctx, CommandRequest request, List<String> errors) {
        String processorId = request.getRemProcessor().getProcessorId();
        RelayInstance removed = ctx.relayInstanceRegistry().remove(processorId);
        if (removed == null) {
            errors.add("Processor not found");
        }
        return new byte[0];
    }
}
