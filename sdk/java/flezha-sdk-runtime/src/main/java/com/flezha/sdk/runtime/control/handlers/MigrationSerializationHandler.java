package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.runtime.ExecutorContext;
import com.flezha.sdk.runtime.control.CommandHandler;
import com.flezha.sdk.runtime.relay.RelayInstance;
import com.flezha.sdk.runtime.relay.RelayStateSerializer;
import java.util.List;

public final class MigrationSerializationHandler implements CommandHandler {

    @Override
    public byte[] handle(ExecutorContext ctx, CommandRequest request, List<String> errors) throws Exception {
        String relayId = request.getMigrationSerialization().getRelayId();
        if (ctx.relayInstanceRegistry().getRelay(relayId) == null) {
            errors.add("Relay with id " + relayId + " not found");
            return new byte[0];
        }
        RelayInstance instance = (RelayInstance) ctx.relayInstanceRegistry().remove(relayId);
        return RelayStateSerializer.serialize(instance);
    }
}
