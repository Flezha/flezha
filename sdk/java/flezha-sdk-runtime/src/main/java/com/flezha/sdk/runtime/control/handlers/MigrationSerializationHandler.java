package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.runtime.RunnerContext;
import com.flezha.sdk.runtime.control.CommandHandler;
import com.flezha.sdk.runtime.operative.UnboundedOperativeInstance;
import com.flezha.sdk.runtime.operative.UnboundedOperativeStateSerializer;
import java.util.List;

public final class MigrationSerializationHandler implements CommandHandler {

    @Override
    public byte[] handle(RunnerContext ctx, CommandRequest request, List<String> errors) throws Exception {
        String operativeId = request.getMigrationSerialization().getOperativeId();
        if (ctx.operativeInstanceRegistry().getUnboundedOperative(operativeId) == null) {
            errors.add("UnboundedOperative with id " + operativeId + " not found");
            return new byte[0];
        }
        UnboundedOperativeInstance instance =
                (UnboundedOperativeInstance) ctx.operativeInstanceRegistry().remove(operativeId);
        return UnboundedOperativeStateSerializer.serialize(instance);
    }
}
