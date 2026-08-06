package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.runtime.RunnerContext;
import com.flezha.sdk.runtime.control.CommandHandler;
import com.flezha.sdk.runtime.operative.UnboundedOperativeInstance;
import com.flezha.sdk.runtime.operative.UnboundedOperativeStateSerializer;
import java.util.List;

public final class EntityStateHandler implements CommandHandler {

    @Override
    public byte[] handle(RunnerContext ctx, CommandRequest request, List<String> errors) throws Exception {
        String operativeId = request.getEntityState().getOperativeId();
        UnboundedOperativeInstance instance = ctx.operativeInstanceRegistry().getUnboundedOperative(operativeId);
        if (instance == null) {
            errors.add("UnboundedOperative with id " + operativeId + " not found");
            return new byte[0];
        }
        return UnboundedOperativeStateSerializer.serialize(instance);
    }
}
