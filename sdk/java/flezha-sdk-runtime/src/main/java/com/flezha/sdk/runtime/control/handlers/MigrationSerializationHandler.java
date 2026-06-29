package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.runtime.ExecutorContext;
import com.flezha.sdk.runtime.control.CommandHandler;
import com.flezha.sdk.runtime.operative.ContinuousOperativeInstance;
import com.flezha.sdk.runtime.operative.ContinuousOperativeStateSerializer;
import java.util.List;

public final class MigrationSerializationHandler implements CommandHandler {

    @Override
    public byte[] handle(ExecutorContext ctx, CommandRequest request, List<String> errors) throws Exception {
        String operativeId = request.getMigrationSerialization().getOperativeId();
        if (ctx.operativeInstanceRegistry().getContinuousOperative(operativeId) == null) {
            errors.add("ContinuousOperative with id " + operativeId + " not found");
            return new byte[0];
        }
        ContinuousOperativeInstance instance = (ContinuousOperativeInstance) ctx.operativeInstanceRegistry().remove(operativeId);
        return ContinuousOperativeStateSerializer.serialize(instance);
    }
}
