package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.runtime.ExecutorContext;
import com.flezha.sdk.runtime.control.CommandHandler;
import com.flezha.sdk.runtime.operative.ContinuousOperativeInstance;
import com.flezha.sdk.runtime.operative.ContinuousOperativeStateSerializer;
import java.util.List;

public final class SegmentStateHandler implements CommandHandler {

    @Override
    public byte[] handle(ExecutorContext ctx, CommandRequest request, List<String> errors) throws Exception {
        String operativeId = request.getSegmentState().getOperativeId();
        ContinuousOperativeInstance instance = ctx.operativeInstanceRegistry().getContinuousOperative(operativeId);
        if (instance == null) {
            errors.add("ContinuousOperative with id " + operativeId + " not found");
            return new byte[0];
        }
        return ContinuousOperativeStateSerializer.serialize(instance);
    }
}
