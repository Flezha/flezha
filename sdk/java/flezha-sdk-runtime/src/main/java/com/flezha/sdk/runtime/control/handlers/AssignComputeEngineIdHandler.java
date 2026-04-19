package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.native_.NativeTransport;
import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.runtime.ComputeEngineContext;
import com.flezha.sdk.runtime.ComputeEngineStatus;
import com.flezha.sdk.runtime.control.CommandHandler;
import java.util.List;

public final class AssignComputeEngineIdHandler implements CommandHandler {

    @Override
    public byte[] handle(ComputeEngineContext ctx, CommandRequest request, List<String> errors) {
        String id = request.getAssignComputeEngineId().getComputeEngineId();
        ctx.setComputeEngineId(id);
        ctx.setStatus(ComputeEngineStatus.RUNNING);
        NativeTransport.setComputeEngineId(ctx.nativeHandle(), id);
        return new byte[0];
    }
}
