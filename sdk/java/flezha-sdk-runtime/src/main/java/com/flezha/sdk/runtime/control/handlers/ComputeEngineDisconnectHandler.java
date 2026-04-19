package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.runtime.ComputeEngineContext;
import com.flezha.sdk.runtime.ComputeEngineStatus;
import com.flezha.sdk.runtime.control.CommandHandler;
import java.util.List;

public final class ComputeEngineDisconnectHandler implements CommandHandler {

    @Override
    public byte[] handle(ComputeEngineContext ctx, CommandRequest request, List<String> errors) {
        ctx.setStatus(ComputeEngineStatus.STOPPED);
        return new byte[0];
    }

    @Override
    public boolean closeAfterResponse() {
        return true;
    }
}
