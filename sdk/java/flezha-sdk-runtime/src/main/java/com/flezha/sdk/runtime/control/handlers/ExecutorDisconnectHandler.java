package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.runtime.ExecutorContext;
import com.flezha.sdk.runtime.ExecutorStatus;
import com.flezha.sdk.runtime.control.CommandHandler;
import java.util.List;

public final class ExecutorDisconnectHandler implements CommandHandler {

    @Override
    public byte[] handle(ExecutorContext ctx, CommandRequest request, List<String> errors) {
        ctx.setStatus(ExecutorStatus.STOPPED);
        return new byte[0];
    }

    @Override
    public boolean closeAfterResponse() {
        return true;
    }
}
