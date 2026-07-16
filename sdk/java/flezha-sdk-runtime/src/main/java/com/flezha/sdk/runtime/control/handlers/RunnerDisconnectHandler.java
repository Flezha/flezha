package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.runtime.RunnerContext;
import com.flezha.sdk.runtime.RunnerStatus;
import com.flezha.sdk.runtime.control.CommandHandler;
import java.util.List;

public final class RunnerDisconnectHandler implements CommandHandler {

    @Override
    public byte[] handle(RunnerContext ctx, CommandRequest request, List<String> errors) {
        ctx.setStatus(RunnerStatus.STOPPED);
        return new byte[0];
    }

    @Override
    public boolean closeAfterResponse() {
        return true;
    }
}
