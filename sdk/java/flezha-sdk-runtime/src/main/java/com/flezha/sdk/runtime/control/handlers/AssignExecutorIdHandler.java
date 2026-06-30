package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.native_.NativeTransport;
import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.runtime.ExecutorContext;
import com.flezha.sdk.runtime.ExecutorStatus;
import com.flezha.sdk.runtime.control.CommandHandler;
import java.util.List;

public final class AssignExecutorIdHandler implements CommandHandler {

    @Override
    public byte[] handle(ExecutorContext ctx, CommandRequest request, List<String> errors) {
        // The controller-minted value is this connection's SESSION TOKEN: it
        // keys the per-connection data-stream sockets and the controller-side
        // Registry/StreamChannel names. It is NOT persisted — the durable
        // identity is the self-owned stable id (see ExecutorIdStore), already
        // presented in the connect handshake.
        String id = request.getAssignExecutorId().getExecutorId();
        ctx.setExecutorId(id);
        ctx.setStatus(ExecutorStatus.RUNNING);
        NativeTransport.setExecutorId(ctx.nativeHandle(), id);
        return new byte[0];
    }
}
