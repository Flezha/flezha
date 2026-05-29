package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.native_.NativeTransport;
import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.runtime.ExecutorContext;
import com.flezha.sdk.runtime.ExecutorIdStore;
import com.flezha.sdk.runtime.ExecutorStatus;
import com.flezha.sdk.runtime.control.CommandHandler;
import java.util.List;

public final class AssignExecutorIdHandler implements CommandHandler {

    @Override
    public byte[] handle(ExecutorContext ctx, CommandRequest request, List<String> errors) {
        String id = request.getAssignExecutorId().getExecutorId();
        ctx.setExecutorId(id);
        ctx.setStatus(ExecutorStatus.RUNNING);
        NativeTransport.setExecutorId(ctx.nativeHandle(), id);
        // Persist node-locally so a fresh process can announce this id as
        // previous_executor_id and let the controller evict the stale binding
        // before re-binding.
        ExecutorIdStore.persist(id);
        return new byte[0];
    }
}
