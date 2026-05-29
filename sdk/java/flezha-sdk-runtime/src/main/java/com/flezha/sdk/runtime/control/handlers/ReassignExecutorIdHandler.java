package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.native_.NativeTransport;
import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.runtime.ExecutorContext;
import com.flezha.sdk.runtime.ExecutorIdStore;
import com.flezha.sdk.runtime.control.CommandHandler;
import java.util.List;

/**
 * Handler for {@code ReassignExecutorId}. Issued when the controller has
 * reassigned this SDK's bindings to a different executor session during a
 * disconnect window. Updates the in-memory executor id in place (no registry
 * mutation, no status change) and persists it node-locally so a later restart
 * announces it as {@code previous_executor_id}. Mirrors the Rust SDK's
 * {@code ReassignExecutorId} handler.
 */
public final class ReassignExecutorIdHandler implements CommandHandler {

    @Override
    public byte[] handle(ExecutorContext ctx, CommandRequest request, List<String> errors) {
        String id = request.getReassignExecutorId().getNewId();
        ctx.setExecutorId(id);
        NativeTransport.setExecutorId(ctx.nativeHandle(), id);
        ExecutorIdStore.persist(id);
        return new byte[0];
    }
}
