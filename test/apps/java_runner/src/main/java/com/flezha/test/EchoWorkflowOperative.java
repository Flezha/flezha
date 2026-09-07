package com.flezha.test;

import com.flezha.sdk.operative.WorkflowOperative;
import com.flezha.sdk.operative.WorkflowOperativeCtx;
import com.flezha.sdk.operative.WorkflowOperativeException;

/**
 * Run-once workflow operative mirroring {@code rust_runner}'s
 * {@code echo_workflow}: emits {@code echo:<args>} as plugin output once, then
 * returns so the SDK ships {@code TaskCompleted} and the controller deregisters
 * it.
 */
public final class EchoWorkflowOperative implements WorkflowOperative {

    @Override
    public String factoryId() {
        return "echo_workflow";
    }

    @Override
    public void run(WorkflowOperativeCtx ctx, String args) throws WorkflowOperativeException {
        ctx.emit("echo:" + args);
    }
}
