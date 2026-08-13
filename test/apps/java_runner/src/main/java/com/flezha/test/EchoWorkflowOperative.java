package com.flezha.test;

import com.flezha.sdk.workflow.WorkflowOperative;
import com.flezha.sdk.workflow.WorkflowOperativeCtx;
import com.flezha.sdk.workflow.WorkflowOperativeException;

/**
 * Run-once workflow operative mirroring {@code rust_runner}'s echo workflow
 * operative: emits {@code echo:<args>} as plugin output and completes. When {@code args}
 * contains {@code fail} (e.g. {@code init_values} {@code {"mode":"fail"}}) it
 * throws, so the integration suite can exercise the terminal-error completion
 * path with one workflow class.
 */
public final class EchoWorkflowOperative implements WorkflowOperative {

    @Override
    public String factoryId() {
        return "echo_workflow";
    }

    @Override
    public void run(WorkflowOperativeCtx ctx, String args) throws WorkflowOperativeException {
        if (args.contains("fail")) {
            throw new WorkflowOperativeException("echo workflow operative asked to fail");
        }
        ctx.emit("echo:" + args);
    }
}
