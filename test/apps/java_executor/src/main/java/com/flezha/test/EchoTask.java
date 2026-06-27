package com.flezha.test;

import com.flezha.sdk.task.Task;
import com.flezha.sdk.task.TaskCtx;
import com.flezha.sdk.task.TaskException;

/**
 * Run-once task mirroring {@code rust_executor}'s {@code EchoTask}: emits
 * {@code echo:<args>} as plugin output and completes. When {@code args}
 * contains {@code fail} (e.g. {@code init_values} {@code {"mode":"fail"}}) it
 * throws, so the integration suite can exercise the terminal-error completion
 * path with one task class.
 */
public final class EchoTask implements Task {

    @Override
    public String factoryId() {
        return "echo_task";
    }

    @Override
    public void run(TaskCtx ctx, String args) throws TaskException {
        if (args.contains("fail")) {
            throw new TaskException("echo task asked to fail");
        }
        ctx.emit("echo:" + args);
    }
}
