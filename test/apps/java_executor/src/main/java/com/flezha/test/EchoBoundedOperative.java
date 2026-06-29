package com.flezha.test;

import com.flezha.sdk.bounded.BoundedOperative;
import com.flezha.sdk.bounded.BoundedOperativeCtx;
import com.flezha.sdk.bounded.BoundedOperativeException;

/**
 * Run-once bounded operative mirroring {@code rust_executor}'s echo bounded
 * operative: emits {@code echo:<args>} as plugin output and completes. When {@code args}
 * contains {@code fail} (e.g. {@code init_values} {@code {"mode":"fail"}}) it
 * throws, so the integration suite can exercise the terminal-error completion
 * path with one bounded class.
 */
public final class EchoBoundedOperative implements BoundedOperative {

    @Override
    public String factoryId() {
        return "echo_bounded";
    }

    @Override
    public void run(BoundedOperativeCtx ctx, String args) throws BoundedOperativeException {
        if (args.contains("fail")) {
            throw new BoundedOperativeException("echo bounded operative asked to fail");
        }
        ctx.emit("echo:" + args);
    }
}
