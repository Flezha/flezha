package com.flezha.examples.spawn;

import com.flezha.sdk.operative.WorkflowOperative;
import com.flezha.sdk.operative.WorkflowOperativeCtx;
import com.flezha.sdk.operative.WorkflowOperativeException;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nonnull;

/** The worker: one per subject, torn down when it returns. */
public final class TriageOperative implements WorkflowOperative {

    @Override
    @Nonnull
    public String factoryId() {
        return "triage";
    }

    @Override
    public void run(@Nonnull WorkflowOperativeCtx ctx, @Nonnull String args) throws WorkflowOperativeException {
        ctx.emit(("triaged:" + args).getBytes(StandardCharsets.UTF_8));
    }
}
