package com.flezha.sdk.runtime.core;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * The live seam of one workflow run-sugar body: emit / recv / cancellation
 * resolve against the poll core's per-instance session while the host runs the
 * imperative body. Injected into {@link ActivationDispatcher} so the dispatch
 * layer stays free of native calls; production wires it to
 * {@code FlzCore.workflowEmit/workflowRecv/workflowCancelled}.
 */
public interface WorkflowSession {

    /** Emit one output payload to the instance's bound plugins. */
    void emit(@Nonnull byte[] payload);

    /**
     * Block up to {@code timeoutMs} for the next inbox item. {@code null} =
     * timeout; a thrown exception means the body is gone or the inbox closed.
     */
    @Nullable
    byte[] recv(long timeoutMs);

    /** Whether the instance's cancellation token has tripped. */
    boolean cancelled();
}
