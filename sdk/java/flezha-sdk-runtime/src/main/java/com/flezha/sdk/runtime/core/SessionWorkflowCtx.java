package com.flezha.sdk.runtime.core;

import com.flezha.sdk.workflow.WorkflowLoopBody;
import com.flezha.sdk.workflow.WorkflowOperativeCtx;
import com.flezha.sdk.workflow.WorkflowOperativeException;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import javax.annotation.Nonnull;

/** {@link WorkflowOperativeCtx} over an injected {@link WorkflowSession}. */
final class SessionWorkflowCtx implements WorkflowOperativeCtx {

    private final WorkflowSession session;
    byte[] result = new byte[0];
    boolean completed;

    SessionWorkflowCtx(WorkflowSession session) {
        this.session = session;
    }

    @Override
    public void emit(@Nonnull byte[] value) {
        session.emit(value);
    }

    @Override
    public void complete(@Nonnull byte[] result) {
        this.result = result;
        this.completed = true;
    }

    @Override
    public boolean isCancelled() {
        return session.cancelled();
    }

    @Override
    @Nonnull
    public Optional<byte[]> recv() {
        while (true) {
            if (isCancelled() || completed) {
                return Optional.empty();
            }
            byte[] item;
            try {
                // ≤200 ms wake granularity so cancellation is honoured.
                item = session.recv(200);
            } catch (RuntimeException e) {
                // Body gone / inbox permanently closed.
                return Optional.empty();
            }
            if (item != null) {
                return Optional.of(item);
            }
        }
    }

    @Override
    public void loopUntil(@Nonnull BooleanSupplier predicate, @Nonnull WorkflowLoopBody body)
            throws WorkflowOperativeException {
        while (!isCancelled() && !completed && !predicate.getAsBoolean()) {
            body.run(this);
        }
    }
}
