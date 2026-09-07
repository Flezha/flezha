package com.flezha.sdk.runtime.core;

import com.flezha.sdk.operative.WorkflowOperative;
import com.flezha.sdk.operative.WorkflowOperativeException;
import com.flezha.sdk.runtime.workflow.WorkflowSpec;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The workflow half of one live instance: whether the class is a structured
 * (declared-DAG) workflow, and — for a run-sugar class — the body run itself.
 * A stream class holds one of these with a {@code null} spec and asks it only
 * the first question.
 */
final class WorkflowDispatch {

    private static final Logger LOG = LoggerFactory.getLogger(WorkflowDispatch.class);

    private final String operativeId;
    private final String factoryId;

    @Nullable
    private final WorkflowSpec spec;

    WorkflowDispatch(String operativeId, String factoryId, @Nullable WorkflowSpec spec) {
        this.operativeId = operativeId;
        this.factoryId = factoryId;
        this.spec = spec;
    }

    /**
     * A structured (declared-DAG) workflow class, whose stages the core
     * folds into the driver's collect sink instead of routing them.
     */
    boolean isStructuredWorkflow() {
        return spec != null && !spec.isRunSugar();
    }

    ActivationOutcome run(byte[] args, Function<String, WorkflowSession> sessions) {
        if (spec == null || !spec.isRunSugar()) {
            throw new IllegalStateException(
                    "WorkflowRun activation for `" + factoryId + "`, which is not a run-sugar workflow class");
        }
        WorkflowOperative body;
        try {
            body = spec.operativeClass().getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(
                    "workflow class `" + factoryId + "` has no public no-arg constructor: " + e, e);
        }
        SessionWorkflowCtx ctx = new SessionWorkflowCtx(sessions.apply(operativeId));
        try {
            body.run(ctx, new String(args, StandardCharsets.UTF_8));
            return new ActivationOutcome.WorkflowDone(ctx.completed, ctx.result);
        } catch (WorkflowOperativeException e) {
            String message = e.getMessage() == null ? e.toString() : e.getMessage();
            return new ActivationOutcome.Failed(new WorkError(message, e.isNonRetryable()));
        } catch (Throwable t) {
            LOG.warn("workflow run-sugar body `{}` failed: {}", operativeId, t.toString());
            return new ActivationOutcome.Failed(new WorkError(t.toString(), false));
        }
    }
}
