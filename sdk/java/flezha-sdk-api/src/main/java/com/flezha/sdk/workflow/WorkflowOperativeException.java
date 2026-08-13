package com.flezha.sdk.workflow;

/**
 * A terminal workflow failure. Thrown from {@link WorkflowOperative#run} (or {@link WorkflowOperativeCtx#emit})
 * to signal that the job failed; the SDK reports it to the controller as a
 * {@code WorkflowCompleted} with {@code status = "error"} and tears the workflow
 * down without retry. Checked, mirroring {@code ProcedureException}.
 */
public class WorkflowOperativeException extends Exception {

    /**
     * Whether the entity's batch may be re-run. Retryable by default: an author
     * who has not classified the failure has not claimed it is permanent, and a
     * thrown {@code RuntimeException} carries no classification at all.
     */
    private final boolean nonRetryable;

    public WorkflowOperativeException(String message) {
        this(message, false);
    }

    public WorkflowOperativeException(String message, Throwable cause) {
        super(message, cause);
        this.nonRetryable = false;
    }

    private WorkflowOperativeException(String message, boolean nonRetryable) {
        super(message);
        this.nonRetryable = nonRetryable;
    }

    /**
     * A failure that re-running cannot fix — a malformed record, a rejected
     * charge. Skips the retry ladder and goes straight to the abort path.
     */
    public static WorkflowOperativeException nonRetryable(String message) {
        return new WorkflowOperativeException(message, true);
    }

    public boolean isNonRetryable() {
        return nonRetryable;
    }
}
