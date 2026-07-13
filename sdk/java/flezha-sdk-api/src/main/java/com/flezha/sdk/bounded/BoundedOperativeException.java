package com.flezha.sdk.bounded;

/**
 * A terminal bounded failure. Thrown from {@link BoundedOperative#run} (or {@link BoundedOperativeCtx#emit})
 * to signal that the job failed; the SDK reports it to the controller as a
 * {@code BoundedCompleted} with {@code status = "error"} and tears the bounded
 * down without retry. Checked, mirroring {@code OperationException}.
 */
public class BoundedOperativeException extends Exception {

    /**
     * Whether the segment's batch may be re-run. Retryable by default: an author
     * who has not classified the failure has not claimed it is permanent, and a
     * thrown {@code RuntimeException} carries no classification at all.
     */
    private final boolean nonRetryable;

    public BoundedOperativeException(String message) {
        this(message, false);
    }

    public BoundedOperativeException(String message, Throwable cause) {
        super(message, cause);
        this.nonRetryable = false;
    }

    private BoundedOperativeException(String message, boolean nonRetryable) {
        super(message);
        this.nonRetryable = nonRetryable;
    }

    /**
     * A failure that re-running cannot fix — a malformed record, a rejected
     * charge. Skips the retry ladder and goes straight to the abort path.
     */
    public static BoundedOperativeException nonRetryable(String message) {
        return new BoundedOperativeException(message, true);
    }

    public boolean isNonRetryable() {
        return nonRetryable;
    }
}
