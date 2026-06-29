package com.flezha.sdk.bounded;

/**
 * A terminal bounded failure. Thrown from {@link BoundedOperative#run} (or {@link BoundedOperativeCtx#emit})
 * to signal that the job failed; the SDK reports it to the controller as a
 * {@code BoundedCompleted} with {@code status = "error"} and tears the bounded
 * down without retry. Checked, mirroring {@code OperationException}.
 */
public class BoundedOperativeException extends Exception {

    public BoundedOperativeException(String message) {
        super(message);
    }

    public BoundedOperativeException(String message, Throwable cause) {
        super(message, cause);
    }
}
