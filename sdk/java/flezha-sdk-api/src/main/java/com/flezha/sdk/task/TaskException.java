package com.flezha.sdk.task;

/**
 * A terminal task failure. Thrown from {@link Task#run} (or {@link TaskCtx#emit})
 * to signal that the job failed; the SDK reports it to the controller as a
 * {@code TaskCompleted} with {@code status = "error"} and tears the task
 * down without retry. Checked, mirroring {@code ShardException}.
 */
public class TaskException extends Exception {

    public TaskException(String message) {
        super(message);
    }

    public TaskException(String message, Throwable cause) {
        super(message, cause);
    }
}
