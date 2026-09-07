package com.flezha.sdk.operative;

/**
 * Saga rollback scope chosen per workflow operative (see
 * {@link WorkflowOperative#rollbackMode}). Applied when the operative terminates
 * abnormally after partial progress; irrelevant for a clean completion or an
 * operative with no compensation hooks.
 */
public enum RollbackMode {
    /**
     * Any failure undoes <strong>all</strong> completed procedures in reverse,
     * back to the start (the classic saga). The default.
     */
    ALL_OR_NOTHING,
    /**
     * Each finished loop iteration commits (is kept); a failure undoes only the
     * in-flight iteration's completed procedures. Within a single iteration — or
     * a no-loop pass — rollback is all-or-nothing.
     */
    KEEP_PROGRESS
}
