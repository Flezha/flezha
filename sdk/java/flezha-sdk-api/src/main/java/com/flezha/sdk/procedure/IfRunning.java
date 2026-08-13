package com.flezha.sdk.procedure;

import javax.annotation.Nonnull;

/**
 * What {@link ProcedureCtx#spawnWorkflow} does when the requested instance id is
 * already running.
 */
public enum IfRunning {
    /** Deliver the payload to the running instance's inbox. */
    DELIVER("deliver"),
    /** Leave the running instance alone and report {@link SpawnException.AlreadyRunning}. */
    ERROR("error");

    private final String wire;

    IfRunning(String wire) {
        this.wire = wire;
    }

    /** The policy as the controller reads it off {@code SpawnWorkflowRequest}. */
    @Nonnull
    public String wire() {
        return wire;
    }
}
