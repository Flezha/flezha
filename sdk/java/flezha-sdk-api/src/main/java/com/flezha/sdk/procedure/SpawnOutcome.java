package com.flezha.sdk.procedure;

/** A {@link ProcedureCtx#spawnBounded} request that reached an instance. */
public enum SpawnOutcome {
    /** The id was free: a fresh instance started with the payload as its first input. */
    SPAWNED,
    /** The id was taken: the payload became one more input of the running instance. */
    DELIVERED,
    /**
     * The invocation was a replay of already-processed input, so no request was
     * issued. Spawning is a side effect of live processing only.
     */
    SKIPPED_ON_REPLAY
}
