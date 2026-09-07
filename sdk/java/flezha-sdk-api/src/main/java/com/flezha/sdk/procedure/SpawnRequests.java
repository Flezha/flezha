package com.flezha.sdk.procedure;

import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;

/**
 * The seam between {@link ProcedureCtx#spawnWorkflow} and the control plane. The
 * runtime installs a {@link Requester} when it opens the control stream; the api
 * layer — which cannot see the wire — routes every request through it.
 *
 * <p>Mirrors the Rust SDK's process-global coordinator handle: one installed
 * sender, and a request that resolves immediately (rather than waiting out its
 * deadline) when there is nothing to send on.
 */
public final class SpawnRequests {

    /** Ships one {@code SpawnWorkflowRequest} and returns its pending reply. */
    @FunctionalInterface
    public interface Requester {
        @Nonnull
        CompletableFuture<String> request(String instanceId, String factoryId, byte[] payload, String ifRunning);
    }

    private static volatile Requester requester;

    private SpawnRequests() {}

    /** Install the control-plane requester. A later install replaces the earlier one. */
    public static void install(Requester newRequester) {
        requester = newRequester;
    }

    /** Drop the installed requester (runner close, and test isolation). */
    public static void uninstall() {
        requester = null;
    }

    /**
     * Issue the request, unless this invocation is a replay — spawning is a
     * side effect of live processing only, so a replay answers
     * {@link SpawnOutcome#SKIPPED_ON_REPLAY} without touching the wire.
     */
    @Nonnull
    public static SpawnHandle request(
            String instanceId, String factoryId, byte[] payload, IfRunning ifRunning, boolean replaying) {
        if (replaying) {
            return SpawnHandle.resolved(instanceId, factoryId, SpawnOutcome.SKIPPED_ON_REPLAY);
        }
        Requester current = requester;
        if (current == null) {
            return SpawnHandle.failed(instanceId, factoryId, new SpawnException.Unavailable());
        }
        return SpawnHandle.pending(
                instanceId, factoryId, current.request(instanceId, factoryId, payload, ifRunning.wire()));
    }
}
