package com.flezha.sdk.runtime.core;

import com.flezha.sdk.native_.FlzCore;
import com.flezha.sdk.procedure.SpawnRequests;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;

/**
 * The {@link SpawnRequests.Requester} behind {@code ctx.spawnWorkflow}: the
 * poll core's {@code spawnWorkflow} ABI call, run off the caller's thread (the
 * ABI blocks on the controller round-trip). Installed by the runtime at
 * {@code start()}; the ABI function is injectable so the installation can be
 * tested without a native context.
 */
public final class CoreSpawnRequester implements SpawnRequests.Requester {

    /** The blocking spawn ABI — {@code FlzCore.spawnWorkflow} in production. */
    @FunctionalInterface
    public interface SpawnFn {
        String spawn(long ctx, String instanceId, String factoryId, byte[] payload, String ifRunning);
    }

    private final long ctx;
    private final SpawnFn fn;

    public CoreSpawnRequester(long ctx) {
        this(ctx, FlzCore::spawnWorkflow);
    }

    public CoreSpawnRequester(long ctx, @Nonnull SpawnFn fn) {
        this.ctx = ctx;
        this.fn = fn;
    }

    @Override
    @Nonnull
    public CompletableFuture<String> request(
            @Nonnull String instanceId, @Nonnull String factoryId, @Nonnull byte[] payload, @Nonnull String ifRunning) {
        return CompletableFuture.supplyAsync(() -> fn.spawn(ctx, instanceId, factoryId, payload, ifRunning));
    }
}
