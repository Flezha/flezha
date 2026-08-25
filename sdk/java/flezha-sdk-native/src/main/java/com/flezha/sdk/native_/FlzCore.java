package com.flezha.sdk.native_;

/**
 * Java surface of the poll-based SDK core ({@code flz_core_ffi}). The host
 * polls postcard-encoded Activations, runs user code, and completes with
 * postcard-encoded ActivationResults.
 *
 * <p>Buffer ownership: every {@code byte[]} returned is a Java-owned copy —
 * the native buffer is freed before the call returns; every {@code byte[]}
 * passed in is copied by the native side before the call returns.
 *
 * <p>Native method names are the short JNI form
 * ({@code Java_com_flezha_sdk_native_1_FlzCore_<method>}), matching the
 * exports in {@code flz_core_ffi/src/jni_glue.rs} 1:1. Failures throw
 * {@link FlezhaNativeException}.
 */
public final class FlzCore {

    /** The activation schema version this shim was built against. */
    public static final int SCHEMA_VERSION = 3;

    static {
        NativeLibraryLoader.loadNativeLibrary("libflz_core_ffi");
    }

    private FlzCore() {}

    /** Create a context negotiated at {@link #SCHEMA_VERSION}. */
    public static long newContext() {
        return contextNew(SCHEMA_VERSION);
    }

    /** The activation schema version the loaded native core speaks. */
    public static native int schemaVersion();

    /**
     * Create the core context, negotiating the schema version. Throws on
     * mismatch, naming both versions. No controller connection is attempted.
     */
    public static native long contextNew(int shimSchemaVersion);

    /** Declare one host class from its JSON declaration. Throws the refusal. */
    public static native void declareClass(long ctx, byte[] json);

    /** Connect to the controller and start the runner loop. Throws on failure. */
    public static native void start(long ctx, String namespace);

    /**
     * Block up to {@code timeoutMs} for one activation. {@code null} = timeout;
     * a zero-length array = shutting down (an encoded activation is never
     * empty); otherwise the postcard-encoded Activation bytes.
     */
    public static native byte[] poll(long ctx, long timeoutMs);

    /**
     * Complete an activation with a postcard-encoded ActivationResult.
     * Accepted from any thread. Throws on a decode/apply refusal.
     */
    public static native void complete(long ctx, byte[] result);

    /** {@link #waitStarvation} saw work queued with no thread left to take it. */
    public static final int STARVATION_OBSERVED = 1;

    /** {@link #waitStarvation} reached its timeout with nothing owed. */
    public static final int STARVATION_TIMEOUT = 0;

    /** {@link #waitStarvation} returned because the core is shutting down. */
    public static final int STARVATION_SHUTTING_DOWN = 2;

    /**
     * Block up to {@code timeoutMs} until the core observes starvation —
     * runnable work with no thread inside {@link #poll} and the jobs-underway
     * cap not reached. Returns {@link #STARVATION_OBSERVED},
     * {@link #STARVATION_TIMEOUT} or {@link #STARVATION_SHUTTING_DOWN}.
     *
     * <p>The other half of the bring-your-own-thread contract: the core caps
     * how much runs, the host supplies threads, and this is how the host is
     * told it owes one. A host that ships a fixed worker count and never calls
     * it wedges with no recovery and no report when every worker is held.
     */
    public static native int waitStarvation(long ctx, long timeoutMs);

    /**
     * Abandon a polled activation by token: its untouched items are re-handed
     * and its entity released, exactly as if it had never been polled. No
     * retry-ladder attempt is consumed.
     *
     * <p>The third and last way to answer a handout. No unwind crosses the
     * native boundary, so a throwable that escapes the dispatch has no way to
     * reach {@link #complete}; without this call the activation's entity would
     * be retired for the life of the process (per-entity exclusivity is the
     * handout itself). Call it from a {@code finally}, never from a
     * hand-written return site.
     *
     * @throws RuntimeException if the token was never handed out or was
     *     already answered
     */
    public static native void abandon(long ctx, long token);

    /** Emit one output payload from a live workflow run-sugar body. */
    public static native void workflowEmit(long ctx, String instanceId, byte[] payload);

    /**
     * Block up to {@code timeoutMs} for the next inbox item of a live workflow
     * run-sugar body. {@code null} = timeout; throws when the body is gone or
     * the inbox closed.
     */
    public static native byte[] workflowRecv(long ctx, String instanceId, long timeoutMs);

    /** Whether the named workflow body's cancellation token has tripped. */
    public static native boolean workflowCancelled(long ctx, String instanceId);

    /**
     * The ABI form of {@code ctx.spawnWorkflow}: spawn-or-deliver a workflow
     * instance in the runner's own namespace. {@code ifRunning} is the wire
     * string {@code "deliver"} or {@code "error"}. Returns the controller's
     * outcome string ({@code "spawned"}, {@code "delivered"}, ...); throws on
     * refusal, no control plane, or timeout.
     */
    public static native String spawnWorkflow(
            long ctx, String instanceId, String factoryId, byte[] payload, String ifRunning);

    /**
     * The core's monotonic clock in nanoseconds since an arbitrary process
     * origin — the reading {@link #entryLastDeliveryNanos(String)} is measured
     * against. Context-free: the core records deliveries process-wide.
     */
    public static native long monotonicNanos();

    /**
     * When the named entry channel last received a frame, on the same
     * monotonic clock. {@code 0} means no frame ever has — which is also the
     * answer for a channel no declared operative has.
     */
    public static native long entryLastDeliveryNanos(String channel);

    /** Wake every blocked poller and stop the runner loop. */
    public static native void shutdown(long ctx);

    /** Destroy the context. The handle is dead afterwards. */
    public static native void contextFree(long ctx);
}
