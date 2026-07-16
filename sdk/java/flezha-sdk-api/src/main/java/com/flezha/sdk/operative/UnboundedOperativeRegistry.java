package com.flezha.sdk.operative;

import com.flezha.sdk.internal.RegistryCore;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * Process-global registry of registered UnboundedOperative classes. One entry per UnboundedOperative
 * class, keyed by {@code factoryId()}. The registry is frozen at
 * {@code RunnerBuilder.start()} time.
 *
 * <p>Acts as the public-API view of the runtime's {@code UnboundedOperativeClassRegistry}
 * (which holds the frozen {@code UnboundedOperativeTopology} produced by inspecting each
 * registered class). A thin static facade over the shared
 * {@link RegistryCore}, which reaches the runtime side reflectively so
 * {@code flezha-sdk-api} compiles standalone.
 */
public final class UnboundedOperativeRegistry {

    private static final RegistryCore<UnboundedOperative> CORE =
            new RegistryCore<>("com.flezha.sdk.runtime.operative.UnboundedOperativeClassRegistry", "operative");

    private UnboundedOperativeRegistry() {}

    /**
     * Register an UnboundedOperative class. Called by {@code RunnerBuilder.defineUnbounded(Class)} before {@code start()}.
     *
     * @throws IllegalStateException if the registry is already frozen.
     * @throws IllegalArgumentException on duplicate factory id.
     * @throws UnboundedOperativeConfigurationException if the class's topology declaration
     *     fails the wiring constraints (entry-channel-type, type-equality,
     *     unwired-channel, multi-producer, default-impl detection).
     */
    public static void registerUnbounded(@Nonnull Class<? extends UnboundedOperative> operativeClass) {
        CORE.register(operativeClass);
    }

    /** Freeze the registry. Called by {@code RunnerBuilder.start()}. */
    public static void freeze() {
        CORE.freeze();
    }

    /**
     * Unfreeze the registry, keeping its entries. Called when the active
     * runner closes, so a later runner in the same JVM may declare new
     * operative types.
     */
    public static void unfreeze() {
        CORE.unfreeze();
    }

    /** Thaw the registry. Intended for tests that reset SDK state between runs. */
    public static void reset() {
        CORE.reset();
    }

    @Nonnull
    public static List<String> factoryIds() {
        return CORE.factoryIds();
    }

    public static boolean isEmpty() {
        return CORE.isEmpty();
    }

    public static boolean contains(@Nonnull String factoryId) {
        return CORE.contains(factoryId);
    }
}
