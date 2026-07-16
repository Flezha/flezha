package com.flezha.sdk.bounded;

import com.flezha.sdk.internal.RegistryCore;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * Process-global registry of registered {@link BoundedOperative} classes, keyed by
 * {@code factoryId()}. The public-API sibling of
 * {@link com.flezha.sdk.operative.UnboundedOperativeRegistry} — like it, a thin
 * static facade over the shared {@link RegistryCore}, which reaches the runtime
 * side ({@code com.flezha.sdk.runtime.bounded.BoundedOperativeClassRegistry})
 * reflectively so {@code flezha-sdk-api} compiles standalone. Frozen at
 * {@code RunnerBuilder.start()} time.
 */
public final class BoundedOperativeRegistry {

    private static final RegistryCore<BoundedOperative> CORE =
            new RegistryCore<>("com.flezha.sdk.runtime.bounded.BoundedOperativeClassRegistry", "bounded");

    private BoundedOperativeRegistry() {}

    /**
     * Register a BoundedOperative class. Called by {@code RunnerBuilder.defineBounded(Class)} before {@code start()}.
     *
     * @throws IllegalStateException if the registry is already frozen.
     * @throws IllegalArgumentException on a factory id that collides with a
     *     operative or another bounded.
     */
    public static void registerBounded(@Nonnull Class<? extends BoundedOperative> boundedClass) {
        CORE.register(boundedClass);
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
