package com.flezha.sdk.runtime.bounded;

import com.flezha.sdk.runtime.operative.ContinuousOperativeClassRegistry;
import com.flezha.sdk.bounded.BoundedOperative;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;

/**
 * Process-global registry of registered {@link BoundedOperative} classes keyed by
 * {@code factoryId}. The runtime sibling of {@link ContinuousOperativeClassRegistry};
 * reached reflectively from {@link com.flezha.sdk.bounded.BoundedOperativeRegistry} so
 * {@code flezha-sdk-api} compiles standalone. Looked up when an
 * {@code AddOperative} whose kind resolves to {@code :bounded} arrives.
 */
public final class BoundedOperativeClassRegistry {

    private static final Map<String, Class<? extends BoundedOperative>> ENTRIES = new ConcurrentHashMap<>();

    private BoundedOperativeClassRegistry() {}

    /**
     * Reflective entry point invoked from {@link com.flezha.sdk.bounded.BoundedOperativeRegistry}.
     * Instantiates the BoundedOperative class via its no-arg ctor to read a stable
     * {@code factoryId()}, then records {@code (factoryId → class)}. Rejects a
     * factory id that collides with a registered operative or another bounded.
     * Returns the controller-facing factory id.
     *
     * @throws IllegalArgumentException if the BoundedOperative class lacks a public no-arg
     *     constructor, returns a null/empty factory id, or collides with an
     *     existing operative/bounded.
     */
    @Nonnull
    public static String registerClass(@Nonnull Class<? extends BoundedOperative> boundedClass) {
        BoundedOperative bounded;
        try {
            bounded = boundedClass.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException(
                    "BoundedOperative class `" + boundedClass.getName() + "` must have a public no-arg constructor: " + e.toString(),
                    e);
        }

        String factoryId = bounded.factoryId();
        if (factoryId == null || factoryId.isEmpty()) {
            throw new IllegalArgumentException(
                    "BoundedOperative class `" + boundedClass.getName() + "` returned null/empty from factoryId()");
        }

        if (ContinuousOperativeClassRegistry.getTopology(factoryId) != null) {
            throw new IllegalArgumentException("Duplicate factory id `" + factoryId + "` for BoundedOperative class `"
                    + boundedClass.getName() + "` (already registered as a ContinuousOperative)");
        }

        Class<? extends BoundedOperative> existing = ENTRIES.putIfAbsent(factoryId, boundedClass);
        if (existing != null) {
            throw new IllegalArgumentException("Duplicate factory id `" + factoryId + "` for BoundedOperative class `"
                    + boundedClass.getName() + "` (already registered as `" + existing.getName() + "`)");
        }
        return factoryId;
    }

    public static Class<? extends BoundedOperative> getBoundedOperativeClass(@Nonnull String factoryId) {
        return ENTRIES.get(factoryId);
    }

    @Nonnull
    public static List<String> factoryIds() {
        return List.copyOf(ENTRIES.keySet());
    }

    public static boolean isEmpty() {
        return ENTRIES.isEmpty();
    }

    /** Test-only — invoked by {@code BoundedOperativeRegistry.reset()}. */
    public static void reset() {
        ENTRIES.clear();
    }
}
