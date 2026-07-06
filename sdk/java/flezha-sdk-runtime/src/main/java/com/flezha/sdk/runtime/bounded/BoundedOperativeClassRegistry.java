package com.flezha.sdk.runtime.bounded;

import com.flezha.sdk.bounded.BoundedOperative;
import com.flezha.sdk.runtime.operative.UnboundedOperativeClassRegistry;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;

/**
 * Process-global registry of registered {@link BoundedOperative} classes keyed by
 * {@code factoryId}, each carrying its frozen {@link BoundedSpec} (the run-sugar
 * body or the validated structured linear chain). The runtime sibling of
 * {@link UnboundedOperativeClassRegistry}; reached reflectively from
 * {@link com.flezha.sdk.bounded.BoundedOperativeRegistry} so {@code flezha-sdk-api}
 * compiles standalone. Looked up when an {@code AddOperative} whose kind
 * resolves to {@code :bounded} arrives.
 */
public final class BoundedOperativeClassRegistry {

    private static final Map<String, BoundedSpec> ENTRIES = new ConcurrentHashMap<>();

    private BoundedOperativeClassRegistry() {}

    /**
     * Reflective entry point invoked from {@link com.flezha.sdk.bounded.BoundedOperativeRegistry}.
     * Builds and validates the class's {@link BoundedSpec} (which reads a stable
     * {@code factoryId()} and validates the authoring), then records
     * {@code (factoryId → spec)}. Rejects a factory id that collides with a
     * registered operative or another bounded. Returns the controller-facing
     * factory id.
     *
     * @throws IllegalArgumentException if the BoundedOperative class lacks a public
     *     no-arg constructor, returns a null/empty factory id, or collides with
     *     an existing operative/bounded.
     * @throws com.flezha.sdk.bounded.BoundedOperativeConfigurationException if the
     *     authoring is invalid (mirrors the Rust SDK's {@code register_bounded}
     *     panic on an invalid bounded operative).
     */
    @Nonnull
    public static String registerClass(@Nonnull Class<? extends BoundedOperative> boundedClass) {
        BoundedSpec spec = BoundedSpec.build(boundedClass);
        String factoryId = spec.factoryId();

        if (UnboundedOperativeClassRegistry.getTopology(factoryId) != null) {
            throw new IllegalArgumentException("Duplicate factory id `" + factoryId + "` for BoundedOperative class `"
                    + boundedClass.getName() + "` (already registered as an UnboundedOperative)");
        }

        BoundedSpec existing = ENTRIES.putIfAbsent(factoryId, spec);
        if (existing != null) {
            throw new IllegalArgumentException("Duplicate factory id `" + factoryId + "` for BoundedOperative class `"
                    + boundedClass.getName() + "` (already registered as `"
                    + existing.operativeClass().getName() + "`)");
        }
        return factoryId;
    }

    /** The frozen spec for a registered bounded factory, or {@code null} if unknown. */
    public static BoundedSpec getSpec(@Nonnull String factoryId) {
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
