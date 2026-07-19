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
 * {@link UnboundedOperativeClassRegistry}; reached reflectively from the api
 * module's {@code RegistryCore} so {@code flezha-sdk-api}
 * compiles standalone. Overwrite-on-insert — the per-runner declared set owns
 * duplicate detection. Looked up when an {@code AddOperative} whose kind
 * resolves to {@code :bounded} arrives.
 */
public final class BoundedOperativeClassRegistry {

    private static final Map<String, BoundedSpec> ENTRIES = new ConcurrentHashMap<>();

    private BoundedOperativeClassRegistry() {}

    /**
     * Reflective entry point invoked from the api module's {@code RegistryCore}.
     * Builds and validates the class's {@link BoundedSpec} (which reads a stable
     * {@code factoryId()} and validates the authoring), then records
     * {@code (factoryId → spec)} — overwriting any prior registration of the
     * id. Rejects a factory id that collides with a registered unbounded
     * operative. Returns the controller-facing factory id.
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

        ENTRIES.put(factoryId, spec);
        return factoryId;
    }

    /**
     * On-demand removal ({@code FlezhaRunner.removeBounded}). Returns the
     * removed spec (for {@code FlezhaImpl.removeType}'s re-register-on-live-hit
     * path), or {@code null} when the id is unknown. Reached reflectively from
     * the api module's {@code RegistryCore} on the pre-start path.
     */
    public static BoundedSpec unregisterClass(@Nonnull String factoryId) {
        return ENTRIES.remove(factoryId);
    }

    /** Restore a spec {@code unregisterClass} removed (live-instance refusal). */
    public static void restore(@Nonnull String factoryId, @Nonnull BoundedSpec spec) {
        ENTRIES.put(factoryId, spec);
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

    /** Test-only — invoked by the api module's {@code RegistryCore.reset()}. */
    public static void reset() {
        ENTRIES.clear();
    }
}
