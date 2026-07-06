package com.flezha.sdk.runtime.operative;

import com.flezha.sdk.operative.UnboundedOperative;
import com.flezha.sdk.operative.UnboundedOperativeConfigurationException;
import com.flezha.sdk.runtime.operative.builder.UnboundedOperativeTopologyBuilder;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;

/**
 * Process-global registry of registered UnboundedOperative classes keyed by
 * {@code factoryId}. Holds the immutable {@link UnboundedOperativeTopology} produced by
 * {@link UnboundedOperativeTopologyBuilder} at registration time, plus the
 * {@code Class<? extends UnboundedOperative>} for diagnostic logging.
 *
 * <p>Reached reflectively from
 * {@link com.flezha.sdk.operative.UnboundedOperativeRegistry#registerUnbounded(Class)} so that
 * {@code flezha-sdk-api} compiles standalone.
 */
public final class UnboundedOperativeClassRegistry {

    private static final Map<String, Entry> ENTRIES = new ConcurrentHashMap<>();

    private UnboundedOperativeClassRegistry() {}

    /**
     * Reflective entry point invoked from
     * {@link com.flezha.sdk.operative.UnboundedOperativeRegistry}. Instantiates the UnboundedOperative
     * class via its no-arg ctor, builds the topology, and records
     * {@code (factoryId → topology + class)}. Returns the controller-facing
     * factory id so the caller can de-duplicate on its side.
     *
     * @throws UnboundedOperativeConfigurationException if topology validation fails.
     * @throws IllegalArgumentException if the UnboundedOperative class lacks a public
     *     no-arg constructor.
     */
    @Nonnull
    public static String registerClass(@Nonnull Class<? extends UnboundedOperative> operativeClass) {
        UnboundedOperative operative;
        try {
            operative = operativeClass.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException(
                    "UnboundedOperative class `" + operativeClass.getName()
                            + "` must have a public no-arg constructor: " + e.toString(),
                    e);
        }

        String factoryId = operative.factoryId();
        if (factoryId == null || factoryId.isEmpty()) {
            throw new IllegalArgumentException(
                    "UnboundedOperative class `" + operativeClass.getName() + "` returned null/empty from factoryId()");
        }

        if (com.flezha.sdk.runtime.bounded.BoundedOperativeClassRegistry.getSpec(factoryId) != null) {
            throw new IllegalArgumentException("Duplicate factory id `" + factoryId + "` for UnboundedOperative class `"
                    + operativeClass.getName() + "` (already registered as a BoundedOperative)");
        }

        UnboundedOperativeTopology topology = UnboundedOperativeTopologyBuilder.build(operative);

        Entry existing = ENTRIES.putIfAbsent(factoryId, new Entry(operativeClass, topology));
        if (existing != null) {
            throw new IllegalArgumentException("Duplicate factory id `" + factoryId + "` for UnboundedOperative class `"
                    + operativeClass.getName() + "` (already registered as `"
                    + existing.operativeClass().getName() + "`)");
        }
        return factoryId;
    }

    public static UnboundedOperativeTopology getTopology(@Nonnull String factoryId) {
        Entry e = ENTRIES.get(factoryId);
        return e == null ? null : e.topology();
    }

    public static Class<? extends UnboundedOperative> getUnboundedOperativeClass(@Nonnull String factoryId) {
        Entry e = ENTRIES.get(factoryId);
        return e == null ? null : e.operativeClass();
    }

    @Nonnull
    public static List<String> factoryIds() {
        return List.copyOf(ENTRIES.keySet());
    }

    @Nonnull
    public static Map<String, UnboundedOperativeTopology> snapshot() {
        Map<String, UnboundedOperativeTopology> out = new LinkedHashMap<>();
        for (Map.Entry<String, Entry> e : ENTRIES.entrySet()) {
            out.put(e.getKey(), e.getValue().topology());
        }
        return Map.copyOf(out);
    }

    public static boolean isEmpty() {
        return ENTRIES.isEmpty();
    }

    /** Test-only — invoked by {@code UnboundedOperativeRegistry.reset()}. */
    public static void reset() {
        ENTRIES.clear();
    }

    private record Entry(Class<? extends UnboundedOperative> operativeClass, UnboundedOperativeTopology topology) {}
}
