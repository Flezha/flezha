package com.flezha.sdk.runtime.operative;

import com.flezha.sdk.operative.ContinuousOperative;
import com.flezha.sdk.operative.ContinuousOperativeConfigurationException;
import com.flezha.sdk.runtime.operative.builder.ContinuousOperativeTopologyBuilder;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;

/**
 * Process-global registry of registered ContinuousOperative classes keyed by
 * {@code factoryId}. Holds the immutable {@link ContinuousOperativeTopology} produced by
 * {@link ContinuousOperativeTopologyBuilder} at registration time, plus the
 * {@code Class<? extends ContinuousOperative>} for diagnostic logging.
 *
 * <p>Reached reflectively from
 * {@link com.flezha.sdk.operative.ContinuousOperativeRegistry#registerContinuous(Class)} so that
 * {@code flezha-sdk-api} compiles standalone.
 */
public final class ContinuousOperativeClassRegistry {

    private static final Map<String, Entry> ENTRIES = new ConcurrentHashMap<>();

    private ContinuousOperativeClassRegistry() {}

    /**
     * Reflective entry point invoked from
     * {@link com.flezha.sdk.operative.ContinuousOperativeRegistry}. Instantiates the ContinuousOperative
     * class via its no-arg ctor, builds the topology, and records
     * {@code (factoryId → topology + class)}. Returns the controller-facing
     * factory id so the caller can de-duplicate on its side.
     *
     * @throws ContinuousOperativeConfigurationException if topology validation fails.
     * @throws IllegalArgumentException if the ContinuousOperative class lacks a public
     *     no-arg constructor.
     */
    @Nonnull
    public static String registerClass(@Nonnull Class<? extends ContinuousOperative> operativeClass) {
        ContinuousOperative operative;
        try {
            operative = operativeClass.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException(
                    "ContinuousOperative class `" + operativeClass.getName() + "` must have a public no-arg constructor: " + e.toString(),
                    e);
        }

        String factoryId = operative.factoryId();
        if (factoryId == null || factoryId.isEmpty()) {
            throw new IllegalArgumentException(
                    "ContinuousOperative class `" + operativeClass.getName() + "` returned null/empty from factoryId()");
        }

        if (com.flezha.sdk.runtime.bounded.BoundedOperativeClassRegistry.getBoundedOperativeClass(factoryId) != null) {
            throw new IllegalArgumentException("Duplicate factory id `" + factoryId + "` for ContinuousOperative class `"
                    + operativeClass.getName() + "` (already registered as a BoundedOperative)");
        }

        ContinuousOperativeTopology topology = ContinuousOperativeTopologyBuilder.build(operative);

        Entry existing = ENTRIES.putIfAbsent(factoryId, new Entry(operativeClass, topology));
        if (existing != null) {
            throw new IllegalArgumentException("Duplicate factory id `" + factoryId + "` for ContinuousOperative class `"
                    + operativeClass.getName() + "` (already registered as `"
                    + existing.operativeClass().getName() + "`)");
        }
        return factoryId;
    }

    public static ContinuousOperativeTopology getTopology(@Nonnull String factoryId) {
        Entry e = ENTRIES.get(factoryId);
        return e == null ? null : e.topology();
    }

    public static Class<? extends ContinuousOperative> getContinuousOperativeClass(@Nonnull String factoryId) {
        Entry e = ENTRIES.get(factoryId);
        return e == null ? null : e.operativeClass();
    }

    @Nonnull
    public static List<String> factoryIds() {
        return List.copyOf(ENTRIES.keySet());
    }

    @Nonnull
    public static Map<String, ContinuousOperativeTopology> snapshot() {
        Map<String, ContinuousOperativeTopology> out = new LinkedHashMap<>();
        for (Map.Entry<String, Entry> e : ENTRIES.entrySet()) {
            out.put(e.getKey(), e.getValue().topology());
        }
        return Map.copyOf(out);
    }

    public static boolean isEmpty() {
        return ENTRIES.isEmpty();
    }

    /** Test-only — invoked by {@code ContinuousOperativeRegistry.reset()}. */
    public static void reset() {
        ENTRIES.clear();
    }

    private record Entry(Class<? extends ContinuousOperative> operativeClass, ContinuousOperativeTopology topology) {}
}
