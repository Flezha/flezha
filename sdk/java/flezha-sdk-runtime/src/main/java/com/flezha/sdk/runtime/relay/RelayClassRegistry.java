package com.flezha.sdk.runtime.relay;

import com.flezha.sdk.relay.Relay;
import com.flezha.sdk.relay.RelayConfigurationException;
import com.flezha.sdk.runtime.relay.builder.RelayTopologyBuilder;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;

/**
 * Process-global registry of registered Relay classes keyed by
 * {@code factoryId}. Holds the immutable {@link RelayTopology} produced by
 * {@link RelayTopologyBuilder} at registration time, plus the
 * {@code Class<? extends Relay>} for diagnostic logging.
 *
 * <p>Reached reflectively from
 * {@link com.flezha.sdk.relay.RelayRegistry#registerRelay(Class)} so that
 * {@code flezha-sdk-api} compiles standalone.
 */
public final class RelayClassRegistry {

    private static final Map<String, Entry> ENTRIES = new ConcurrentHashMap<>();

    private RelayClassRegistry() {}

    /**
     * Reflective entry point invoked from
     * {@link com.flezha.sdk.relay.RelayRegistry}. Instantiates the Relay
     * class via its no-arg ctor, builds the topology, and records
     * {@code (factoryId → topology + class)}. Returns the controller-facing
     * factory id so the caller can de-duplicate on its side.
     *
     * @throws RelayConfigurationException if topology validation fails.
     * @throws IllegalArgumentException if the Relay class lacks a public
     *     no-arg constructor.
     */
    @Nonnull
    public static String registerClass(@Nonnull Class<? extends Relay> relayClass) {
        Relay relay;
        try {
            relay = relayClass.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException(
                    "Relay class `" + relayClass.getName() + "` must have a public no-arg constructor: " + e.toString(),
                    e);
        }

        String factoryId = relay.factoryId();
        if (factoryId == null || factoryId.isEmpty()) {
            throw new IllegalArgumentException(
                    "Relay class `" + relayClass.getName() + "` returned null/empty from factoryId()");
        }

        RelayTopology topology = RelayTopologyBuilder.build(relay);

        Entry existing = ENTRIES.putIfAbsent(factoryId, new Entry(relayClass, topology));
        if (existing != null) {
            throw new IllegalArgumentException("Duplicate factory id `" + factoryId + "` for Relay class `"
                    + relayClass.getName() + "` (already registered as `"
                    + existing.relayClass().getName() + "`)");
        }
        return factoryId;
    }

    public static RelayTopology getTopology(@Nonnull String factoryId) {
        Entry e = ENTRIES.get(factoryId);
        return e == null ? null : e.topology();
    }

    public static Class<? extends Relay> getRelayClass(@Nonnull String factoryId) {
        Entry e = ENTRIES.get(factoryId);
        return e == null ? null : e.relayClass();
    }

    @Nonnull
    public static List<String> factoryIds() {
        return List.copyOf(ENTRIES.keySet());
    }

    @Nonnull
    public static Map<String, RelayTopology> snapshot() {
        Map<String, RelayTopology> out = new LinkedHashMap<>();
        for (Map.Entry<String, Entry> e : ENTRIES.entrySet()) {
            out.put(e.getKey(), e.getValue().topology());
        }
        return Map.copyOf(out);
    }

    public static boolean isEmpty() {
        return ENTRIES.isEmpty();
    }

    /** Test-only — invoked by {@code RelayRegistry.reset()}. */
    public static void reset() {
        ENTRIES.clear();
    }

    private record Entry(Class<? extends Relay> relayClass, RelayTopology topology) {}
}
