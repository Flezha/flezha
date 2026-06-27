package com.flezha.sdk.runtime.relay;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;

/**
 * Per-{@code ExecutorContext} registry of live {@link Unit} instances keyed
 * by {@code instanceId} (= what the wire calls {@code relay_id}). One sealed
 * keyspace across compute-unit kinds — a relay instance and a task instance
 * never share an id, and {@code RemRelay} teardown is uniform across kinds.
 */
public final class RelayInstanceRegistry {

    private final Map<String, Unit> byId = new ConcurrentHashMap<>();

    public void add(@Nonnull Unit instance) {
        byId.put(instance.instanceId(), instance);
    }

    public Unit get(@Nonnull String instanceId) {
        return byId.get(instanceId);
    }

    /**
     * The relay variant for {@code instanceId}, or {@code null} when the id is
     * unknown or bound to a non-relay unit (e.g. a task). Relay-only callers
     * (state queries, migration) use this so a task id reads as "not a relay".
     */
    public RelayInstance getRelay(@Nonnull String instanceId) {
        Unit u = byId.get(instanceId);
        return u instanceof RelayInstance ri ? ri : null;
    }

    public Unit remove(@Nonnull String instanceId) {
        return byId.remove(instanceId);
    }

    public boolean contains(@Nonnull String instanceId) {
        return byId.containsKey(instanceId);
    }

    @Nonnull
    public Map<String, Unit> snapshot() {
        return Map.copyOf(byId);
    }

    /** Snapshot of the relay variants only — for state/shutdown paths that have no task analog. */
    @Nonnull
    public Map<String, RelayInstance> relaySnapshot() {
        Map<String, RelayInstance> out = new LinkedHashMap<>();
        for (Map.Entry<String, Unit> e : byId.entrySet()) {
            if (e.getValue() instanceof RelayInstance ri) {
                out.put(e.getKey(), ri);
            }
        }
        return Map.copyOf(out);
    }

    public void clear() {
        byId.clear();
    }

    public int size() {
        return byId.size();
    }
}
