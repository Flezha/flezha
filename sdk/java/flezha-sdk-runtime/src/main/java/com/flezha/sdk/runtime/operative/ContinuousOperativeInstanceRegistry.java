package com.flezha.sdk.runtime.operative;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;

/**
 * Per-{@code ExecutorContext} registry of live {@link Operative} instances keyed
 * by {@code instanceId} (= what the wire calls {@code operative_id}). One sealed
 * keyspace across compute-operative kinds — a operative instance and a bounded instance
 * never share an id, and {@code RemOperative} teardown is uniform across kinds.
 */
public final class ContinuousOperativeInstanceRegistry {

    private final Map<String, Operative> byId = new ConcurrentHashMap<>();

    public void add(@Nonnull Operative instance) {
        byId.put(instance.instanceId(), instance);
    }

    public Operative get(@Nonnull String instanceId) {
        return byId.get(instanceId);
    }

    /**
     * The operative variant for {@code instanceId}, or {@code null} when the id is
     * unknown or bound to a non-operative operative (e.g. a bounded). ContinuousOperative-only callers
     * (state queries, migration) use this so a bounded id reads as "not a operative".
     */
    public ContinuousOperativeInstance getContinuousOperative(@Nonnull String instanceId) {
        Operative u = byId.get(instanceId);
        return u instanceof ContinuousOperativeInstance ri ? ri : null;
    }

    public Operative remove(@Nonnull String instanceId) {
        return byId.remove(instanceId);
    }

    public boolean contains(@Nonnull String instanceId) {
        return byId.containsKey(instanceId);
    }

    @Nonnull
    public Map<String, Operative> snapshot() {
        return Map.copyOf(byId);
    }

    /** Snapshot of the operative variants only — for state/shutdown paths that have no bounded analog. */
    @Nonnull
    public Map<String, ContinuousOperativeInstance> operativeSnapshot() {
        Map<String, ContinuousOperativeInstance> out = new LinkedHashMap<>();
        for (Map.Entry<String, Operative> e : byId.entrySet()) {
            if (e.getValue() instanceof ContinuousOperativeInstance ri) {
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
