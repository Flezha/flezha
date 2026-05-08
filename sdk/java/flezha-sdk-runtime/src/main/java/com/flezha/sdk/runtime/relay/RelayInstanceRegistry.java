package com.flezha.sdk.runtime.relay;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;

/**
 * Per-{@code ExecutorContext} registry of live {@link RelayInstance}
 * objects keyed by {@code instanceId} (= what the wire calls
 * {@code relay_id}).
 */
public final class RelayInstanceRegistry {

    private final Map<String, RelayInstance> byId = new ConcurrentHashMap<>();

    public void add(@Nonnull RelayInstance instance) {
        byId.put(instance.instanceId(), instance);
    }

    public RelayInstance get(@Nonnull String instanceId) {
        return byId.get(instanceId);
    }

    public RelayInstance remove(@Nonnull String instanceId) {
        return byId.remove(instanceId);
    }

    public boolean contains(@Nonnull String instanceId) {
        return byId.containsKey(instanceId);
    }

    @Nonnull
    public Map<String, RelayInstance> snapshot() {
        return Map.copyOf(byId);
    }

    public void clear() {
        byId.clear();
    }

    public int size() {
        return byId.size();
    }
}
