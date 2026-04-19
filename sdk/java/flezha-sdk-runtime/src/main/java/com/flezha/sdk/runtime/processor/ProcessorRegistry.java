package com.flezha.sdk.runtime.processor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;

/**
 * Concurrent registry of live processors keyed by processor id. Replaces
 * the Rust SDK's {@code Arc<RwLock<HashMap<String, Arc<Mutex<ProcessorBox>>>>>}
 * with a {@link ConcurrentHashMap}.
 */
public final class ProcessorRegistry {

    private final Map<String, ProcessorInstance<?, ?, ?>> byId = new ConcurrentHashMap<>();

    public void add(@Nonnull ProcessorInstance<?, ?, ?> instance) {
        byId.put(instance.processorId(), instance);
    }

    public ProcessorInstance<?, ?, ?> get(@Nonnull String processorId) {
        return byId.get(processorId);
    }

    public ProcessorInstance<?, ?, ?> remove(@Nonnull String processorId) {
        return byId.remove(processorId);
    }

    public boolean contains(@Nonnull String processorId) {
        return byId.containsKey(processorId);
    }

    @Nonnull
    public Map<String, ProcessorInstance<?, ?, ?>> snapshot() {
        return Map.copyOf(byId);
    }

    public void clear() {
        byId.clear();
    }

    public int size() {
        return byId.size();
    }
}
