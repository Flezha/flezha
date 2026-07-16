package com.flezha.sdk.internal;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nonnull;

/**
 * The ONE implementation behind the two process-global class registries
 * ({@code UnboundedOperativeRegistry}, {@code BoundedOperativeRegistry}),
 * which stay as thin static facades for the published API. Owns the
 * lock/freeze/unfreeze/reset semantics and the delegation into the runtime's
 * class registry via {@link RuntimeBridge}.
 *
 * <p><b>Internal.</b> Not part of the SDK's public API; user code never calls
 * this.
 *
 * @param <C> the authoring interface the registry holds classes of
 */
public final class RegistryCore<C> {

    private final Object lock = new Object();
    private final Map<String, Class<? extends C>> entries = new LinkedHashMap<>();
    private final AtomicBoolean frozen = new AtomicBoolean(false);
    private final String runtimeRegistryFqn;
    private final String kindLabel;

    /**
     * @param runtimeRegistryFqn the runtime-side class registry reached via
     *     {@link RuntimeBridge} ({@code registerClass(Class)} / {@code reset()})
     * @param kindLabel {@code "operative"} / {@code "bounded"} — used in error
     *     messages
     */
    public RegistryCore(@Nonnull String runtimeRegistryFqn, @Nonnull String kindLabel) {
        this.runtimeRegistryFqn = runtimeRegistryFqn;
        this.kindLabel = kindLabel;
    }

    /**
     * Register a class: validate via the runtime side (which returns the
     * controller-facing factory id), then record it here.
     *
     * @throws IllegalStateException if the registry is already frozen.
     * @throws IllegalArgumentException on a null class or duplicate factory id.
     */
    public void register(Class<? extends C> cls) {
        if (cls == null) {
            throw new IllegalArgumentException(kindLabel + "Class must not be null");
        }
        synchronized (lock) {
            if (frozen.get()) {
                throw new IllegalStateException(
                        "Cannot register " + kindLabel + " '" + cls.getName() + "' — the SDK is already initialized");
            }
            // Delegate to the runtime side (which validates the authoring and
            // returns the controller-facing factory id) before we record it here.
            String factoryId = (String) RuntimeBridge.invoke(
                    runtimeRegistryFqn,
                    "registerClass",
                    new Class<?>[] {Class.class},
                    "Failed to register " + kindLabel + " '" + cls.getName() + "'",
                    cls);
            if (entries.containsKey(factoryId)) {
                throw new IllegalArgumentException("Duplicate factory id: '" + factoryId + "'");
            }
            entries.put(factoryId, cls);
        }
    }

    /** Freeze the registry. Called by {@code RunnerBuilder.start()}. */
    public void freeze() {
        frozen.set(true);
    }

    /**
     * Unfreeze the registry, keeping its entries. Called when the active
     * runner closes, so a later runner in the same JVM may declare new
     * operative types.
     */
    public void unfreeze() {
        frozen.set(false);
    }

    /** Thaw and clear the registry (and its runtime side). For tests. */
    public void reset() {
        synchronized (lock) {
            entries.clear();
            frozen.set(false);
            RuntimeBridge.invokeIfPresent(runtimeRegistryFqn, "reset");
        }
    }

    @Nonnull
    public List<String> factoryIds() {
        synchronized (lock) {
            return List.copyOf(entries.keySet());
        }
    }

    public boolean isEmpty() {
        synchronized (lock) {
            return entries.isEmpty();
        }
    }

    public boolean contains(@Nonnull String factoryId) {
        synchronized (lock) {
            return entries.containsKey(factoryId);
        }
    }
}
