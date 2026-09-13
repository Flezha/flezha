package com.flezha.sdk.internal;

import com.flezha.sdk.operative.Operative;
import java.lang.reflect.InvocationTargetException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * The ONE implementation behind a {@code FlezhaRunner}'s two declared factory
 * sets (stream + workflow). Instance-scoped: each runner carries its own
 * cores, so a later runner starts with an empty declared set (generation 2
 * must re-define its types); the runtime-side class registries reached via
 * {@link RuntimeBridge} stay process-global and overwrite on insert.
 *
 * <p><b>Internal.</b> Not part of the SDK's public API; user code never calls
 * this.
 *
 * @param <C> the authoring interface the registry holds classes of
 */
public final class RegistryCore<C extends Operative> {

    private final Object lock = new Object();
    private final Map<String, Class<? extends C>> entries = new LinkedHashMap<>();
    private final String runtimeRegistryFqn;
    private final String kindLabel;

    /**
     * @param runtimeRegistryFqn the runtime-side class registry reached via
     *     {@link RuntimeBridge} ({@code registerClass(Class)} /
     *     {@code unregisterClass(String)} / {@code reset()})
     * @param kindLabel {@code "operative"} / {@code "workflow"} — used in error
     *     messages
     */
    public RegistryCore(String runtimeRegistryFqn, String kindLabel) {
        this.runtimeRegistryFqn = runtimeRegistryFqn;
        this.kindLabel = kindLabel;
    }

    /**
     * Register a class: validate via the runtime side (which returns the
     * controller-facing factory id), then record it here.
     *
     * @return the controller-facing factory id.
     * @throws IllegalArgumentException on a null class or duplicate factory id.
     */
    public String register(Class<? extends C> cls) {
        if (cls == null) {
            throw new IllegalArgumentException(kindLabel + "Class must not be null");
        }
        synchronized (lock) {
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
            return factoryId;
        }
    }

    /**
     * The factory id under which {@code cls} is defined in this core.
     *
     * @throws IllegalArgumentException naming the class's factory id when it
     *     is not defined here (never defined, or already removed).
     */
    @Nonnull
    public String definedFactoryIdOf(Class<? extends C> cls) {
        synchronized (lock) {
            for (Map.Entry<String, Class<? extends C>> e : entries.entrySet()) {
                if (e.getValue().equals(cls)) {
                    return e.getKey();
                }
            }
        }
        throw notDefined(cls);
    }

    /**
     * Remove {@code factoryId}: unregister the class from the runtime-side
     * registry and drop the entry here. Pre-start path — no live instance can
     * exist, so no in-use check is needed.
     */
    public void remove(String factoryId) {
        synchronized (lock) {
            RuntimeBridge.invoke(
                    runtimeRegistryFqn,
                    "unregisterClass",
                    new Class<?>[] {String.class},
                    "Failed to remove " + kindLabel + " factory '" + factoryId + "'",
                    factoryId);
            dropEntry(factoryId);
        }
    }

    /**
     * Drop {@code factoryId}'s entry WITHOUT touching the runtime registry —
     * the post-start path, where the runtime's {@code removeType} already
     * unregistered it under the race-free live-instance check.
     */
    public void dropEntry(String factoryId) {
        synchronized (lock) {
            entries.remove(factoryId);
        }
    }

    /** Thaw and clear the registry (and its runtime side). For tests. */
    public void reset() {
        synchronized (lock) {
            entries.clear();
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

    public boolean contains(String factoryId) {
        synchronized (lock) {
            return entries.containsKey(factoryId);
        }
    }

    /**
     * The refusal for a class not defined here, naming its self-declared
     * factory id, or its class name when its no-arg constructor cannot be
     * called. What that constructor throws is the refusal's cause.
     */
    private IllegalArgumentException notDefined(Class<? extends C> cls) {
        String factoryId = cls.getName();
        Throwable cause = null;
        try {
            factoryId = cls.getDeclaredConstructor().newInstance().factoryId();
        } catch (InvocationTargetException e) {
            cause = e.getCause();
        } catch (ReflectiveOperationException e) {
            // No callable constructor: the class name stands in for the id.
        }
        return new IllegalArgumentException(
                kindLabel + " factory '" + factoryId + "' is not defined on this runner", cause);
    }
}
