package com.flezha.sdk.relay;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nonnull;

/**
 * Process-global registry of registered Relay classes. One entry per Relay
 * class, keyed by {@code factoryId()}. The registry is frozen at
 * {@code Flezha.newInstance(...)} time.
 *
 * <p>Acts as the public-API view of the runtime's {@code RelayClassRegistry}
 * (which holds the frozen {@code RelayTopology} produced by inspecting each
 * registered class). The runtime side is reached reflectively at
 * registration time so {@code flezha-sdk-api} compiles standalone.
 */
public final class RelayRegistry {

    private static final Object LOCK = new Object();
    private static final Map<String, Class<? extends Relay>> ENTRIES = new LinkedHashMap<>();
    private static final AtomicBoolean FROZEN = new AtomicBoolean(false);

    private RelayRegistry() {
    }

    /**
     * Register a Relay class. Must be called before {@code Flezha.newInstance}.
     *
     * @throws IllegalStateException if the registry is already frozen.
     * @throws IllegalArgumentException on duplicate factory id.
     * @throws RelayConfigurationException if the class's topology declaration
     *     fails the wiring constraints (entry-channel-type, type-equality,
     *     unwired-channel, multi-producer, default-impl detection).
     */
    public static void registerRelay(@Nonnull Class<? extends Relay> relayClass) {
        if (relayClass == null) {
            throw new IllegalArgumentException("relayClass must not be null");
        }

        synchronized (LOCK) {
            if (FROZEN.get()) {
                throw new IllegalStateException(
                        "Cannot register relay '" + relayClass.getName()
                                + "' — the SDK is already initialized");
            }
            // Delegate to the runtime side (which validates the topology and
            // returns the agent-facing factory id) before we record it here.
            String factoryId = registerInRuntime(relayClass);
            if (ENTRIES.containsKey(factoryId)) {
                throw new IllegalArgumentException(
                        "Duplicate factory id: '" + factoryId + "'");
            }
            ENTRIES.put(factoryId, relayClass);
        }
    }

    /** Freeze the registry. Called by the SDK runtime when {@code newInstance} begins. */
    public static void freeze() {
        FROZEN.set(true);
    }

    /** Thaw the registry. Intended for tests that reset SDK state between runs. */
    public static void reset() {
        synchronized (LOCK) {
            ENTRIES.clear();
            FROZEN.set(false);
            resetRuntime();
        }
    }

    @Nonnull
    public static List<String> factoryIds() {
        synchronized (LOCK) {
            return List.copyOf(ENTRIES.keySet());
        }
    }

    public static boolean isEmpty() {
        synchronized (LOCK) {
            return ENTRIES.isEmpty();
        }
    }

    public static boolean contains(@Nonnull String factoryId) {
        synchronized (LOCK) {
            return ENTRIES.containsKey(factoryId);
        }
    }

    /**
     * Reflectively call into the runtime's RelayClassRegistry. The runtime
     * lives in {@code flezha-sdk-runtime} so {@code flezha-sdk-api} can
     * compile standalone (mirror of how {@code FlezhaImpl} is loaded).
     */
    private static String registerInRuntime(Class<? extends Relay> relayClass) {
        try {
            Class<?> impl = Class.forName("com.flezha.sdk.runtime.relay.RelayClassRegistry");
            return (String) impl.getMethod("registerClass", Class.class).invoke(null, relayClass);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(
                    "com.flezha.sdk.runtime.relay.RelayClassRegistry not on the classpath. "
                            + "Make sure flezha-sdk-runtime is on the runtime classpath.", e);
        } catch (ReflectiveOperationException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            throw new IllegalStateException(
                    "Failed to register relay '" + relayClass.getName() + "': " + cause.getMessage(),
                    cause);
        }
    }

    private static void resetRuntime() {
        try {
            Class<?> impl = Class.forName("com.flezha.sdk.runtime.relay.RelayClassRegistry");
            impl.getMethod("reset").invoke(null);
        } catch (ClassNotFoundException ignored) {
            // Runtime not loaded — nothing to reset.
        } catch (ReflectiveOperationException e) {
            // Best-effort; tests will surface the failure another way.
        }
    }
}
