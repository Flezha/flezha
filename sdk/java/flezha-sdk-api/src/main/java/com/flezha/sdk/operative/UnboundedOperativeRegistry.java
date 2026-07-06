package com.flezha.sdk.operative;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nonnull;

/**
 * Process-global registry of registered UnboundedOperative classes. One entry per UnboundedOperative
 * class, keyed by {@code factoryId()}. The registry is frozen at
 * {@code Flezha.newInstance(...)} time.
 *
 * <p>Acts as the public-API view of the runtime's {@code UnboundedOperativeClassRegistry}
 * (which holds the frozen {@code UnboundedOperativeTopology} produced by inspecting each
 * registered class). The runtime side is reached reflectively at
 * registration time so {@code flezha-sdk-api} compiles standalone.
 */
public final class UnboundedOperativeRegistry {

    private static final Object LOCK = new Object();
    private static final Map<String, Class<? extends UnboundedOperative>> ENTRIES = new LinkedHashMap<>();
    private static final AtomicBoolean FROZEN = new AtomicBoolean(false);

    private UnboundedOperativeRegistry() {}

    /**
     * Register an UnboundedOperative class. Must be called before {@code Flezha.newInstance}.
     *
     * @throws IllegalStateException if the registry is already frozen.
     * @throws IllegalArgumentException on duplicate factory id.
     * @throws UnboundedOperativeConfigurationException if the class's topology declaration
     *     fails the wiring constraints (entry-channel-type, type-equality,
     *     unwired-channel, multi-producer, default-impl detection).
     */
    public static void registerUnbounded(@Nonnull Class<? extends UnboundedOperative> operativeClass) {
        if (operativeClass == null) {
            throw new IllegalArgumentException("operativeClass must not be null");
        }

        synchronized (LOCK) {
            if (FROZEN.get()) {
                throw new IllegalStateException("Cannot register operative '" + operativeClass.getName()
                        + "' — the SDK is already initialized");
            }
            // Delegate to the runtime side (which validates the topology and
            // returns the controller-facing factory id) before we record it here.
            String factoryId = registerInRuntime(operativeClass);
            if (ENTRIES.containsKey(factoryId)) {
                throw new IllegalArgumentException("Duplicate factory id: '" + factoryId + "'");
            }
            ENTRIES.put(factoryId, operativeClass);
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
     * Reflectively call into the runtime's UnboundedOperativeClassRegistry. The runtime
     * lives in {@code flezha-sdk-runtime} so {@code flezha-sdk-api} can
     * compile standalone (mirror of how {@code FlezhaImpl} is loaded).
     */
    private static String registerInRuntime(Class<? extends UnboundedOperative> operativeClass) {
        try {
            Class<?> impl = Class.forName("com.flezha.sdk.runtime.operative.UnboundedOperativeClassRegistry");
            return (String) impl.getMethod("registerClass", Class.class).invoke(null, operativeClass);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(
                    "com.flezha.sdk.runtime.operative.UnboundedOperativeClassRegistry not on the classpath. "
                            + "Make sure flezha-sdk-runtime is on the runtime classpath.",
                    e);
        } catch (ReflectiveOperationException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            throw new IllegalStateException(
                    "Failed to register operative '" + operativeClass.getName() + "': " + cause.getMessage(), cause);
        }
    }

    private static void resetRuntime() {
        try {
            Class<?> impl = Class.forName("com.flezha.sdk.runtime.operative.UnboundedOperativeClassRegistry");
            impl.getMethod("reset").invoke(null);
        } catch (ClassNotFoundException ignored) {
            // Runtime not loaded — nothing to reset.
        } catch (ReflectiveOperationException e) {
            // Best-effort; tests will surface the failure another way.
        }
    }
}
