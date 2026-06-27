package com.flezha.sdk.task;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nonnull;

/**
 * Process-global registry of registered {@link Task} classes, keyed by
 * {@code factoryId()}. The public-API sibling of
 * {@link com.flezha.sdk.relay.RelayRegistry}; the runtime side
 * ({@code com.flezha.sdk.runtime.task.TaskClassRegistry}) is reached
 * reflectively at registration time so {@code flezha-sdk-api} compiles
 * standalone. Frozen at {@code Flezha.newInstance(...)} time.
 */
public final class TaskRegistry {

    private static final Object LOCK = new Object();
    private static final Map<String, Class<? extends Task>> ENTRIES = new LinkedHashMap<>();
    private static final AtomicBoolean FROZEN = new AtomicBoolean(false);

    private TaskRegistry() {}

    /**
     * Register a Task class. Must be called before {@code Flezha.newInstance}.
     *
     * @throws IllegalStateException if the registry is already frozen.
     * @throws IllegalArgumentException on a factory id that collides with a
     *     relay or another task.
     */
    public static void registerTask(@Nonnull Class<? extends Task> taskClass) {
        if (taskClass == null) {
            throw new IllegalArgumentException("taskClass must not be null");
        }
        synchronized (LOCK) {
            if (FROZEN.get()) {
                throw new IllegalStateException(
                        "Cannot register task '" + taskClass.getName() + "' — the SDK is already initialized");
            }
            // The runtime side validates and records (and rejects a factory id
            // colliding with a registered relay) before we record it here.
            String factoryId = registerInRuntime(taskClass);
            if (ENTRIES.containsKey(factoryId)) {
                throw new IllegalArgumentException("Duplicate factory id: '" + factoryId + "'");
            }
            ENTRIES.put(factoryId, taskClass);
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

    private static String registerInRuntime(Class<? extends Task> taskClass) {
        try {
            Class<?> impl = Class.forName("com.flezha.sdk.runtime.task.TaskClassRegistry");
            return (String) impl.getMethod("registerClass", Class.class).invoke(null, taskClass);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(
                    "com.flezha.sdk.runtime.task.TaskClassRegistry not on the classpath. "
                            + "Make sure flezha-sdk-runtime is on the runtime classpath.",
                    e);
        } catch (ReflectiveOperationException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            throw new IllegalStateException(
                    "Failed to register task '" + taskClass.getName() + "': " + cause.getMessage(), cause);
        }
    }

    private static void resetRuntime() {
        try {
            Class<?> impl = Class.forName("com.flezha.sdk.runtime.task.TaskClassRegistry");
            impl.getMethod("reset").invoke(null);
        } catch (ClassNotFoundException ignored) {
            // Runtime not loaded — nothing to reset.
        } catch (ReflectiveOperationException e) {
            // Best-effort; tests will surface the failure another way.
        }
    }
}
