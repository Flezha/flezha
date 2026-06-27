package com.flezha.sdk.runtime.task;

import com.flezha.sdk.runtime.relay.RelayClassRegistry;
import com.flezha.sdk.task.Task;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;

/**
 * Process-global registry of registered {@link Task} classes keyed by
 * {@code factoryId}. The runtime sibling of {@link RelayClassRegistry};
 * reached reflectively from {@link com.flezha.sdk.task.TaskRegistry} so
 * {@code flezha-sdk-api} compiles standalone. Looked up when an
 * {@code AddRelay} whose kind resolves to {@code :task} arrives.
 */
public final class TaskClassRegistry {

    private static final Map<String, Class<? extends Task>> ENTRIES = new ConcurrentHashMap<>();

    private TaskClassRegistry() {}

    /**
     * Reflective entry point invoked from {@link com.flezha.sdk.task.TaskRegistry}.
     * Instantiates the Task class via its no-arg ctor to read a stable
     * {@code factoryId()}, then records {@code (factoryId → class)}. Rejects a
     * factory id that collides with a registered relay or another task.
     * Returns the controller-facing factory id.
     *
     * @throws IllegalArgumentException if the Task class lacks a public no-arg
     *     constructor, returns a null/empty factory id, or collides with an
     *     existing relay/task.
     */
    @Nonnull
    public static String registerClass(@Nonnull Class<? extends Task> taskClass) {
        Task task;
        try {
            task = taskClass.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException(
                    "Task class `" + taskClass.getName() + "` must have a public no-arg constructor: " + e.toString(),
                    e);
        }

        String factoryId = task.factoryId();
        if (factoryId == null || factoryId.isEmpty()) {
            throw new IllegalArgumentException(
                    "Task class `" + taskClass.getName() + "` returned null/empty from factoryId()");
        }

        if (RelayClassRegistry.getTopology(factoryId) != null) {
            throw new IllegalArgumentException("Duplicate factory id `" + factoryId + "` for Task class `"
                    + taskClass.getName() + "` (already registered as a Relay)");
        }

        Class<? extends Task> existing = ENTRIES.putIfAbsent(factoryId, taskClass);
        if (existing != null) {
            throw new IllegalArgumentException("Duplicate factory id `" + factoryId + "` for Task class `"
                    + taskClass.getName() + "` (already registered as `" + existing.getName() + "`)");
        }
        return factoryId;
    }

    public static Class<? extends Task> getTaskClass(@Nonnull String factoryId) {
        return ENTRIES.get(factoryId);
    }

    @Nonnull
    public static List<String> factoryIds() {
        return List.copyOf(ENTRIES.keySet());
    }

    public static boolean isEmpty() {
        return ENTRIES.isEmpty();
    }

    /** Test-only — invoked by {@code TaskRegistry.reset()}. */
    public static void reset() {
        ENTRIES.clear();
    }
}
