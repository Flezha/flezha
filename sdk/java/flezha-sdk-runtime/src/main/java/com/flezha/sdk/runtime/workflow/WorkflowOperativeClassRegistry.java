package com.flezha.sdk.runtime.workflow;

import com.flezha.sdk.operative.WorkflowOperative;
import com.flezha.sdk.runtime.operative.StreamOperativeClassRegistry;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;

/**
 * Process-global registry of registered {@link WorkflowOperative} classes keyed by
 * {@code factoryId}, each carrying its frozen {@link WorkflowSpec} (the run-sugar
 * body or the validated structured linear chain). The runtime sibling of
 * {@link StreamOperativeClassRegistry}; reached reflectively from the api
 * module's {@code RegistryCore} so {@code flezha-sdk-api}
 * compiles standalone. Overwrite-on-insert — the per-runner declared set owns
 * duplicate detection. Looked up when an {@code AddOperative} whose kind
 * resolves to {@code :workflow} arrives.
 */
public final class WorkflowOperativeClassRegistry {

    private static final Map<String, WorkflowSpec> ENTRIES = new ConcurrentHashMap<>();

    private WorkflowOperativeClassRegistry() {}

    /**
     * Reflective entry point invoked from the api module's {@code RegistryCore}.
     * Builds and validates the class's {@link WorkflowSpec} (which reads a stable
     * {@code factoryId()} and validates the authoring), then records
     * {@code (factoryId → spec)} — overwriting any prior registration of the
     * id. Rejects a factory id that collides with a registered stream
     * operative. Returns the controller-facing factory id.
     *
     * @throws IllegalArgumentException if the WorkflowOperative class lacks a public
     *     no-arg constructor, returns a null/empty factory id, or collides with
     *     an existing operative/workflow.
     * @throws com.flezha.sdk.operative.WorkflowOperativeConfigurationException if the
     *     authoring is invalid (mirrors the Rust SDK's {@code register_workflow}
     *     panic on an invalid workflow operative).
     */
    @Nonnull
    public static String registerClass(@Nonnull Class<? extends WorkflowOperative> workflowClass) {
        WorkflowSpec spec = WorkflowSpec.build(workflowClass);
        String factoryId = spec.factoryId();

        if (StreamOperativeClassRegistry.getTopology(factoryId) != null) {
            throw new IllegalArgumentException("Duplicate factory id `" + factoryId + "` for WorkflowOperative class `"
                    + workflowClass.getName() + "` (already registered as a StreamOperative)");
        }

        ENTRIES.put(factoryId, spec);
        return factoryId;
    }

    /**
     * On-demand removal ({@code FlezhaRunner.removeWorkflow}). Returns the
     * removed spec (for {@code FlezhaImpl.removeType}'s re-register-on-live-hit
     * path), or {@code null} when the id is unknown. Reached reflectively from
     * the api module's {@code RegistryCore} on the pre-start path.
     */
    public static WorkflowSpec unregisterClass(@Nonnull String factoryId) {
        return ENTRIES.remove(factoryId);
    }

    /** Restore a spec {@code unregisterClass} removed (live-instance refusal). */
    public static void restore(@Nonnull String factoryId, @Nonnull WorkflowSpec spec) {
        ENTRIES.put(factoryId, spec);
    }

    /** The frozen spec for a registered workflow factory, or {@code null} if unknown. */
    public static WorkflowSpec getSpec(@Nonnull String factoryId) {
        return ENTRIES.get(factoryId);
    }

    @Nonnull
    public static List<String> factoryIds() {
        return List.copyOf(ENTRIES.keySet());
    }

    public static boolean isEmpty() {
        return ENTRIES.isEmpty();
    }

    /** Test-only — invoked by the api module's {@code RegistryCore.reset()}. */
    public static void reset() {
        ENTRIES.clear();
    }
}
