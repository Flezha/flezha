package com.flezha.sdk.operative;

/**
 * Thrown at {@link com.flezha.sdk.FlezhaRunner#defineWorkflow declaration} time when a
 * {@link WorkflowOperative}'s authoring is invalid: an external-stop-only
 * operative (no self-termination and no {@code maxDuration()}), or a structured
 * {@link WorkflowOperative#procedures} declaration that is not a single connected
 * linear chain (branching, a cycle, a dynamic-entity procedure, a disconnected
 * procedure, or no entry channel).
 *
 * <p>Unchecked, mirroring {@link com.flezha.sdk.operative.StreamOperativeConfigurationException}
 * — it surfaces a programming error in the authoring, caught at registration
 * rather than at run time.
 */
public class WorkflowOperativeConfigurationException extends RuntimeException {

    /** More than one attempt with no backoff would hot-spin the failing entity. */
    public static WorkflowOperativeConfigurationException zeroRetryBackoff(String factoryId) {
        return new WorkflowOperativeConfigurationException("ZeroRetryBackoff: workflow operative `" + factoryId
                + "` declares retryAttempts() > 1 with a zero retryBackoff(); the ladder would hot-spin. "
                + "Declare a backoff, or set retryAttempts() to 1 for fail-fast");
    }

    /** A loop iterates the chain; run-sugar has none, so a declared loop has nothing to iterate. */
    public static WorkflowOperativeConfigurationException loopWithoutChain(String factoryId) {
        return new WorkflowOperativeConfigurationException("LoopWithoutChain: workflow operative `" + factoryId
                + "` declares a loopMode but authors its body with run()-sugar, which has no chain to iterate; "
                + "declare procedures() or drop the loop mode");
    }

    public WorkflowOperativeConfigurationException(String message) {
        super(message);
    }
}
