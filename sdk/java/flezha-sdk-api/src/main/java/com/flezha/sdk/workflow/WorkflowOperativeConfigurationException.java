package com.flezha.sdk.workflow;

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

    /**
     * A durable CHAINED loop re-seeds each iteration from the terminal proc's
     * journaled handoff. With no codec it would re-seed EMPTY on a crash-resume,
     * which the loop reads as "converged" — silently truncating the run.
     */
    public static WorkflowOperativeConfigurationException loopTerminalWithoutCodec(String factoryId) {
        return new WorkflowOperativeConfigurationException("LoopTerminalWithoutCodec: workflow operative `" + factoryId
                + "` declares a durable CHAINED loop but its terminal procedure has no codec'd output channel; "
                + "a crash-resumed iteration would re-seed empty and read as converged");
    }

    /**
     * Raised when a durable structured workflow class declares an
     * inter-procedure channel with no codec. The handoff between two procedures
     * is journaled at the proc barrier and a crash-resumed run re-seeds the next
     * procedure from that blob — with no codec there is nothing to journal, so
     * the resume would silently degrade to re-running the chain from the top.
     */
    public static WorkflowOperativeConfigurationException missingInterProcCodec(String factoryId, String channel) {
        return new WorkflowOperativeConfigurationException("MissingInterProcCodec: workflow operative `" + factoryId
                + "` channel `" + channel + "` carries an inter-procedure handoff but declares no `.codec()`; "
                + "a durable workflow class journals that handoff so a resume can skip committed procedures");
    }

    public WorkflowOperativeConfigurationException(String message) {
        super(message);
    }
}
