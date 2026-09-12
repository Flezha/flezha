/**
 * The two operative kinds and the base they share. An operative is the deployable unit.
 *
 * <p>A procedure cannot be deployed on its own, because nothing in the system knows where its
 * messages originate or where its output belongs. An operative supplies that binding, which makes
 * it the unit the controller schedules onto a host, restarts after a failure, relocates when a host
 * is withdrawn, and scales across several hosts.
 *
 * <p>{@link com.flezha.sdk.operative.Operative} is the shared base, and its single member is the
 * stable {@code factoryId()} that both kinds carry. The two kinds differ in what they declare
 * beyond it.
 *
 * <p>{@link com.flezha.sdk.operative.StreamOperative} declares a graph of channels and procedures
 * that runs continuously, holds per-entity state, and is the right choice when work is expressed as
 * messages flowing through one or more procedures. Its graph is declared in
 * {@code channels(ChannelBuilder)}, invoked once at registration and validated as a whole; a
 * declaration that is inconsistent or that requests an unavailable capability raises
 * {@link com.flezha.sdk.operative.StreamOperativeConfigurationException} naming the declaration at
 * fault.
 *
 * <p>{@link com.flezha.sdk.operative.WorkflowOperative} declares ordered steps that must not be
 * re-executed as a unit after a partial failure, such as a transaction spanning external systems.
 * Each completed step is journaled at its boundary, so a crash resumes at the next step rather than
 * the first, and the value passed between steps is written down there — encoded by a codec the
 * runtime derives, which no channel declares. {@link com.flezha.sdk.operative.RollbackMode} selects
 * whether an exhausted retry ladder unwinds completed steps through their {@code compensate}
 * methods or retains progress. {@link com.flezha.sdk.operative.LoopMode} and {@code wakeAfter()}
 * turn a workflow into a long-lived loop: one durable iteration per inbound message, optionally
 * woken by a timer, parking between iterations while holding its journal.
 */
@javax.annotation.ParametersAreNonnullByDefault
package com.flezha.sdk.operative;
