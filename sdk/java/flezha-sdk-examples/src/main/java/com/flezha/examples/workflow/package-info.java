/**
 * A saga: ordered steps that must not be re-executed as a unit after a partial failure.
 *
 * <p>Reserve stock, charge the card, dispatch the parcel. If dispatch fails, restarting the
 * sequence is not a correct recovery, because the card has already been charged. Each completed
 * step is journaled at its boundary, so a crash part-way through resumes at the next step, and a
 * failure that exhausts its retries unwinds the completed steps by invoking their
 * {@code compensate}.
 *
 * <p>{@link com.flezha.examples.workflow.CheckoutOperative} declares the chain and its failure
 * policy, and nothing about how the handoffs are encoded. The value passed between two steps is
 * written down at that boundary, and that record is what allows a workflow that died after
 * charging to resume at dispatch rather than at reservation; the runtime derives how to write it.
 *
 * <p>Two details govern correctness in the compensating path.
 * {@link com.flezha.examples.workflow.Reserve} derives its reservation identifier from
 * {@code ctx.stableId()} rather than a random UUID, so a re-executed step derives the same
 * identifier and the external system observes a retry rather than a second reservation. And a
 * step's undo handle is held in its state rather than read from its emitted output, because state
 * is restored on resume and emitted output is not: after a crash-resumed rollback the
 * completed-output list handed to {@code compensate} may be empty.
 *
 * <p>{@link com.flezha.sdk.operative.RollbackMode} selects between unwinding everything and
 * retaining progress, the latter being meaningful only for loops. Where work is one message in and
 * one result out, a plain {@link com.flezha.sdk.operative.StreamOperative} is the correct choice;
 * this is the heavier mechanism and costs a journal write per step.
 */
package com.flezha.examples.workflow;
