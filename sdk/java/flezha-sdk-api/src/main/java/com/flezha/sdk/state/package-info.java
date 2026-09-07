/**
 * Per-entity state, the change log that makes it durable, and the clocks a procedure may declare.
 *
 * <p>State is not persisted by serialising the state object. It is persisted as an ordered log of
 * {@link com.flezha.sdk.state.Change} values, and the object is reconstructed after a restart by
 * replaying that log over the value returned from {@code initState}, on top of the most recent
 * snapshot where one exists. A field assigned directly from {@code run} is therefore absent from
 * the log and absent from the reconstructed object.
 *
 * <p>Consequently, {@link com.flezha.sdk.state.StateHandle#apply} is the only supported way to
 * mutate state, and {@code Procedure.applyChange} is the only method that writes fields. The
 * runtime invokes {@code applyChange} once when a change is applied and again for every logged
 * change during reconstruction, so it must apply the change and do nothing else: no emitting, no
 * clock reads, no external calls. This failure mode is silent under normal operation, because a
 * direct assignment behaves correctly for as long as the process survives.
 *
 * <p>A procedure that persists nothing declares {@link com.flezha.sdk.state.NoChange}, which opts
 * out of the persistence machinery entirely. {@link com.flezha.sdk.state.CoalesceKey} allows
 * changes carrying equal keys to collapse to the latest within a flush window, which suits
 * "current value of X" updates and is wrong for additive deltas, since collapsing those loses
 * every value but the last.
 *
 * <p>The clock is a type-level declaration rather than a runtime choice.
 * {@link com.flezha.sdk.state.WallClock} evaluates processing time once per invocation and serves
 * the same value on replay. {@link com.flezha.sdk.state.EventTimeFrom} takes the time from the
 * message itself, which is what makes late arrival meaningful and what allows a correction replay
 * to place a message in the interval it belongs to. A procedure declaring
 * {@code EventTimeFrom} must also override the extractor, because Java's erasure cannot bind it
 * from the type argument; failing to do so is refused at registration rather than at run time.
 *
 * <p>{@link com.flezha.sdk.state.RestoreInitPayload} carries state handed over when an entity is
 * relocated from another host rather than started cold.
 * {@link com.flezha.sdk.state.ReplayMismatch} reports a reconstruction that did not reproduce the
 * observed state, which is the diagnostic for non-deterministic user code.
 * {@link com.flezha.sdk.state.TimerKind} distinguishes the timer families delivered to
 * {@code onTimer}.
 */
@javax.annotation.ParametersAreNonnullByDefault
package com.flezha.sdk.state;
