/**
 * Durable per-entity state, and the reason a procedure never assigns to its own fields.
 *
 * <p>The procedure here counts the messages an entity has seen, and the count survives a restart.
 * Compared with the {@code echo} example, exactly one thing changed: the change type argument
 * became a real type instead of {@link com.flezha.sdk.state.NoChange}. Declaring a change type is
 * the whole of what makes a procedure durable, and the wiring is untouched.
 *
 * <p>The mechanism is a change log rather than object serialisation. Flezha records the ordered
 * list of changes the procedure applied and reconstructs the state object after a restart by
 * replaying that list, on top of the most recent snapshot where one exists. A field assigned
 * directly from {@code run} never enters the log and is therefore absent from the reconstructed
 * object. This is the single most common mistake against the SDK, and it is silent: a direct
 * assignment behaves correctly for as long as the process stays alive, and is only lost at the
 * moment recovery matters.
 *
 * <p>Three members carry the pattern in
 * {@link com.flezha.examples.state.CountProcedure}. The nested {@code Increment} record is the
 * change, and must carry everything needed to redo the mutation later, which is why its timestamp
 * is a field rather than something read during replay. {@code applyChange} is the only method
 * permitted to write state, and is called both when a change is applied and again for each logged
 * change during reconstruction. {@code run} applies changes and emits, and reads the declared clock
 * through {@code ctx.now()} rather than the system clock, because it will be executed a second time
 * during recovery and must produce the same result.
 *
 * <p>Declaring a change type also makes an entity readable from outside the pipeline. Nothing
 * further is required: {@code flz state get} returns the current value of any entity of a running
 * operative, which is how a dashboard or an operational query reads a value without waiting for it
 * to reach a sink.
 */
package com.flezha.examples.state;
