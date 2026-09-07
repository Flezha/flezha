/**
 * The unit of user code: a procedure, the context handed to it, and the values it may request.
 *
 * <p>{@link com.flezha.sdk.procedure.Procedure} is parameterised by six types, in declaration
 * order: the input value, the per-entity state, the emitted output, the change type used to record
 * state mutations, the clock the procedure reads, and its configuration type. A procedure that
 * keeps no state declares {@link com.flezha.sdk.state.NoChange}; one that takes no configuration
 * declares {@link java.lang.Void}. The runtime recovers these six as {@code Class} handles from the
 * {@code implements} clause, so they must be concrete there or supplied by overriding the
 * corresponding accessor.
 *
 * <p>Invocation is serialised per entity. Two messages for the same key are never handled
 * concurrently, so a procedure requires no internal locking around its own state. Messages for
 * distinct keys are handled concurrently, on the same host or on different hosts, and a procedure
 * instance is held per entity rather than shared across them.
 *
 * <p>{@link com.flezha.sdk.procedure.ProcedureCtx} is the runtime handle passed to every
 * invocation. It supplies output through {@code emit} and {@code emitKeyed}, the declared clock
 * through {@code now()}, a replay-stable identifier through {@code stableId()}, checkpoint control,
 * timer registration, and the workflow spawn request described below.
 *
 * <p>Recovery re-executes user code. A procedure must therefore be a deterministic function of its
 * declared inputs: reading {@code System.currentTimeMillis()} or generating a random identifier
 * produces a different result on replay, and the rebuilt state then diverges from the state that
 * was originally observed. {@code ctx.now()} and {@link com.flezha.sdk.procedure.StableId} exist to
 * remove both sources of divergence.
 *
 * <p>{@link com.flezha.sdk.procedure.SpawnRequests}, {@link com.flezha.sdk.procedure.SpawnHandle},
 * {@link com.flezha.sdk.procedure.SpawnOutcome} and {@link com.flezha.sdk.procedure.IfRunning}
 * cover procedure-requested workflow spawning, in which a procedure asks the controller for a
 * worker instance rather than dispatching one itself. The request is resolved controller-side, so
 * concurrent requests against one instance id converge on a single instance.
 * {@link com.flezha.sdk.procedure.ProcedureException} is the checked failure type a procedure
 * raises; a raised failure is classified, retried according to the channel's policy, and parked in
 * the dead-letter queue if the retry ladder is exhausted.
 */
@javax.annotation.ParametersAreNonnullByDefault
package com.flezha.sdk.procedure;
