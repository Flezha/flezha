/**
 * Procedure-requested spawning: a stream procedure asking the cluster for a workflow worker.
 *
 * <p>A detector watching a stream identifies something warranting substantial work and wants a
 * worker assigned to it, without an external dispatcher and without becoming one itself.
 *
 * <p>{@code ctx.spawnWorkflow} is spawn-or-deliver against a single instance identifier. The first
 * alert for a subject starts a worker with that alert as its first input; subsequent alerts for the
 * same subject are delivered to the worker already handling it. Concurrent requests, whether from
 * other entities or other hosts, are resolved controller-side onto exactly one instance, so there is
 * no check-then-create race for the author to defend against.
 * {@link com.flezha.sdk.procedure.IfRunning} selects that behaviour.
 *
 * <p>The call is fire-and-forget. It returns before the controller has decided anything, and a
 * refusal, whether from a quota, an unknown factory or an unreachable controller, resolves the
 * returned handle rather than failing the calling procedure. This is deliberate: a detector that
 * stops detecting because a worker could not be started is a worse outcome than a worker that was
 * not started. A handle nobody reads logs itself.
 *
 * <p>{@link com.flezha.examples.spawn.Main} declares both operatives on one runner, since the
 * requesting stream operative and the workflow operative it requests are ordinary declarations with
 * no special relationship beyond the factory id.
 */
package com.flezha.examples.spawn;
