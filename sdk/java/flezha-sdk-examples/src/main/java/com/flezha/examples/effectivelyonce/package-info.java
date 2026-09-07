/**
 * Output that a retry cannot duplicate, for sinks where a duplicate is not acceptable.
 *
 * <p>Recovery re-executes user code, so a naive emit can reach a sink more than once for one
 * logical input. Where the sink is a database row, a payment or any other system that cannot absorb
 * a repeat, two mechanisms together prevent it, and both are required.
 *
 * <p>The first is keyed emission. {@code ctx.emitKeyed} attaches a key derived from the input, so a
 * replay of the same input produces the same key and the sink updates the record it already wrote
 * rather than appending a second one. It is the key, not the delivery machinery, that makes a
 * repeated emit harmless.
 *
 * <p>The second is the checkpoint interval. Until {@code ctx.checkpoint()} is called the consumed
 * offset is withheld and the applied changes remain staged, so a crash replays the whole open
 * interval rather than leaving a partially constructed window on disk. The procedure here closes an
 * interval every hundred ticks.
 *
 * <p>{@link com.flezha.examples.effectivelyonce.RollupOperative} declares both
 * {@code effectivelyOnce()} and {@code checkpointAck()}. The first makes state, output and consumed
 * offset durable together in a single record, so a crash cannot advance the offset while losing the
 * output. The second is what withholds the offset until the procedure declares the state coherent.
 * Registration refuses this combination without keyed sinks, because without a key the sink has no
 * means of recognising a repeat.
 */
package com.flezha.examples.effectivelyonce;
