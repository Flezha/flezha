/**
 * The minimal complete application: an operative that declares the graph, a stateless procedure it
 * routes each message into, and a main that serves it.
 *
 * <p>This example deliberately introduces nothing else. A line of text arrives on an entry channel,
 * the procedure prefixes it, and the result is delivered to a plugin sink.
 *
 * <p>{@link com.flezha.examples.echo.EchoOperative} declares the graph and is the unit the cluster
 * actually schedules. {@link com.flezha.examples.echo.EchoProcedure} is the processing stage
 * invoked once per message. {@link com.flezha.examples.echo.Main} starts a runner and declares the
 * operative on it. Read the operative first: its single wiring expression is the shortest complete
 * description of what this application does.
 *
 * <p>The procedure declares {@link com.flezha.sdk.state.NoChange} as its change type, which opts
 * out of the persistence machinery entirely. Nothing is journaled, no snapshot is written, and a
 * restart begins with no history. The {@code state} example is the next step, and covers what
 * changes when that is no longer acceptable.
 */
package com.flezha.examples.echo;
