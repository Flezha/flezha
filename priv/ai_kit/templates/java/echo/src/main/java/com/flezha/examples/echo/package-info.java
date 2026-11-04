/**
 * The minimal complete application: a relay that declares the graph, a stateless tasklet it
 * routes each message into, and a main that serves it.
 *
 * <p>This example deliberately introduces nothing else. A line of text arrives on an entry channel,
 * the tasklet prefixes it, and the result is delivered to a plugin output.
 *
 * <p>{@link com.flezha.examples.echo.EchoRelay} declares the graph and is the unit the cluster
 * actually schedules. {@link com.flezha.examples.echo.Echo} is the processing stage invoked once per
 * message, and the class the relay routes into. {@link com.flezha.examples.echo.Main} starts a
 * runner and declares the relay on it. Read the relay first: its single wiring expression is
 * the shortest complete description of what this application does.
 *
 * <p>The tasklet declares {@code Void} state, so it persists nothing. Nothing is
 * journaled, no snapshot is written, and a restart begins with no history. The {@code state}
 * example is the next step, and covers what changes when that is no longer acceptable.
 */
package com.flezha.examples.echo;
