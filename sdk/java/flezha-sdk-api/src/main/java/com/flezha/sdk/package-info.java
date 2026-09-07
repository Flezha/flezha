/**
 * Entry points for declaring and running a Flezha application.
 *
 * <p>An application is built from two authoring types and started by a runner. A
 * {@link com.flezha.sdk.procedure.Procedure} contains the code that handles a single message. A
 * {@link com.flezha.sdk.operative.StreamOperative} or
 * {@link com.flezha.sdk.operative.WorkflowOperative} declares the graph that connects entry
 * channels, procedures and sinks, and is the unit the cluster schedules, restarts and relocates.
 * {@link com.flezha.sdk.Flezha#createRunner(String)} returns a {@link com.flezha.sdk.FlezhaRunner}
 * builder on which every operative class is declared before the runner is started.
 *
 * <p>A runner connects to a controller, which owns placement, durability, delivery guarantees,
 * retries and reconnection. {@code run()} connects and blocks until shutdown; {@code start()}
 * returns to the caller and leaves the runner to be closed explicitly.
 *
 * <p>{@link com.flezha.sdk.LocalOperative} executes the same declared graph inside the calling JVM
 * with no controller, no socket and no container. Messages are supplied with {@code feed} and sink
 * output is delivered to a callback registered with {@code onSink}. Nothing is durable in this
 * mode: state is held in memory, no write-ahead log or snapshot is written, and no recovery is
 * possible. Declarations that require a controller, including durability, effectively-once
 * delivery, plugins, placement and the dead-letter queue, are refused at startup and the refusal
 * names the declaration responsible.
 *
 * <p>The remaining types in this package are the supporting surface for those two paths:
 * {@link com.flezha.sdk.SinkOutput} is the frame handed to a local-mode sink callback,
 * {@link com.flezha.sdk.IdleObserver} reports when a local run has drained, and
 * {@link com.flezha.sdk.OperativeTypeInUseException} is raised when one operative id is declared
 * twice on a single runner.
 */
@javax.annotation.ParametersAreNonnullByDefault
package com.flezha.sdk;
