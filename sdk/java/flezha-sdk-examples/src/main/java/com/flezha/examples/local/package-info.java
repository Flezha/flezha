/**
 * Executing a complete graph inside the calling JVM, with no controller.
 *
 * <p>{@link com.flezha.sdk.LocalOperative} takes the same operative class that would be deployed,
 * runs its declared wiring in this process, and delivers sink output to a callback. No controller,
 * no socket and no container are involved, which makes this both the fastest way to try an idea and
 * the basis for a test that exercises real wiring rather than a mock.
 *
 * <p>{@code feed} places a message on a named entry channel and blocks while the consumer's mailbox
 * is full. {@code onSink} registers a callback against a channel name, which is the producing
 * procedure's name unless the declaration supplied one through {@code named}.
 * {@code runUntilIdle} drives the graph until it drains or the supplied timeout elapses; the
 * calling thread is the poll loop, so the graph makes no progress without it.
 *
 * <p>Nothing is durable in this mode. State is held in memory, no write-ahead log or snapshot is
 * written, no dead-letter queue exists and no recovery is possible. Every run reports this on
 * standard error and the notice cannot be suppressed. Local mode establishes that a graph does what
 * its author intends; it cannot establish anything about crash behaviour.
 *
 * <p>Declarations requiring a controller, including durability, effectively-once delivery, plugins,
 * placement and migration, observability export and multi-runner scale, are refused at startup, and
 * the refusal names the declaration responsible.
 * {@link com.flezha.examples.local.WordCountOperative} declares a two-stage graph, splitting lines
 * into words and counting each word in its own entity, so the run also demonstrates keying.
 */
package com.flezha.examples.local;
