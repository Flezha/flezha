/**
 * A two-stage graph, demonstrating that a procedure's output is an ordinary channel.
 *
 * <p>Every preceding example has a single procedure, which can leave the impression that chaining
 * is a distinct feature. It is not. What a procedure emits travels on a channel, and a channel can
 * feed another procedure exactly as readily as it can feed a sink, so a chain of stages is the
 * normal shape rather than a special case.
 *
 * <p>{@link com.flezha.examples.pipeline.PipelineOperative} holds the entire lesson in one
 * expression: an entry channel feeds {@link com.flezha.examples.pipeline.Normalize}, whose output
 * is labelled and feeds {@link com.flezha.examples.pipeline.Shout}, whose output reaches a sink.
 * The two procedures are deliberately trivial so that nothing distracts from the wiring.
 *
 * <p>Each stage is an independent procedure with its own state, scheduled and restarted on its own,
 * so a slow or failing stage does not take the others down with it. Neither stage knows its
 * position in the chain; a stage consumes messages and emits results, and the graph decides where
 * those come from and go. The only coupling is the type: a stage's output type must match the next
 * stage's input type.
 *
 * <p>The labels supplied to {@code named} are not documentation. They are the identities the rest of
 * the system uses, including sink registration and the channel a test feeds or reads.
 * {@code PipelineTest} exercises this graph end to end in local mode.
 */
package com.flezha.examples.pipeline;
