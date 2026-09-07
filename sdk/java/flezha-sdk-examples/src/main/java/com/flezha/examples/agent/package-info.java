/**
 * A long-lived agent: one durable iteration per message, holding its history across restarts.
 *
 * <p>The conversation is kept in entity state, so the agent resumes mid-thread after a crash, a
 * redeployment, or an arbitrary period of silence. Nothing runs between messages; the instance
 * parks holding its journal and is resumed when addressed.
 *
 * <p>{@link com.flezha.examples.agent.ChatOperative} declares the two properties that make this a
 * loop rather than a single pass. {@link com.flezha.sdk.operative.LoopMode} of {@code PER_INPUT}
 * gives one durable iteration per inbound message. {@code wakeAfter} additionally schedules a wake
 * when no message has arrived, which is how an agent produces a nudge, a summary or a follow-up
 * without being addressed; omit it and the agent wakes only when spoken to.
 *
 * <p>{@link com.flezha.examples.agent.ReplyProcedure} is where a real implementation would call a
 * model, passing the accumulated turns as history. Keeping the reply a function of that history is
 * what allows an iteration that failed part-way to be re-executed safely.
 *
 * <p>This is also the example where {@code kryoTypes} must be written by hand. The default covers
 * the change and state classes recovered from the type signature, but the conversation holds an
 * {@code ArrayList}, which is not visible there and must be declared. Entries are positional and
 * therefore append-only: reordering the list reinterprets previously written state as the wrong
 * type.
 */
package com.flezha.examples.agent;
