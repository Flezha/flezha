/**
 * Session windows: closing an entity after a period of inactivity, using timers.
 *
 * <p>Each click extends a session's deadline. If no further click arrives within the idle period
 * the timer fires, the session is closed, and a summary is emitted. One entity per user means every
 * session maintains its own independent deadline.
 *
 * <p>{@link com.flezha.examples.timers.SessionWindow} carries the pattern, which is to retract the
 * deadline set previously before registering a new one. Omitting the cancellation accumulates one
 * live timer per click, and the earliest of them closes the session while it is still active.
 *
 * <p>{@code onTimer} is the second entry point into user code alongside {@code run}. It receives
 * the same state handle, so a timer expiry can record a change and emit output exactly as a message
 * can, and the two are indistinguishable from the perspective of durability.
 *
 * <p>The pending deadline is held in a plain field rather than in durable state. That is safe here
 * for two reasons: the runtime holds one procedure instance per entity, so the field is not shared,
 * and a restart re-arms timers rather than relying on a value carried across it. State that must
 * survive a restart still belongs in the change log, as everywhere else.
 */
package com.flezha.examples.timers;
