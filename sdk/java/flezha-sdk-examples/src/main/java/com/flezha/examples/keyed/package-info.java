/**
 * Partitioning one stream into many independent entities, and reshaping values on the wiring.
 *
 * <p>Each incoming trade is folded into four timeframes, and every distinct symbol-and-timeframe
 * pair maintains its own running candle with its own state, processed in parallel with the others.
 * A hundred symbols across four timeframes is four hundred independent entities, none of which is
 * declared in advance: an entity is created on first sight of its key.
 *
 * <p>{@link com.flezha.examples.keyed.CandleOperative} carries the lesson. Three transforms run on
 * the wiring itself before any user code is invoked: {@code filter} discards values, {@code flatMap}
 * expands one value into several, and {@code keyBy} assigns each result to an entity. These execute
 * without an entity, a mailbox or a dispatch hop, which makes them the appropriate place to reshape
 * a value and the wrong place to perform substantial work.
 *
 * <p>The ordering is significant. {@code keyBy} is total: it always names an entity and has no way
 * to decline one, so discarding unwanted values is {@code filter}'s responsibility and must happen
 * before the key is computed.
 *
 * <p>{@link com.flezha.examples.keyed.CandleProcedure} declares no keying of any kind. It is placed
 * in dynamic mode because a keyed edge feeds it, so the partitioning is visible where the graph is
 * declared rather than buried in the procedure. This keeps the procedure reusable under a different
 * keying scheme without modification.
 */
package com.flezha.examples.keyed;
