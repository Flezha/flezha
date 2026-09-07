/**
 * Channel and stream declarations: the graph connecting entry points, procedures and sinks.
 *
 * <p>These types are declared inside an operative's {@code channels(ChannelBuilder)} or
 * {@code procedures(ChannelBuilder)} method and are never constructed directly.
 * {@link com.flezha.sdk.channel.ChannelBuilder} is the entry point, and every call returns the next
 * handle in the chain, so a complete graph is expressed as one fluent expression. The declaration
 * is read once at registration and validated as a whole; a malformed graph is refused there rather
 * than failing during processing.
 *
 * <p>{@link com.flezha.sdk.channel.ChannelBuilder#input(Class, String)} declares an entry channel
 * fed by the controller. A {@code byte[]} payload is delivered unchanged, a {@code String} is
 * decoded as UTF-8, and any other type is deserialised at the entry edge with the SDK's default
 * JSON codec. An overload accepts an explicit parser for other wire formats, which must be
 * deterministic; a parse failure is classified like a failing invocation and follows the same
 * retry and dead-letter path.
 *
 * <p>{@link com.flezha.sdk.channel.Stream} is a procedure's output handle. {@code named} labels the
 * channel, and that label is the identity the rest of the system uses, including sink registration
 * and test feeds; unlabelled, a channel takes the producing procedure's name. {@code into} wires a
 * further procedure as a consumer, which is how multi-stage graphs are expressed. {@code sink}
 * attaches a plugin, {@code keyed} declares that emits carry a natural key supplied through
 * {@code emitKeyed}, and {@code codec} declares the payload codec for a channel, which is required
 * on every inter-procedure channel of a durable workflow operative because the handoff is journaled
 * at the step boundary.
 *
 * <p>{@link com.flezha.sdk.channel.EdgeStream} carries the transforms applied on the wiring itself:
 * {@code filter} discards values, {@code map} and {@code flatMap} reshape or expand them, and
 * {@code keyBy} assigns each value to an entity. These execute without an entity, a mailbox or a
 * dispatch hop, which makes them the cheap place to reshape a value and the wrong place to do
 * substantial work. {@code keyBy} is total: it always names an entity and never declines, so
 * discarding is {@code filter}'s responsibility and must precede it.
 *
 * <p>{@link com.flezha.sdk.channel.Merge} opens a merged subscription, in which two or more source
 * channels each supply a mapping into one sum type consumed by a single procedure.
 * {@link com.flezha.sdk.channel.RoutedDocument} supports operative-wide document delivery, in which
 * every key observed on a traffic channel receives each distinct document exactly once, on that
 * key's own next traffic item. {@link com.flezha.sdk.channel.RetryPolicy},
 * {@link com.flezha.sdk.channel.OverflowPolicy} and
 * {@link com.flezha.sdk.channel.BeyondWindowPolicy} declare, respectively, the retry ladder before
 * a payload is parked, the behaviour of a full mailbox, and the treatment of a message arriving
 * outside the correction window.
 */
@javax.annotation.ParametersAreNonnullByDefault
package com.flezha.sdk.channel;
