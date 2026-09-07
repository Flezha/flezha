package com.flezha.sdk.channel;

/**
 * One delivery made by {@link ChannelBuilder#routeLatest}: the operative-wide
 * document, paired with the key whose own traffic earned it.
 *
 * <p>The pair is what the consumer keys by
 * ({@code .keyBy(RoutedDocument::key)}), which is what makes the delivery reach
 * that key's Entity and no other. Rust's equivalent stream carries a
 * {@code (String, D)} tuple; Java has no tuple, and the runtime needs a
 * {@link Class} token for the routed payload, so the pair is a record.
 *
 * @param key the Entity key this delivery is for
 * @param document the retained document, unparsed
 */
public record RoutedDocument<D>(String key, D document) {}
