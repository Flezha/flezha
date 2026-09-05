package com.flezha.sdk.runtime.core;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * What {@code flz_core::host::register::check} answered about a record: the
 * consumers each chain wires, the kind each channel ends up with, the name a
 * {@code named} link overrode, and the plugin descriptor a {@code sink} link
 * named. The answer also carries the acknowledgement granularity the
 * checkpoint keys imply; no Java behaviour reads it (the controller derives it
 * from the registered topology), so it is not modelled here.
 *
 * <p>Everything here is the core's answer. The builder submits its record and
 * reads this back; it re-derives none of it.
 */
public record DerivedDeclaration(@Nonnull List<Channel> channels) {

    /** One channel as the core read it. */
    public record Channel(
            int index,
            @Nonnull String name,
            @Nonnull String kind,
            @Nullable Integer producer,
            @Nonnull List<Edge> consumers,
            @Nullable String sink) {}

    /**
     * One consumer edge as the core derived it: the procedure it feeds, and
     * the index of the chain that produced it. Which chain survives a
     * re-declaration is the core's answer — the builder attaches that chain's
     * closures and decides nothing.
     */
    public record Edge(int procedure, int chain) {}

    /** The answer for the channel declared at {@code index}. */
    @Nonnull
    public Channel channel(int index) {
        for (Channel channel : channels) {
            if (channel.index() == index) {
                return channel;
            }
        }
        throw new IllegalStateException("the core derived no channel at index " + index);
    }

    /** Read the derived declaration off the JSON the core handed back. */
    @Nonnull
    static DerivedDeclaration of(@Nonnull JsonNode json) {
        List<Channel> channels = new ArrayList<>();
        for (JsonNode node : json.get("channels")) {
            List<Edge> consumers = new ArrayList<>();
            node.get("consumers")
                    .forEach(edge -> consumers.add(new Edge(
                            edge.get("procedure").asInt(), edge.get("chain").asInt())));
            JsonNode producer = node.get("producer");
            JsonNode sink = node.get("sink");
            channels.add(new Channel(
                    node.get("index").asInt(),
                    node.get("name").asText(),
                    node.get("kind").asText(),
                    producer == null || producer.isNull() ? null : producer.asInt(),
                    List.copyOf(consumers),
                    sink == null || sink.isNull() ? null : sink.asText()));
        }
        return new DerivedDeclaration(List.copyOf(channels));
    }
}
