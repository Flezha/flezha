package com.flezha.sdk.runtime.control;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.flezha.sdk.bounded.BoundedOperativeRegistry;
import com.flezha.sdk.operative.ContinuousOperativeRegistry;
import com.flezha.sdk.runtime.ExecutorIdStore;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * JSON payload sent with the {@code connect_control_channel} frame. Matches
 * the Rust SDK's {@code ControlInitMessage} field names exactly —
 * {@code namespace}, {@code factory_ids}, {@code factories}, {@code stable_id}.
 *
 * <p>{@code factories} carries a per-ContinuousOperative description record. The
 * ContinuousOperative interface has no description hook today, so the per-id
 * description is always empty.
 *
 * <p>{@code stable_id} is the executor's self-owned, durable stable identity
 * (see {@link ExecutorIdStore}). It is present on EVERY connect — initial and
 * reconnect alike — and trusted by the controller as the operative-binding key.
 * Mirrors the Rust SDK.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record HandshakeMessage(
        String namespace, List<String> factory_ids, List<FactoryInfo> factories, String stable_id) {

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record FactoryInfo(String id, String description) {}

    public static HandshakeMessage from(String namespace) {
        // ContinuousOperative factories first, then bounded factories — both kinds are advertised
        // in one combined list so the controller validates any AddOperative kind.
        List<String> ids = new ArrayList<>(ContinuousOperativeRegistry.factoryIds());
        ids.addAll(BoundedOperativeRegistry.factoryIds());
        List<FactoryInfo> infos =
                ids.stream().map(id -> new FactoryInfo(id, "")).collect(Collectors.toList());
        return new HandshakeMessage(namespace, ids, infos, ExecutorIdStore.stableId(namespace, ids));
    }
}
