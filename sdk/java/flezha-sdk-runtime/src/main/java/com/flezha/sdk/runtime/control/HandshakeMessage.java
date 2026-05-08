package com.flezha.sdk.runtime.control;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.flezha.sdk.relay.RelayRegistry;
import java.util.List;
import java.util.stream.Collectors;

/**
 * JSON payload sent with the {@code connect_control_channel} frame. Matches
 * the Rust SDK's {@code ControlInitMessage} field names exactly —
 * {@code namespace}, {@code factory_ids}, {@code factories}.
 *
 * <p>{@code factories} carries a per-Relay description record. The
 * Relay interface has no description hook today, so the per-id
 * description is always empty.
 */
public record HandshakeMessage(String namespace, List<String> factory_ids, List<FactoryInfo> factories) {

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record FactoryInfo(String id, String description) {}

    public static HandshakeMessage from(String namespace) {
        List<String> ids = RelayRegistry.factoryIds();
        List<FactoryInfo> infos =
                ids.stream().map(id -> new FactoryInfo(id, "")).collect(Collectors.toList());
        return new HandshakeMessage(namespace, ids, infos);
    }
}
